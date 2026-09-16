package client

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
)

// CallStream is one outbound call. Send/Recv/HalfClose go to the outermost
// OpenFilter-decorated Stream; Header/Trailer/Close act on the call itself.
type CallStream struct {
	client *Client
	stream stream.Stream
	call   framing.Call
	sess   framing.ClientSession
	md     metadata.CallMetadata

	callCtx    context.Context
	callCancel context.CancelFunc
	stopBridge func() bool
	stopWatch  func() bool // cancels framing.Call when callCtx is done

	headersCh   chan struct{}
	headersOnce sync.Once

	closeOnce sync.Once
	closeErr  error

	leak    *leakState
	cleanup runtime.Cleanup
}

// leakState is what the cleanup hook sees. It must not reference the
// CallStream, or the CallStream would never become unreachable and the hook
// would never run.
type leakState struct {
	closed atomic.Bool
	cfg    *argos.Config
	info   argos.CallInfo
	// client is reclaimed so a leaked call does not hold its admission
	// reservation for the lifetime of the Client. The Client outlives every
	// CallStream, so holding it here keeps nothing else alive.
	client *Client
}

// Send delegates to the decorated stream.
//
// There is no per-operation goroutine: that design returned s.callCtx.Err()
// while abandoning the operation, so a call that had already delivered its
// message was reported as a failure and the abandoned goroutine could still
// write into the caller's memory after Send/Recv had returned. Blocking is
// bounded instead by the call ctx: CallStream.Close closes the framing.Call,
// and stopWatch does the same as soon as callCtx is done (installed in Open
// before any operation can run), so a blocked Send/Recv is always unblocked.
func (s *CallStream) Send(v any) error {
	if err := s.callCtx.Err(); err != nil {
		return err
	}
	return s.interrupted(s.stream.Send(v))
}

// Recv delegates to the decorated stream. First successful or terminal Recv
// marks empty initial metadata arrived (fake Framing has no HEADERS frame).
func (s *CallStream) Recv(v any) error {
	if err := s.callCtx.Err(); err != nil {
		// Do not mark headers ready: none arrived. Marking here let a waiter in
		// Header() take the headersCh branch and return (stale metadata, nil)
		// for a call that was already cancelled. Header() selects on callCtx
		// itself, so it still wakes — with the ctx error.
		return err
	}
	err := s.stream.Recv(v)
	s.markHeadersReady()
	return s.interrupted(err)
}

// HalfClose delegates to the decorated stream.
func (s *CallStream) HalfClose() error {
	if err := s.callCtx.Err(); err != nil {
		return err
	}
	return s.interrupted(s.stream.HalfClose())
}

// interrupted maps an operation that was unblocked by cancelation onto the call
// context's error. A resolved result is never rewritten (§2.4): a delivered
// message (nil), a normal receive end (io.EOF) and an already-parsed remote
// status all win over a ctx that fired at the same moment. stream.ErrSendClosed
// is likewise preserved — it ends only the send direction.
func (s *CallStream) interrupted(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	var se *status.StatusError
	if errors.As(err, &se) {
		return err
	}
	if ctxErr := s.callCtx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// Header waits until response initial metadata is available (or implied by
// first DATA/STATUS via Recv), or the call ctx is done.
func (s *CallStream) Header() (metadata.Metadata, error) {
	select {
	case <-s.headersCh:
		return s.md.IncomingHeaders(), nil
	case <-s.callCtx.Done():
		return nil, s.callCtx.Err()
	}
}

// Trailer returns the current incoming trailers snapshot.
func (s *CallStream) Trailer() metadata.Metadata {
	return s.md.IncomingTrailers()
}

// Close closes the framing.Call, releases the session to the pool, and
// returns the admission reservation. Idempotent.
func (s *CallStream) Close() error {
	s.closeOnce.Do(func() {
		s.markHeadersReady()
		if s.leak != nil {
			s.leak.closed.Store(true)
		}
		s.cleanup.Stop()

		if s.stopWatch != nil {
			s.stopWatch()
		}
		if s.stopBridge != nil {
			s.stopBridge()
		}
		if s.callCancel != nil {
			s.callCancel()
		}

		err := s.call.Close()
		if s.client != nil && s.client.pool != nil && s.sess != nil {
			s.client.pool.Release(s.sess)
		}
		if s.client != nil {
			s.client.releaseAdmit()
		}
		s.closeErr = err
	})
	return s.closeErr
}

func (s *CallStream) markHeadersReady() {
	s.headersOnce.Do(func() {
		close(s.headersCh)
	})
}
