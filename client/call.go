package client

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
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

type leakState struct {
	closed atomic.Bool
	cfg    *argos.Config
	info   argos.CallInfo
}

// Send delegates to the decorated stream, aborted when the call ctx is done.
func (s *CallStream) Send(v any) error {
	if err := s.callCtx.Err(); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.stream.Send(v) }()
	select {
	case err := <-errCh:
		return err
	case <-s.callCtx.Done():
		return s.callCtx.Err()
	}
}

// Recv delegates to the decorated stream. First successful or terminal Recv
// marks empty initial metadata arrived (fake Framing has no HEADERS frame).
// Honors call ctx cancel even when the underlying Recv stays blocked.
func (s *CallStream) Recv(v any) error {
	if err := s.callCtx.Err(); err != nil {
		s.markHeadersReady()
		return err
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.stream.Recv(v) }()
	select {
	case err := <-errCh:
		s.markHeadersReady()
		return err
	case <-s.callCtx.Done():
		s.markHeadersReady()
		return s.callCtx.Err()
	}
}

// HalfClose delegates to the decorated stream.
func (s *CallStream) HalfClose() error {
	if err := s.callCtx.Err(); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.stream.HalfClose() }()
	select {
	case err := <-errCh:
		return err
	case <-s.callCtx.Done():
		return s.callCtx.Err()
	}
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
