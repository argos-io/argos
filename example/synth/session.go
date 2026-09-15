package synth

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

type session struct {
	framing *Framing
	conn    transport.Conn
	carrier transport.ByteStreamCarrier
	cfg     framing.Config
	client  bool
	state   *ConnState

	openTimeout   time.Duration
	maxDrainBytes int64
	maxFrame      int64
	maxMessage    int64

	handoffGoid uint64

	writeMu sync.Mutex

	mu         sync.Mutex
	closed     bool
	reusable   bool
	exclusive  bool
	ioError    bool
	inCall     bool
	spentClean bool // prior call ended without terminal → not reusable
}

func newSession(f *Framing, conn transport.Conn, car transport.ByteStreamCarrier, cfg framing.Config, client bool, st *ConnState) *session {
	s := &session{
		framing:       f,
		conn:          conn,
		carrier:       car,
		cfg:           cfg,
		client:        client,
		state:         st,
		openTimeout:   f.openTimeout,
		maxDrainBytes: f.maxDrainBytes,
		maxFrame:      defaultMaxFrameSize,
		maxMessage:    defaultMaxMessage,
		reusable:      true,
	}
	if cfg.OpenTimeout > 0 {
		s.openTimeout = cfg.OpenTimeout
	}
	if cfg.MaxDrainBytes > 0 {
		s.maxDrainBytes = cfg.MaxDrainBytes
	}
	if cfg.MaxFrameSize > 0 {
		s.maxFrame = cfg.MaxFrameSize
	}
	if cfg.MaxMessageSize > 0 {
		s.maxMessage = cfg.MaxMessageSize
	}
	st.setOnExclusive(func() {
		s.mu.Lock()
		s.exclusive = true
		s.reusable = false
		s.mu.Unlock()
	})
	return s
}

func (s *session) markBad() {
	s.mu.Lock()
	s.reusable = false
	s.ioError = true
	s.mu.Unlock()
	_ = s.carrier.Abort()
}

func (s *session) Reusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reusable && !s.closed && !s.ioError && !s.exclusive && !s.spentClean
}

func (s *session) closeSession() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.reusable = false
	goid := s.handoffGoid
	s.mu.Unlock()
	if goid != 0 {
		clearHandoff(goid)
	}
	return s.conn.Close()
}

type clientSession struct{ *session }

func (s *clientSession) OpenCall(ctx context.Context, m descriptor.Method, spec framing.CallSpec) (framing.Call, error) {
	_ = spec
	if ctx == nil {
		return nil, status.Error(status.InvalidArgument, "synth: nil context")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "synth: session closed")
	}
	if s.inCall {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	if !s.reusable || s.ioError || s.exclusive {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	s.inCall = true
	s.mu.Unlock()

	if m.FullName() == MethodExclusive {
		s.state.EnterExclusive()
	}

	c := &call{
		session: s.session,
		method:  m.FullName(),
		client:  true,
		shape:   m.Shape(),
		recvCh:  make(chan recvItem, 8),
		done:    make(chan struct{}),
		cancel:  func() {},
	}
	c.ctx, c.cancel = context.WithCancel(ctx)

	flags := byte(0)
	// Unary / client-streaming still send OPEN then DATA; FlagOpenEnd only for
	// explicit zero-message — callers use HalfClose after Send as usual.
	s.writeMu.Lock()
	err := writeFrame(s.carrier, frame{typ: typeOpen, method: m.FullName(), flags: flags})
	s.writeMu.Unlock()
	if err != nil {
		s.detach(c, true)
		return nil, err
	}

	go c.recvLoop()
	return c, nil
}

func (s *clientSession) Reusable() bool { return s.session.Reusable() }
func (s *clientSession) Close() error   { return s.closeSession() }

type serverSession struct{ *session }

func (s *serverSession) AcceptCall(ctx context.Context, spec framing.CallSpec) (framing.ServerCall, error) {
	_ = spec
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.inCall {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	s.mu.Unlock()

	f, err := s.readOpen(ctx)
	if err != nil {
		if err == io.EOF || err == context.Canceled {
			return nil, io.EOF
		}
		if err == context.DeadlineExceeded {
			s.markBad()
			return nil, err
		}
		s.markBad()
		return nil, err
	}

	s.mu.Lock()
	s.inCall = true
	s.mu.Unlock()

	c := &serverCall{call: &call{
		session: s.session,
		method:  f.method,
		client:  false,
		recvCh:  make(chan recvItem, 8),
		done:    make(chan struct{}),
	}}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	if f.flags&flagOpenEnd != 0 {
		c.peerHalfClosed = true
		close(c.done) // no recvLoop; Recv sees EOF via done
		return c, nil
	}
	go c.recvLoop()
	return c, nil
}

func (s *serverSession) Close() error { return s.closeSession() }

func (s *session) readOpen(ctx context.Context) (frame, error) {
	type result struct {
		f   frame
		err error
	}
	ch := make(chan result, 1)

	// Wait for first byte under accept ctx only (no OpenTimeout yet).
	go func() {
		var hdr [4]byte
		// Peek-style: read length prefix, then body — OpenTimeout starts after
		// first byte of the length prefix arrives.
		first := make([]byte, 1)
		setReadDeadline(s.carrier, time.Time{})
		if _, err := io.ReadFull(s.carrier, first); err != nil {
			ch <- result{err: err}
			return
		}
		deadline := time.Now().Add(s.openTimeout)
		if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
			deadline = dl
		}
		setReadDeadline(s.carrier, deadline)
		defer clearReadDeadline(s.carrier)

		hdr[0] = first[0]
		if _, err := io.ReadFull(s.carrier, hdr[1:]); err != nil {
			ch <- result{err: err}
			return
		}
		n := int(binaryUint32(hdr[:]))
		if n < 1 {
			ch <- result{err: errTruncated}
			return
		}
		if s.maxFrame > 0 && int64(n) > s.maxFrame {
			ch <- result{err: errFrameTooLarge}
			return
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(s.carrier, body); err != nil {
			ch <- result{err: err}
			return
		}
		f, err := parseBody(body)
		if err != nil {
			ch <- result{err: err}
			return
		}
		if f.typ != typeOpen {
			ch <- result{err: fmt.Errorf("synth: expected OPEN, got type %d", f.typ)}
			return
		}
		ch <- result{f: f}
	}()

	select {
	case <-ctx.Done():
		_ = s.carrier.Abort()
		return frame{}, ctx.Err()
	case res := <-ch:
		return res.f, res.err
	}
}

func binaryUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func (s *session) detach(c *call, poison bool) {
	s.mu.Lock()
	if s.inCall {
		s.inCall = false
		if poison {
			s.spentClean = true
			s.reusable = false
		}
	}
	s.mu.Unlock()
	c.cancel()
}
