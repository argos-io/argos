package synth

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

type session struct {
	axis    *Transport
	conn    transport.Conn
	carrier transport.ByteStreamCarrier
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
	spentClean bool
}

func newSession(a *Transport, conn transport.Conn, car transport.ByteStreamCarrier, client bool, st *ConnState) *session {
	if st == nil {
		st = &ConnState{}
	}
	s := &session{
		axis:          a,
		conn:          conn,
		carrier:       car,
		client:        client,
		state:         st,
		openTimeout:   a.openTimeout,
		maxDrainBytes: a.maxDrainBytes,
		maxFrame:      a.maxFrame,
		maxMessage:    a.maxMessage,
		reusable:      true,
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

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

func (s *session) clientHandshake(ctx context.Context) error {
	closeOnCancel := func() { _ = s.conn.Close() }
	var greeting string
	if err := readAll(ctx, s.carrier, closeOnCancel, func(r io.Reader) error {
		g, e := readGreeting(r)
		greeting = g
		return e
	}); err != nil {
		return fmt.Errorf("synth: read greeting: %w", err)
	}
	if err := writeAll(ctx, s.carrier, closeOnCancel, writeAck); err != nil {
		return fmt.Errorf("synth: write ack: %w", err)
	}
	s.state.setGreeting(greeting)
	return nil
}

func (s *session) serverHandshake(ctx context.Context) error {
	greeting := s.axis.greeting
	if greeting == "" {
		greeting = DefaultGreeting
	}
	s.state.setGreeting(greeting)

	closeOnCancel := func() { _ = s.conn.Close() }
	if err := writeAll(ctx, s.carrier, closeOnCancel, func(w io.Writer) error {
		return writeGreeting(w, greeting)
	}); err != nil {
		return fmt.Errorf("synth: write greeting: %w", err)
	}
	if err := readAll(ctx, s.carrier, closeOnCancel, readAck); err != nil {
		return fmt.Errorf("synth: read ack: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Client connection
// ---------------------------------------------------------------------------

type clientConn struct{ *session }

func (s *clientConn) OpenCall(ctx context.Context, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	_ = spec
	if ctx == nil {
		return nil, status.Error(status.InvalidArgument, "synth: nil context")
	}
	if m.IsZero() {
		return nil, status.Error(status.InvalidArgument, "synth: zero Method")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "synth: session closed")
	}
	if s.inCall {
		s.mu.Unlock()
		return nil, transport.ErrConnBusy
	}
	if !s.reusable || s.ioError || s.exclusive {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "synth: session not reusable")
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

func (s *clientConn) Close() error { return s.closeSession() }

// ---------------------------------------------------------------------------
// Server connection
// ---------------------------------------------------------------------------

type serverConn struct {
	axis *Transport
	conn transport.Conn
	*session
}

func (s *serverConn) Handshake(ctx context.Context) error {
	car, err := assertByteCarrier(s.conn)
	if err != nil {
		return err
	}
	st, _ := ctx.Value(ctxKey{}).(*ConnState)
	if st == nil {
		st = &ConnState{}
	}
	sess := newSession(s.axis, s.conn, car, false, st)
	if err := sess.serverHandshake(ctx); err != nil {
		_ = sess.closeSession()
		return err
	}
	sess.handoffGoid = goroutineID()
	installHandoff(sess.handoffGoid, st)
	s.session = sess
	return nil
}

func (s *serverConn) AcceptCall(ctx context.Context, spec transport.CallSpec) (transport.ServerCall, error) {
	_ = spec
	if s.session == nil {
		return nil, fmt.Errorf("synth: AcceptCall before Handshake")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.inCall {
		s.mu.Unlock()
		return nil, transport.ErrConnBusy
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
		close(c.done)
		return c, nil
	}
	go c.recvLoop()
	return c, nil
}

func (s *serverConn) Close() error {
	if s.session == nil {
		return s.conn.Close()
	}
	return s.closeSession()
}

func (s *session) readOpen(ctx context.Context) (frame, error) {
	type result struct {
		f   frame
		err error
	}
	ch := make(chan result, 1)

	go func() {
		var hdr [4]byte
		first := make([]byte, 1)
		setReadDeadline(s.carrier, time.Time{})
		if _, err := io.ReadFull(s.carrier, first); err != nil {
			ch <- result{err: err}
			return
		}
		if s.openTimeout > 0 {
			deadline := time.Now().Add(s.openTimeout)
			if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
				deadline = dl
			}
			setReadDeadline(s.carrier, deadline)
			defer clearReadDeadline(s.carrier)
		}

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
