package resp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

var (
	_ framing.Framing       = (*Framing)(nil)
	_ framing.ClientSession = (*clientSession)(nil)
	_ framing.ServerSession = (*serverSession)(nil)
	_ framing.Call          = (*call)(nil)
	_ framing.ServerCall    = (*serverCall)(nil)
)

// session is the shared Sequential state for one TCP connection.
type session struct {
	framing *Framing
	conn    transport.Conn
	carrier transport.ByteStreamCarrier
	cfg     framing.Config
	client  bool

	openTimeout time.Duration
	service     string
	password    string

	writeMu sync.Mutex

	mu       sync.Mutex
	closed   bool
	reusable bool
	busy     bool // one in-flight call
	readBuf  []byte
	readErr  error
	readWait chan struct{} // closed+replaced when new data/err arrives

	readerDone chan struct{}
	closeCh    chan struct{}
}

func assertByteStream(c transport.Conn) (transport.ByteStreamCarrier, error) {
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("resp: Conn %T is not transport.CarrierConn", c)
	}
	bs, ok := cc.Carrier().(transport.ByteStreamCarrier)
	if !ok {
		return nil, fmt.Errorf("resp: Carrier %T is not ByteStreamCarrier", cc.Carrier())
	}
	return bs, nil
}

func newSession(f *Framing, conn transport.Conn, car transport.ByteStreamCarrier, cfg framing.Config, client bool) *session {
	s := &session{
		framing:     f,
		conn:        conn,
		carrier:     car,
		cfg:         cfg,
		client:      client,
		openTimeout: applyOpenTimeout(f, cfg),
		service:     f.service,
		password:    f.password,
		reusable:    true,
		readWait:    make(chan struct{}),
		readerDone:  make(chan struct{}),
		closeCh:     make(chan struct{}),
	}
	go s.readLoop()
	return s
}

func (s *session) readLoop() {
	defer close(s.readerDone)
	tmp := make([]byte, 4096)
	for {
		n, err := s.carrier.Read(tmp)
		s.mu.Lock()
		if n > 0 {
			s.readBuf = append(s.readBuf, tmp[:n]...)
		}
		if err != nil {
			if s.readErr == nil {
				s.readErr = err
			}
			s.broadcastReadLocked()
			s.mu.Unlock()
			return
		}
		s.broadcastReadLocked()
		s.mu.Unlock()
	}
}

func (s *session) broadcastReadLocked() {
	close(s.readWait)
	s.readWait = make(chan struct{})
}

func (s *session) Reusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reusable && !s.closed
}

func (s *session) markBad() {
	s.mu.Lock()
	s.reusable = false
	s.mu.Unlock()
}

func (s *session) closeSession() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.reusable = false
	close(s.closeCh)
	s.broadcastReadLocked()
	s.mu.Unlock()
	err := s.conn.Close()
	<-s.readerDone
	return err
}

// readValue blocks until one complete RESP value is available or ctx is done.
func (s *session) readValue(ctx context.Context) (Value, error) {
	for {
		s.mu.Lock()
		if s.closed && len(s.readBuf) == 0 {
			err := s.readErr
			s.mu.Unlock()
			if err == nil {
				err = io.EOF
			}
			return Value{}, err
		}
		if len(s.readBuf) > 0 {
			v, n, err := parseValue(s.readBuf)
			if err == nil {
				s.readBuf = s.readBuf[n:]
				if len(s.readBuf) == 0 {
					s.readBuf = nil
				}
				s.mu.Unlock()
				return v, nil
			}
			if !errors.Is(err, errNeedMore) {
				s.reusable = false
				s.mu.Unlock()
				return Value{}, err
			}
		}
		if s.readErr != nil && len(s.readBuf) == 0 {
			err := s.readErr
			s.mu.Unlock()
			return Value{}, err
		}
		wait := s.readWait
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return Value{}, ctx.Err()
		case <-s.closeCh:
			// Loop again to drain buffer or surface readErr.
			continue
		case <-wait:
		}
	}
}

// waitFirstByte waits (accept ctx only) until at least one byte is buffered.
func (s *session) waitFirstByte(ctx context.Context) error {
	for {
		s.mu.Lock()
		if len(s.readBuf) > 0 {
			s.mu.Unlock()
			return nil
		}
		if s.readErr != nil {
			err := s.readErr
			s.mu.Unlock()
			return err
		}
		if s.closed {
			s.mu.Unlock()
			return io.EOF
		}
		wait := s.readWait
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closeCh:
			continue
		case <-wait:
		}
	}
}

func (s *session) writeRaw(p []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.carrier.Write(p)
	if err != nil {
		s.markBad()
	}
	return err
}

func (s *session) writeCommand(args ...string) error {
	return s.writeRaw(EncodeArray(args...))
}

func (s *session) readReply(ctx context.Context) (Value, error) {
	return s.readValue(ctx)
}

// ---------------------------------------------------------------------------
// Framing session constructors
// ---------------------------------------------------------------------------

// NewClientSession implements framing.Framing.
func (f *Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	car, err := assertByteStream(c)
	if err != nil {
		return nil, err
	}
	s := newSession(f, c, car, spec.Config, true)
	if err := s.clientHandshake(ctx); err != nil {
		_ = s.closeSession()
		return nil, err
	}
	return &clientSession{session: s}, nil
}

// NewServerSession implements framing.Framing.
func (f *Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	car, err := assertByteStream(c)
	if err != nil {
		return nil, err
	}
	s := newSession(f, c, car, spec.Config, false)
	if err := s.serverHandshake(ctx); err != nil {
		_ = s.closeSession()
		return nil, err
	}
	return &serverSession{session: s}, nil
}

func (s *session) clientHandshake(ctx context.Context) error {
	if err := s.writeCommand("HELLO", "2"); err != nil {
		return err
	}
	s.framing.hellos.Add(1)
	if _, err := s.readReply(ctx); err != nil {
		return fmt.Errorf("resp: HELLO: %w", err)
	}
	if s.password == "" {
		return nil
	}
	if err := s.writeCommand("AUTH", s.password); err != nil {
		return err
	}
	v, err := s.readReply(ctx)
	if err != nil {
		return fmt.Errorf("resp: AUTH: %w", err)
	}
	if v.Type == '-' {
		return status.Error(status.Unauthenticated, v.Str)
	}
	if v.Type != '+' || !strings.EqualFold(v.Str, "OK") {
		return fmt.Errorf("resp: AUTH unexpected reply type %q", string(v.Type))
	}
	return nil
}

func (s *session) serverHandshake(ctx context.Context) error {
	// Expect HELLO 2.
	v, err := s.readValue(ctx)
	if err != nil {
		return fmt.Errorf("resp: handshake read: %w", err)
	}
	cmd, args, err := commandFromValue(v)
	if err != nil {
		return err
	}
	if !strings.EqualFold(cmd, "HELLO") {
		return fmt.Errorf("resp: expected HELLO, got %q", cmd)
	}
	_ = args
	if err := s.writeRaw(EncodeSimple("OK")); err != nil {
		return err
	}
	if s.password == "" {
		return nil
	}
	v, err = s.readValue(ctx)
	if err != nil {
		return fmt.Errorf("resp: AUTH read: %w", err)
	}
	cmd, args, err = commandFromValue(v)
	if err != nil {
		return err
	}
	if !strings.EqualFold(cmd, "AUTH") {
		return fmt.Errorf("resp: expected AUTH, got %q", cmd)
	}
	if len(args) < 1 || args[0] != s.password {
		_ = s.writeRaw(EncodeError("ERR invalid password"))
		return status.Error(status.Unauthenticated, "resp: AUTH failed")
	}
	return s.writeRaw(EncodeSimple("OK"))
}

func commandFromValue(v Value) (cmd string, args []string, err error) {
	if v.Type != '*' || v.Null || len(v.Array) == 0 {
		return "", nil, fmt.Errorf("resp: command must be a non-empty array")
	}
	if v.Array[0].Type != '$' || v.Array[0].Null {
		return "", nil, fmt.Errorf("resp: command name must be a bulk string")
	}
	cmd = v.Array[0].Str
	args = make([]string, 0, len(v.Array)-1)
	for _, el := range v.Array[1:] {
		if el.Type != '$' || el.Null {
			return "", nil, fmt.Errorf("resp: command arg must be a bulk string")
		}
		args = append(args, el.Str)
	}
	return cmd, args, nil
}

// ---------------------------------------------------------------------------
// Client session
// ---------------------------------------------------------------------------

type clientSession struct {
	*session
}

func (s *clientSession) OpenCall(ctx context.Context, m descriptor.Method, _ framing.CallSpec) (framing.Call, error) {
	if m.IsZero() {
		return nil, status.Error(status.InvalidArgument, "resp: zero Method")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	if s.closed || !s.reusable {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "resp: session not reusable")
	}
	if s.busy {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	s.busy = true
	s.mu.Unlock()

	return newCall(s.session, m.FullName(), m.Name(), true), nil
}

func (s *clientSession) Close() error { return s.closeSession() }

// ---------------------------------------------------------------------------
// Server session
// ---------------------------------------------------------------------------

type serverSession struct {
	*session
}

func (s *serverSession) AcceptCall(ctx context.Context, _ framing.CallSpec) (framing.ServerCall, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.busy {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	s.busy = true
	s.mu.Unlock()

	releaseBusy := true
	defer func() {
		if releaseBusy {
			s.mu.Lock()
			s.busy = false
			s.mu.Unlock()
		}
	}()

	if err := s.waitFirstByte(ctx); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}

	openCtx := ctx
	var cancel context.CancelFunc
	if s.openTimeout > 0 {
		openCtx, cancel = context.WithTimeout(ctx, s.openTimeout)
		defer cancel()
	}

	v, err := s.readValue(openCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.markBad()
			return nil, fmt.Errorf("resp: OpenTimeout exceeded")
		}
		return nil, err
	}
	cmd, args, err := commandFromValue(v)
	if err != nil {
		s.markBad()
		return nil, err
	}
	cmdUp := strings.ToUpper(cmd)
	switch cmdUp {
	case "PING", "GET", "SET":
		// ok
	default:
		_ = s.writeRaw(EncodeError("ERR unknown command '" + cmd + "'"))
		return nil, fmt.Errorf("%w: %w", framing.ErrCallRejected,
			status.Error(status.Unimplemented, "resp: unknown command "+cmd))
	}

	full := methodFullName(s.service, cmdUp)
	c := newCall(s.session, full, cmdUp, false)
	c.args = args
	releaseBusy = false
	return &serverCall{call: c}, nil
}

func (s *serverSession) Close() error { return s.closeSession() }

// endCall releases the in-flight slot. poison marks the session non-reusable
// (carrier hygiene: closed without reading a terminal reply).
func (s *session) endCall(poison bool) {
	s.mu.Lock()
	s.busy = false
	if poison {
		s.reusable = false
	}
	s.mu.Unlock()
}
