package resp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// session is the shared sequential state for one TCP connection.
type session struct {
	axis    *Transport
	conn    transport.Conn
	carrier transport.ByteStreamCarrier
	client  bool

	openTimeout time.Duration
	service     string
	password    string

	writeMu sync.Mutex

	mu        sync.Mutex
	closed    bool
	reusable  bool
	exclusive bool // SUBSCRIBE (or other exclusive protocol state)
	busy      bool // one in-flight call
	readBuf   []byte
	readErr   error
	readWait  chan struct{} // closed+replaced when new data/err arrives

	readerDone chan struct{}
	closeCh    chan struct{}
	peerGone   chan struct{} // closed once when the carrier read fails or session closes
	peerOnce   sync.Once
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

func newSession(a *Transport, conn transport.Conn, car transport.ByteStreamCarrier, client bool) *session {
	s := &session{
		axis:        a,
		conn:        conn,
		carrier:     car,
		client:      client,
		openTimeout: a.openTimeout,
		service:     a.service,
		password:    a.password,
		reusable:    true,
		readWait:    make(chan struct{}),
		readerDone:  make(chan struct{}),
		closeCh:     make(chan struct{}),
		peerGone:    make(chan struct{}),
	}
	go s.readLoop()
	return s
}

func (s *session) tripPeerGone() {
	s.peerOnce.Do(func() { close(s.peerGone) })
}

func (s *session) readLoop() {
	defer close(s.readerDone)
	defer s.tripPeerGone()
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
			// The carrier is gone for good: no further byte can arrive and no
			// further broadcast is coming. Terminate the session for waiters
			// here — otherwise a reader that parks after this broadcast waits
			// on a wake channel nothing will ever close again, holding the
			// accept goroutine and its buffers — and clear reusable so a pool
			// stops handing out a session that can never carry another call.
			s.reusable = false
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
	return s.reusable && !s.closed && !s.exclusive
}

func (s *session) markBad() {
	s.mu.Lock()
	s.reusable = false
	s.mu.Unlock()
}

func (s *session) markExclusive() {
	s.mu.Lock()
	s.exclusive = true
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
	s.tripPeerGone()
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
			// The buffer holds only part of a value. Only more bytes can
			// complete it, and none can arrive once the read loop has failed or
			// the session is closing: surface the failure instead of parking on
			// a wake channel that broadcastReadLocked replaces on every read
			// and, with the loop gone, would never be closed again.
			if s.readErr != nil {
				err := s.readErr
				s.mu.Unlock()
				return Value{}, err
			}
			if s.closed {
				s.mu.Unlock()
				return Value{}, io.EOF
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
// Handshake
// ---------------------------------------------------------------------------

func (s *session) clientHandshake(ctx context.Context) error {
	if err := s.writeCommand("HELLO", "2"); err != nil {
		return err
	}
	s.axis.hellos.Add(1)
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

type clientConn struct {
	*session
}

func (s *clientConn) OpenCall(ctx context.Context, m descriptor.Method, _ transport.CallSpec) (transport.Call, error) {
	if m.IsZero() {
		return nil, status.Error(status.InvalidArgument, "resp: zero Method")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	if s.closed || !s.reusable || s.exclusive {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "resp: session not reusable")
	}
	if s.busy {
		s.mu.Unlock()
		return nil, transport.ErrConnBusy
	}
	s.busy = true
	s.mu.Unlock()

	// Initiator: the ctx handed to OpenCall carries the caller's deadline and
	// cancellation. Inheriting it is what keeps a call from outliving the
	// deadline its caller set and parking forever on a peer that never replies.
	c := newCall(s.session, m.FullName(), m.Name(), true, ctx)
	if c.streaming {
		s.markExclusive()
	}
	return c, nil
}

func (s *clientConn) Close() error { return s.closeSession() }

// ---------------------------------------------------------------------------
// Server session
// ---------------------------------------------------------------------------

// serverConn is one inbound connection. It is handed to the composition layer
// before handshake, so Handshake is where the session (and its read loop)
// starts.
type serverConn struct {
	axis *Transport
	conn transport.Conn
	*session
}

// Handshake implements transport.ServerConn.
func (s *serverConn) Handshake(ctx context.Context) error {
	car, err := assertByteStream(s.conn)
	if err != nil {
		return err
	}
	sess := newSession(s.axis, s.conn, car, false)
	if err := sess.serverHandshake(ctx); err != nil {
		_ = sess.closeSession()
		return err
	}
	s.session = sess
	return nil
}

func (s *serverConn) AcceptCall(ctx context.Context, _ transport.CallSpec) (transport.ServerCall, error) {
	if s.session == nil {
		return nil, fmt.Errorf("resp: AcceptCall before Handshake")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.busy {
		s.mu.Unlock()
		return nil, transport.ErrConnBusy
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
	case "PING", "GET", "SET", "PUBLISH", "SUBSCRIBE":
		// ok
	default:
		// ErrCallRejected must come back with a ServerCall the composition
		// layer can Finish: Finish is what writes the trailing status, and it
		// already encodes a responder's error as a RESP error. Writing the
		// error here and returning no call would leave Finish nothing to act
		// on, so the rejection would reach the client with no wire status.
		c := newCall(s.session, methodFullName(s.service, cmdUp), cmdUp, false, context.Background())
		// The returned ServerCall owns the in-flight slot from here, exactly as
		// on the success path: Close releases it via endCall. Leaving the defer
		// to release it as well would free the slot while the composition layer
		// still holds a live call.
		releaseBusy = false
		return &serverCall{call: c}, fmt.Errorf("%w: %w", transport.ErrCallRejected,
			status.Error(status.Unimplemented, "unknown command '"+cmd+"'"))
	}

	full := methodFullName(s.service, cmdUp)
	// Responder: parent the call ctx on Background rather than on the accept ctx
	// — the composition layer cancels that one as soon as AcceptCall returns
	// (server/conn.go waitCancel), so inheriting it would cancel the handler at
	// birth — and RESP2 has no inbound deadline channel to carry one instead.
	c := newCall(s.session, full, cmdUp, false, context.Background())
	c.args = args
	if c.streaming {
		s.markExclusive()
		// Sequential accept goroutine == handleCall goroutine; hand off peerGone
		// so SUBSCRIBE can unblock when the client closes the TCP connection.
		bindPeerGone(goroutineID(), s.peerGone)
	}
	releaseBusy = false
	return &serverCall{call: c}, nil
}

func (s *serverConn) Close() error {
	if s.session == nil {
		// Handshake never got far enough to own the Conn.
		return s.conn.Close()
	}
	return s.closeSession()
}

// endCall releases the in-flight slot. poison marks the session non-reusable
// (carrier hygiene: closed without reading a terminal reply). Exclusive
// sessions (SUBSCRIBE) stay non-reusable so the pool closes the connection.
func (s *session) endCall(poison bool) {
	s.mu.Lock()
	s.busy = false
	if poison || s.exclusive {
		s.reusable = false
	}
	s.mu.Unlock()
	clearPeerGone(goroutineID())
}

// peerGoneHandoff lets the SUBSCRIBE handler observe carrier death without a
// core API change (Sequential: accept and handle share one goroutine).
var peerGoneHandoff sync.Map // goid → <-chan struct{}

func bindPeerGone(goid uint64, ch chan struct{}) {
	peerGoneHandoff.Store(goid, ch)
}

func clearPeerGone(goid uint64) {
	peerGoneHandoff.Delete(goid)
}

// PeerGone returns a channel closed when the current Sequential accept
// goroutine's carrier read fails or the session closes. Only set during
// an in-flight SUBSCRIBE on the server.
func PeerGone() <-chan struct{} {
	if v, ok := peerGoneHandoff.Load(goroutineID()); ok {
		return v.(chan struct{})
	}
	return nil
}

func goroutineID() uint64 {
	var buf [32]byte
	n := runtime.Stack(buf[:], false)
	s := string(buf[:n])
	s = strings.TrimPrefix(s, "goroutine ")
	if i := strings.IndexByte(s, ' '); i > 0 {
		s = s[:i]
	}
	id, _ := strconv.ParseUint(s, 10, 64)
	return id
}
