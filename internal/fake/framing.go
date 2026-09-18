package fake

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

var (
	_ session.Framing       = (*Framing)(nil)
	_ session.ClientSession = (*ClientSession)(nil)
	_ session.ServerSession = (*ServerSession)(nil)
	_ session.Call          = (*Call)(nil)
	_ session.ServerCall    = (*ServerCall)(nil)
)

const (
	frameOpen   byte = 1
	frameData   byte = 2
	frameEnd    byte = 3
	frameStatus byte = 4
)

func writeFrame(w io.Writer, typ byte, callID uint32, payload []byte) error {
	body := make([]byte, 1+4+len(payload))
	body[0] = typ
	binary.BigEndian.PutUint32(body[1:5], callID)
	copy(body[5:], payload)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

func readFrame(r io.Reader, buf []byte) (typ byte, callID uint32, payload, rest []byte, err error) {
	need := func(n int) error {
		for len(buf) < n {
			tmp := make([]byte, 4096)
			nr, e := r.Read(tmp)
			if nr > 0 {
				buf = append(buf, tmp[:nr]...)
			}
			if e != nil {
				if len(buf) >= n {
					return nil
				}
				if e == io.EOF && len(buf) == 0 {
					return io.EOF
				}
				if e == io.EOF {
					return io.ErrUnexpectedEOF
				}
				return e
			}
		}
		return nil
	}
	if err := need(4); err != nil {
		return 0, 0, nil, buf, err
	}
	n := int(binary.BigEndian.Uint32(buf[:4]))
	buf = buf[4:]
	if err := need(n); err != nil {
		hdr := make([]byte, 4)
		binary.BigEndian.PutUint32(hdr, uint32(n))
		buf = append(hdr, buf...)
		return 0, 0, nil, buf, err
	}
	raw := buf[:n]
	rest = buf[n:]
	if len(rest) == 0 {
		rest = nil
	} else {
		cp := make([]byte, len(rest))
		copy(cp, rest)
		rest = cp
	}
	if len(raw) < 5 {
		return 0, 0, nil, rest, io.ErrUnexpectedEOF
	}
	typ = raw[0]
	callID = binary.BigEndian.Uint32(raw[1:5])
	payload = append([]byte(nil), raw[5:]...)
	return typ, callID, payload, rest, nil
}

// Framing is a configurable fake session.Framing for in-process tests.
type Framing struct {
	Model session.ReuseModel

	// Handshake runs during New*Session; may block until ctx is done.
	Handshake func(ctx context.Context, c transport.Conn) error

	// OpenCallHook is invoked before allocating a call (1-based callSeq).
	// Return session.ErrSessionBusy / ErrSessionSpent to script pool fallbacks.
	OpenCallHook func(callSeq int) error

	// AcceptCallHook is invoked after reading OPEN (1-based callSeq).
	// Return session.ErrCallRejected to script accept-loop continue paths.
	AcceptCallHook func(callSeq int) error

	// Hygiene, when true (default), marks Reusable false if Close without terminal.
	Hygiene bool

	// SessionID is copied onto sessions for hook correlation.
	SessionID int
}

// NewFraming returns a Framing with the given reuse model and Hygiene enabled.
func NewFraming(model session.ReuseModel) *Framing {
	return &Framing{Model: model, Hygiene: true}
}

// Reuse implements session.Framing.
func (f *Framing) Reuse() session.ReuseModel { return f.Model }

// NewClientSession implements session.Framing.
func (f *Framing) NewClientSession(ctx context.Context, c transport.Conn, _ session.SessionSpec) (session.ClientSession, error) {
	if err := f.handshake(ctx, c); err != nil {
		return nil, err
	}
	s := &ClientSession{
		framing:  f,
		conn:     c,
		reusable: true,
		id:       f.SessionID,
	}
	switch f.Model {
	case session.Concurrent:
		sc, ok := c.(transport.StreamConn)
		if !ok {
			return nil, fmt.Errorf("fake: Concurrent client requires StreamConn")
		}
		s.stream = sc
	default:
		cc, ok := c.(transport.CarrierConn)
		if !ok {
			return nil, fmt.Errorf("fake: Sequential/OneCall client requires CarrierConn")
		}
		car, ok := cc.Carrier().(transport.ByteStreamCarrier)
		if !ok {
			return nil, fmt.Errorf("fake: Carrier must be ByteStreamCarrier")
		}
		s.carrier = car
		s.carrierConn = cc
	}
	return s, nil
}

// NewServerSession implements session.Framing.
func (f *Framing) NewServerSession(ctx context.Context, c transport.Conn, _ session.SessionSpec) (session.ServerSession, error) {
	if err := f.handshake(ctx, c); err != nil {
		return nil, err
	}
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("fake: server requires CarrierConn")
	}
	car, ok := cc.Carrier().(transport.ByteStreamCarrier)
	if !ok {
		return nil, fmt.Errorf("fake: Carrier must be ByteStreamCarrier")
	}
	return &ServerSession{
		framing:     f,
		conn:        c,
		carrierConn: cc,
		carrier:     car,
		id:          f.SessionID,
	}, nil
}

func (f *Framing) handshake(ctx context.Context, c transport.Conn) error {
	if f.Handshake == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return f.Handshake(ctx, c)
}

// ClientSession is the fake client session.
type ClientSession struct {
	framing *Framing
	conn    transport.Conn
	id      int

	stream      transport.StreamConn
	carrierConn transport.CarrierConn
	carrier     transport.ByteStreamCarrier

	mu       sync.Mutex
	buf      []byte
	reusable bool
	closed   bool
	inFlight int
	spent    bool
	callSeq  int
	nextID   uint32
}

// MarkBad forces Reusable() to false (pool / fault tests).
func (s *ClientSession) MarkBad() {
	s.mu.Lock()
	s.reusable = false
	s.mu.Unlock()
}

// Reusable implements session.ClientSession.
func (s *ClientSession) Reusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.reusable || s.spent {
		return false
	}
	if bc, ok := s.conn.(*ByteConn); ok && bc.Aborted() {
		return false
	}
	return true
}

// Close implements session.ClientSession.
func (s *ClientSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.reusable = false
	s.mu.Unlock()
	return s.conn.Close()
}

// OpenCall implements session.ClientSession.
func (s *ClientSession) OpenCall(ctx context.Context, m descriptor.Method, _ session.CallSpec) (session.Call, error) {
	s.mu.Lock()
	if s.closed || !s.reusable {
		s.mu.Unlock()
		return nil, errors.New("fake: session not reusable")
	}
	if s.framing.Model == session.OneCallPerConn && s.spent {
		s.mu.Unlock()
		return nil, session.ErrSessionSpent
	}
	if s.framing.Model != session.Concurrent && s.inFlight > 0 {
		s.mu.Unlock()
		return nil, session.ErrSessionBusy
	}
	s.callSeq++
	seq := s.callSeq
	hook := s.framing.OpenCallHook
	s.mu.Unlock()

	if hook != nil {
		if err := hook(seq); err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var (
		car    transport.ByteStreamCarrier
		callID uint32
		local  bool
	)

	s.mu.Lock()
	if s.framing.Model != session.Concurrent && s.inFlight > 0 {
		s.mu.Unlock()
		return nil, session.ErrSessionBusy
	}
	if s.framing.Model == session.OneCallPerConn {
		s.spent = true
	}
	s.inFlight++
	s.nextID++
	callID = s.nextID
	s.mu.Unlock()

	if s.stream != nil {
		c, err := s.stream.OpenStream(ctx, transport.RequestPreface{
			RequestTarget: "/" + m.FullName(),
		})
		if err != nil {
			s.endFlight()
			return nil, err
		}
		bc, ok := c.(transport.ByteStreamCarrier)
		if !ok {
			_ = c.Abort()
			s.endFlight()
			return nil, fmt.Errorf("fake: OpenStream Carrier is not ByteStreamCarrier")
		}
		car = bc
		local = true
		callID = 1
	} else {
		car = s.carrier
	}

	if err := writeFrame(car, frameOpen, callID, []byte(m.FullName())); err != nil {
		s.MarkBad()
		s.endFlight()
		return nil, err
	}

	call := &Call{
		sess:     s,
		carrier:  car,
		callID:   callID,
		method:   m.FullName(),
		localCar: local,
		hygiene:  s.framing.Hygiene,
		inbox:    make(chan recvItem, 8),
		done:     make(chan struct{}),
	}
	call.startRecv()
	return call, nil
}

func (s *ClientSession) endFlight() {
	s.mu.Lock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	s.mu.Unlock()
}

type recvItem struct {
	payload []byte
	err     error
}

// Call is one fake framing call.
type Call struct {
	sess     *ClientSession
	server   *ServerSession
	carrier  transport.ByteStreamCarrier
	callID   uint32
	method   string
	localCar bool
	hygiene  bool
	inbox    chan recvItem

	mu       sync.Mutex
	terminal bool
	closed   bool
	recvDone chan struct{}
	stopRecv context.CancelFunc
	half     bool

	// done is closed by Close. recvLoop never closes inbox, so a Recv parked on
	// it would otherwise stay blocked forever — Close must unblock in-flight
	// Recv/Send (session.Call).
	done chan struct{}
}

// errCallClosed is a local, non-status error: a resolved remote status must
// stay distinguishable from a read unblocked by Close.
var errCallClosed = errors.New("fake: call closed")

func (c *Call) startRecv() {
	ctx, cancel := context.WithCancel(context.Background())
	c.stopRecv = cancel
	c.recvDone = make(chan struct{})
	go func() {
		defer close(c.recvDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			typ, id, payload, err := c.readOne()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				select {
				case c.inbox <- recvItem{err: err}:
				case <-ctx.Done():
				}
				return
			}
			if id != c.callID {
				// Concurrent: ignore other streams on shared carrier (should not happen for Sequential).
				continue
			}
			switch typ {
			case frameData:
				select {
				case c.inbox <- recvItem{payload: payload}:
				case <-ctx.Done():
					return
				}
			case frameEnd, frameStatus:
				select {
				case c.inbox <- recvItem{payload: nil}: // terminal → Recv returns EOF
				case <-ctx.Done():
				}
				return
			case frameOpen:
				// unexpected on client
				continue
			default:
				continue
			}
		}
	}()
}

func (c *Call) readOne() (typ byte, id uint32, payload []byte, err error) {
	if c.sess != nil {
		c.sess.mu.Lock()
		buf := c.sess.buf
		c.sess.buf = nil
		c.sess.mu.Unlock()
		typ, id, payload, rest, err := readFrame(c.carrier, buf)
		c.sess.mu.Lock()
		c.sess.buf = rest
		c.sess.mu.Unlock()
		return typ, id, payload, err
	}
	c.server.mu.Lock()
	buf := c.server.buf
	c.server.buf = nil
	c.server.mu.Unlock()
	typ, id, payload, rest, err := readFrame(c.carrier, buf)
	c.server.mu.Lock()
	c.server.buf = rest
	c.server.mu.Unlock()
	return typ, id, payload, err
}

// Method implements session.Call.
func (c *Call) Method() string { return c.method }

// Deadline implements session.Call.
func (c *Call) Deadline() (time.Time, bool) { return time.Time{}, false }

// SendHeaders implements session.Call.
func (c *Call) SendHeaders() error {
	return status.Error(status.Unimplemented, "fake: SendHeaders not supported")
}

// Recv implements session.Call.
func (c *Call) Recv() (payload []byte, release func(), err error) {
	// A buffered item wins over the close signal so an already-delivered
	// message is never dropped; the close signal then guarantees Close unblocks
	// a parked Recv, since recvLoop never closes inbox.
	select {
	case it, ok := <-c.inbox:
		return c.handleRecvItem(it, ok)
	default:
	}
	select {
	case it, ok := <-c.inbox:
		return c.handleRecvItem(it, ok)
	case <-c.done:
		return nil, nil, errCallClosed
	}
}

func (c *Call) handleRecvItem(it recvItem, ok bool) (payload []byte, release func(), err error) {
	if !ok {
		return nil, nil, io.EOF
	}
	if it.err != nil {
		if it.err != io.EOF {
			c.markBad()
		} else {
			c.mu.Lock()
			c.terminal = true
			c.mu.Unlock()
		}
		return nil, nil, it.err
	}
	if it.payload == nil {
		c.mu.Lock()
		c.terminal = true
		c.mu.Unlock()
		return nil, nil, io.EOF
	}
	p := it.payload
	return p, func() {}, nil
}

// Send implements session.Call.
func (c *Call) Send(payload []byte) error {
	if err := writeFrame(c.carrier, frameData, c.callID, payload); err != nil {
		c.markBad()
		return err
	}
	return nil
}

// HalfClose implements session.Call.
func (c *Call) HalfClose() error {
	c.mu.Lock()
	if c.half {
		c.mu.Unlock()
		return nil
	}
	c.half = true
	c.mu.Unlock()
	if err := writeFrame(c.carrier, frameEnd, c.callID, nil); err != nil {
		c.markBad()
		return err
	}
	return nil
}

// Finish implements session.Call (responder).
func (c *Call) Finish(err error) error {
	code := uint32(status.OK)
	msg := ""
	if err != nil {
		code = uint32(status.CodeOf(err))
		msg = err.Error()
	}
	p := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(p[:4], code)
	copy(p[4:], msg)
	if werr := writeFrame(c.carrier, frameStatus, c.callID, p); werr != nil {
		c.markBad()
		return werr
	}
	c.mu.Lock()
	c.terminal = true
	c.mu.Unlock()
	return nil
}

// Close implements session.Call. Does not close Conn/Carrier.
func (c *Call) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	terminal := c.terminal
	c.mu.Unlock()

	// Unblock a parked Recv before waiting for the receive goroutine, which
	// exits without closing inbox.
	select {
	case <-c.done:
	default:
		close(c.done)
	}

	if c.stopRecv != nil {
		c.stopRecv()
	}
	c.wakeRead()
	if c.recvDone != nil {
		<-c.recvDone
	}
	c.clearDeadline()

	if c.hygiene && !terminal {
		c.markBad()
	}
	if c.localCar {
		_ = c.carrier.Abort()
	}
	if c.sess != nil {
		c.sess.endFlight()
	}
	if c.server != nil {
		c.server.endFlight()
	}
	return nil
}

func (c *Call) markBad() {
	if c.sess != nil {
		c.sess.MarkBad()
	}
	if c.server != nil {
		c.server.MarkBad()
	}
}

func (c *Call) wakeRead() {
	type deadliner interface {
		SetReadDeadline(time.Time) error
	}
	if d, ok := c.carrier.(deadliner); ok {
		_ = d.SetReadDeadline(time.Now())
		return
	}
	if bc, ok := c.carrier.(*ByteConn); ok {
		_ = bc.SetReadDeadline(time.Now())
	}
}

func (c *Call) clearDeadline() {
	type deadliner interface {
		SetReadDeadline(time.Time) error
	}
	if d, ok := c.carrier.(deadliner); ok {
		_ = d.SetReadDeadline(time.Time{})
		return
	}
	if bc, ok := c.carrier.(*ByteConn); ok {
		_ = bc.SetReadDeadline(time.Time{})
	}
}

// ServerSession is the fake server session.
type ServerSession struct {
	framing     *Framing
	conn        transport.Conn
	carrierConn transport.CarrierConn
	carrier     transport.ByteStreamCarrier
	id          int

	mu       sync.Mutex
	buf      []byte
	closed   bool
	inFlight int
	callSeq  int
	spent    bool
	reusable bool
	bad      bool
}

// MarkBad forces the session unhealthy for AcceptCall loops / tests.
func (s *ServerSession) MarkBad() {
	s.mu.Lock()
	s.bad = true
	s.reusable = false
	s.mu.Unlock()
}

func (s *ServerSession) endFlight() {
	s.mu.Lock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	s.mu.Unlock()
}

// Close implements session.ServerSession.
func (s *ServerSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	return s.conn.Close()
}

// AcceptCall implements session.ServerSession.
func (s *ServerSession) AcceptCall(ctx context.Context, _ session.CallSpec) (session.ServerCall, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.framing.Model == session.OneCallPerConn && s.spent {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.framing.Model != session.Concurrent && s.inFlight > 0 {
		s.mu.Unlock()
		return nil, session.ErrSessionBusy
	}
	s.mu.Unlock()

	type result struct {
		typ     byte
		id      uint32
		payload []byte
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		s.mu.Lock()
		buf := s.buf
		s.buf = nil
		s.mu.Unlock()
		typ, id, payload, rest, err := readFrame(s.carrier, buf)
		s.mu.Lock()
		s.buf = rest
		s.mu.Unlock()
		ch <- result{typ, id, payload, err}
	}()

	var res result
	select {
	case <-ctx.Done():
		// Wake the blocked read so the goroutine can exit.
		if bc, ok := s.carrier.(*ByteConn); ok {
			_ = bc.SetReadDeadline(time.Now())
		}
		res = <-ch
		if bc, ok := s.carrier.(*ByteConn); ok {
			_ = bc.SetReadDeadline(time.Time{})
		}
		if res.err == nil && res.typ == frameOpen {
			// Put OPEN back into buffer for a later AcceptCall.
			raw := encodeRaw(res.typ, res.id, res.payload)
			s.mu.Lock()
			s.buf = append(raw, s.buf...)
			s.mu.Unlock()
		}
		return nil, ctx.Err()
	case res = <-ch:
	}

	if res.err != nil {
		if res.err == io.EOF {
			return nil, io.EOF
		}
		s.MarkBad()
		return nil, res.err
	}
	if res.typ != frameOpen {
		s.MarkBad()
		return nil, fmt.Errorf("fake: expected OPEN, got type %d", res.typ)
	}

	s.mu.Lock()
	s.callSeq++
	seq := s.callSeq
	hook := s.framing.AcceptCallHook
	if s.framing.Model == session.OneCallPerConn {
		s.spent = true
	}
	s.inFlight++
	s.mu.Unlock()

	if hook != nil {
		if err := hook(seq); err != nil {
			s.endFlight()
			return nil, err
		}
	}

	call := &Call{
		server:  s,
		carrier: s.carrier,
		callID:  res.id,
		method:  string(res.payload),
		hygiene: s.framing.Hygiene,
		inbox:   make(chan recvItem, 8),
		done:    make(chan struct{}),
	}
	// Promote to ServerCall wrapper.
	sc := &ServerCall{Call: call}
	call.startRecv()
	return sc, nil
}

func encodeRaw(typ byte, callID uint32, payload []byte) []byte {
	body := make([]byte, 1+4+len(payload))
	body[0] = typ
	binary.BigEndian.PutUint32(body[1:5], callID)
	copy(body[5:], payload)
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out[:4], uint32(len(body)))
	copy(out[4:], body)
	return out
}

// ServerCall embeds Call and adds Accept.
type ServerCall struct {
	*Call
}

// Accept implements session.ServerCall.
func (c *ServerCall) Accept(m descriptor.Method) error {
	if m.FullName() != c.method {
		return fmt.Errorf("fake: method mismatch: got %q want %q", m.FullName(), c.method)
	}
	return nil
}
