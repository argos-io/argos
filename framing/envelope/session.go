package envelope

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

type carrierKind int

const (
	kindByteStream carrierKind = iota
	kindMessage
)

type demuxMode int

const (
	modeIdle demuxMode = iota
	modeAccepting
	modeInCall
)

type acceptResult struct {
	frame Frame
	err   error
}

// session is the shared Sequential session state for client and server.
type session struct {
	framing *Framing
	conn    transport.Conn
	carrier transport.Carrier
	kind    carrierKind
	cfg     framing.Config
	client  bool

	writeMu sync.Mutex

	mu       sync.Mutex
	closed   bool
	reusable bool
	ioError  bool

	mode       demuxMode
	active     *call
	lastCallID uint64
	nextCallID uint64

	acceptCtx context.Context
	acceptCh  chan acceptResult

	// Cross-call leftover for ByteStreamCarrier length-prefix framing.
	readBuf []byte

	// wake notifies recvLoop that mode/active/accept changed or session closed.
	wake chan struct{}

	recvDone chan struct{}
}

func newSession(f *Framing, conn transport.Conn, car transport.Carrier, kind carrierKind, cfg framing.Config, client bool) *session {
	s := &session{
		framing:    f,
		conn:       conn,
		carrier:    car,
		kind:       kind,
		cfg:        cfg,
		client:     client,
		reusable:   true,
		nextCallID: 1,
		wake:       make(chan struct{}, 1),
		recvDone:   make(chan struct{}),
	}
	go s.recvLoop()
	return s
}

func (s *session) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *session) markBadLocked() {
	s.reusable = false
	s.ioError = true
}

func (s *session) markBad() {
	s.mu.Lock()
	s.markBadLocked()
	s.mu.Unlock()
}

func (s *session) Reusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reusable && !s.closed && !s.ioError
}

func (s *session) closeSession() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.reusable = false
	active := s.active
	acceptCh := s.acceptCh
	s.mu.Unlock()

	s.signal()
	s.wakeRead()
	if acceptCh != nil {
		select {
		case acceptCh <- acceptResult{err: io.EOF}:
		default:
		}
	}
	if active != nil {
		active.abortRecv(errors.New("envelope: session closed"))
	}
	<-s.recvDone
	s.clearReadDeadline()
	return s.conn.Close()
}

func (s *session) recvLoop() {
	defer close(s.recvDone)
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		mode := s.mode
		acceptCtx := s.acceptCtx
		active := s.active
		lastID := s.lastCallID
		s.mu.Unlock()

		switch mode {
		case modeIdle:
			<-s.wake
			continue

		case modeAccepting:
			f, err := s.readFrameAccepting(acceptCtx, lastID)
			if err != nil {
				s.mu.Lock()
				if s.closed {
					s.mu.Unlock()
					return
				}
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					if !errors.Is(err, io.EOF) {
						s.markBadLocked()
					} else {
						s.reusable = false
					}
				}
				ch := s.acceptCh
				s.mu.Unlock()
				if ch != nil {
					select {
					case ch <- acceptResult{err: err}:
					default:
					}
				}
				<-s.wake
				continue
			}
			s.mu.Lock()
			ch := s.acceptCh
			s.mu.Unlock()
			if ch != nil {
				// Channel is buffered (1). Never drop OPEN on a wake race.
				select {
				case ch <- acceptResult{frame: f}:
				default:
					// AcceptCall gone; keep frame for a later AcceptCall.
					s.pushFront(f)
				}
			} else {
				s.pushFront(f)
			}
			<-s.wake

		case modeInCall:
			if active == nil {
				<-s.wake
				continue
			}
			f, err := s.readFrame()
			if err != nil {
				if s.isClosed() {
					return
				}
				s.mu.Lock()
				cur := s.active
				mode := s.mode
				s.mu.Unlock()
				s.clearReadDeadline()
				if cur == nil || mode != modeInCall {
					// Call.Close detached us; the deadline wake is expected.
					<-s.wake
					continue
				}
				cur.abortRecv(err)
				s.markBad()
				<-s.wake
				continue
			}
			s.mu.Lock()
			cur := s.active
			s.mu.Unlock()
			if cur == nil {
				s.pushFront(f)
				<-s.wake
				continue
			}
			if f.CallID != cur.callID {
				cur.abortRecv(fmt.Errorf("envelope: unexpected call ID %d want %d", f.CallID, cur.callID))
				s.markBad()
				<-s.wake
				continue
			}
			cur.deliver(f)
			if cur.recvFinished() {
				<-s.wake
			}
		}
	}
}

func (s *session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *session) readFrameAccepting(ctx context.Context, lastCallID uint64) (Frame, error) {
	var drained int64
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return Frame{}, ctx.Err()
			default:
			}
		}
		// Drain residual frames belonging to the finished call.
		f, err := s.readFrameFirstByteThenTimeout(ctx)
		if err != nil {
			return Frame{}, err
		}
		if lastCallID != 0 && f.CallID == lastCallID && f.Type != TypeOpen {
			n, _ := frameWireSize(f, s.kind)
			drained += n
			max := s.framing.maxDrainBytes
			if max > 0 && drained > max {
				return Frame{}, fmt.Errorf("envelope: residual drain exceeded MaxDrainBytes (%d)", max)
			}
			continue
		}
		return f, nil
	}
}

func frameWireSize(f Frame, kind carrierKind) (int64, error) {
	if kind == kindMessage {
		body, err := MarshalFrameBody(f)
		return int64(len(body)), err
	}
	raw, err := MarshalFrame(f)
	return int64(len(raw)), err
}

func (s *session) readFrameFirstByteThenTimeout(ctx context.Context) (Frame, error) {
	if s.kind == kindMessage {
		return s.readMessageFrame(ctx, true)
	}
	// Byte stream: wait for ≥1 byte with only accept ctx, then OpenTimeout.
	if err := s.waitFirstByte(ctx); err != nil {
		return Frame{}, err
	}
	to := s.framing.openTimeout
	if to > 0 {
		s.setReadDeadline(time.Now().Add(to))
		defer s.clearReadDeadline()
	}
	return s.readFrame()
}

func (s *session) waitFirstByte(ctx context.Context) error {
	s.mu.Lock()
	if len(s.readBuf) > 0 {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	bs, ok := s.carrier.(transport.ByteStreamCarrier)
	if !ok {
		return fmt.Errorf("envelope: ByteStreamCarrier required")
	}

	var one [1]byte
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				s.clearReadDeadline()
				return ctx.Err()
			default:
			}
		}
		// Short deadline so accept-ctx cancel is observed promptly without a
		// second concurrent Read on the carrier (net.Pipe deadlocks on that).
		s.setReadDeadline(time.Now().Add(20 * time.Millisecond))
		n, err := bs.Read(one[:])
		s.clearReadDeadline()
		if n > 0 {
			s.mu.Lock()
			s.readBuf = append([]byte{one[0]}, s.readBuf...)
			s.mu.Unlock()
			return nil
		}
		if err == nil {
			continue
		}
		if isTimeoutErr(err) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return err
	}
}

func isTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func (s *session) readFrame() (Frame, error) {
	if s.kind == kindMessage {
		return s.readMessageFrame(context.Background(), false)
	}
	return s.readPrefixedFrame()
}

func (s *session) readPrefixedFrame() (Frame, error) {
	bs, ok := s.carrier.(transport.ByteStreamCarrier)
	if !ok {
		return Frame{}, fmt.Errorf("envelope: ByteStreamCarrier required")
	}
	r := &bufReader{s: s, r: bs}
	return UnmarshalPrefixed(r)
}

func (s *session) readMessageFrame(ctx context.Context, honorCtx bool) (Frame, error) {
	mc, ok := s.carrier.(transport.MessageCarrier)
	if !ok {
		return Frame{}, fmt.Errorf("envelope: MessageCarrier required")
	}
	type result struct {
		msg []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		msg, err := mc.RecvMessage()
		ch <- result{msg, err}
	}()
	var res result
	if honorCtx && ctx != nil {
		select {
		case <-ctx.Done():
			// MessageCarrier has no deadline wake; Close/Abort on session handles hard stop.
			select {
			case res = <-ch:
				// got it anyway
			case <-time.After(50 * time.Millisecond):
				return Frame{}, ctx.Err()
			}
			if res.err != nil && res.msg == nil {
				return Frame{}, ctx.Err()
			}
		case res = <-ch:
		}
	} else {
		res = <-ch
	}
	if res.err != nil {
		return Frame{}, res.err
	}
	return ParseFrameBody(res.msg)
}

func (s *session) pushFront(f Frame) {
	var raw []byte
	var err error
	if s.kind == kindMessage {
		// Message mode cannot unread; mark bad.
		_ = err
		s.markBad()
		return
	}
	raw, err = MarshalFrame(f)
	if err != nil {
		s.markBad()
		return
	}
	s.mu.Lock()
	s.readBuf = append(raw, s.readBuf...)
	s.mu.Unlock()
}

func (s *session) writeFrame(f Frame) error {
	if err := f.Validate(s.cfg.MaxFrameSize, s.cfg.MaxMetadataSize); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	switch s.kind {
	case kindByteStream:
		raw, err := MarshalFrame(f)
		if err != nil {
			return err
		}
		bs := s.carrier.(transport.ByteStreamCarrier)
		_, err = bs.Write(raw)
		return err
	case kindMessage:
		body, err := MarshalFrameBody(f)
		if err != nil {
			return err
		}
		mc := s.carrier.(transport.MessageCarrier)
		return mc.SendMessage(body)
	default:
		return fmt.Errorf("envelope: unknown carrier kind")
	}
}

type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

func (s *session) setReadDeadline(t time.Time) {
	if d, ok := s.carrier.(readDeadliner); ok {
		_ = d.SetReadDeadline(t)
		return
	}
	type deadliner interface {
		SetDeadline(time.Time) error
	}
	if d, ok := s.carrier.(deadliner); ok {
		_ = d.SetDeadline(t)
	}
}

func (s *session) clearReadDeadline() {
	s.setReadDeadline(time.Time{})
}

func (s *session) wakeRead() {
	s.setReadDeadline(time.Now().Add(-time.Second))
}

// bufReader reads from session.readBuf then the carrier.
type bufReader struct {
	s *session
	r io.Reader
}

func (b *bufReader) Read(p []byte) (int, error) {
	b.s.mu.Lock()
	if len(b.s.readBuf) > 0 {
		n := copy(p, b.s.readBuf)
		b.s.readBuf = b.s.readBuf[n:]
		if len(b.s.readBuf) == 0 {
			b.s.readBuf = nil
		}
		b.s.mu.Unlock()
		return n, nil
	}
	b.s.mu.Unlock()
	return b.r.Read(p)
}

// ---------------------------------------------------------------------------
// Client / Server session wrappers
// ---------------------------------------------------------------------------

type clientSession struct {
	*session
}

func (s *clientSession) OpenCall(ctx context.Context, m descriptor.Method, spec framing.CallSpec) (framing.Call, error) {
	if m.IsZero() {
		return nil, status.Error(status.InvalidArgument, "envelope: zero Method")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.Lock()
	if s.closed || !s.reusable || s.ioError {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "envelope: session not reusable")
	}
	if s.mode != modeIdle || s.active != nil {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	id := s.nextCallID
	s.nextCallID++
	s.mu.Unlock()

	if spec.Metadata != nil {
		_ = metadata.FreezeOutgoingHeaders(spec.Metadata)
	}

	c := newCall(s.session, id, m.FullName(), true, spec.Metadata, m.Shape())

	// Attach before writing OPEN so a fast response cannot race the demux.
	s.mu.Lock()
	s.active = c
	s.mode = modeInCall
	s.mu.Unlock()
	s.signal()

	var hdrs []Header
	if spec.Metadata != nil {
		hdrs = mdToHeaders(spec.Metadata.OutgoingHeaders())
	}
	if err := s.writeFrame(Frame{
		Type:    TypeOpen,
		CallID:  id,
		Method:  m.FullName(),
		Headers: hdrs,
	}); err != nil {
		s.mu.Lock()
		if s.active == c {
			s.active = nil
			s.mode = modeIdle
		}
		s.markBadLocked()
		s.mu.Unlock()
		s.signal()
		return nil, err
	}
	c.openSent = true
	return c, nil
}

func (s *clientSession) Close() error { return s.closeSession() }

type serverSession struct {
	*session
}

func (s *serverSession) AcceptCall(ctx context.Context, spec framing.CallSpec) (framing.ServerCall, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.mode != modeIdle || s.active != nil {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	ch := make(chan acceptResult, 1)
	s.acceptCh = ch
	s.acceptCtx = ctx
	s.mode = modeAccepting
	s.mu.Unlock()
	s.signal()

	var res acceptResult
	select {
	case <-ctx.Done():
		s.wakeRead()
		select {
		case res = <-ch:
			if res.err == nil && res.frame.Type == TypeOpen {
				s.pushFront(res.frame)
			}
		case <-time.After(200 * time.Millisecond):
			res.err = ctx.Err()
		}
		s.clearAccept()
		s.clearReadDeadline()
		if res.err == nil {
			res.err = ctx.Err()
		}
		return nil, res.err
	case res = <-ch:
	}

	s.clearAccept()
	if res.err != nil {
		if errors.Is(res.err, io.EOF) {
			return nil, io.EOF
		}
		return nil, res.err
	}
	f := res.frame
	if f.Type != TypeOpen {
		s.markBad()
		return nil, fmt.Errorf("envelope: expected OPEN, got type %d", f.Type)
	}
	if f.Method == "" {
		return nil, fmt.Errorf("%w: %w", framing.ErrCallRejected,
			status.Error(status.InvalidArgument, "envelope: empty method in OPEN"))
	}

	if spec.Metadata != nil {
		_ = metadata.SetIncomingHeaders(spec.Metadata, headersToMD(f.Headers))
	}

	c := newCall(s.session, f.CallID, f.Method, false, spec.Metadata, 0)
	c.openSeen = true
	if f.Flags&FlagOpenEnd != 0 {
		c.peerHalfClosed = true
		c.deliverEnd()
	}

	s.mu.Lock()
	s.active = c
	s.lastCallID = f.CallID
	s.mode = modeInCall
	s.mu.Unlock()
	s.signal()

	return &serverCall{call: c}, nil
}

func (s *serverSession) clearAccept() {
	s.mu.Lock()
	s.acceptCh = nil
	s.acceptCtx = nil
	if s.mode == modeAccepting {
		s.mode = modeIdle
	}
	s.mu.Unlock()
	s.signal()
}

func (s *serverSession) Close() error { return s.closeSession() }

func headersToMD(hdrs []Header) metadata.Metadata {
	if len(hdrs) == 0 {
		return metadata.Metadata{}
	}
	md := make(metadata.Metadata, len(hdrs))
	for _, h := range hdrs {
		md[h.Name] = append(md[h.Name], h.Value)
	}
	return md
}

func mdToHeaders(md metadata.Metadata) []Header {
	if len(md) == 0 {
		return nil
	}
	var out []Header
	// Stable-ish order not required by wire format.
	for k, vs := range md {
		for _, v := range vs {
			out = append(out, Header{Name: k, Value: v})
		}
	}
	return out
}