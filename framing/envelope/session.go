package envelope

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/argos-io/argos/budget"
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
	kindDatagram
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

	openTimeout   time.Duration
	maxDrainBytes int64
	maxDatagram   int64

	writeMu sync.Mutex

	mu       sync.Mutex
	closed   bool
	reusable bool
	ioError  bool
	oneCall  bool // OneCallPerConn or DatagramCarrier
	spent    bool // oneCall: session already used

	mode       demuxMode
	active     *call
	lastCallID uint64
	nextCallID uint64

	acceptCtx context.Context
	acceptCh  chan acceptResult

	// Cross-call leftover for ByteStreamCarrier length-prefix framing.
	readBuf []byte
	// frameBuf accumulates the frame currently being read. It belongs to
	// recvLoop alone and survives a failed read, so a frame read is resumable.
	frameBuf []byte
	// discardRest is what is left of an oversize body that must leave the
	// carrier before the next frame boundary means anything.
	discardRest int64
	// datagramRest holds frames after OPEN from the request datagram until
	// AcceptCall attaches the server call and delivers them.
	datagramRest []Frame
	// pendingFrames holds frames the demux handed back on carriers that cannot
	// unread bytes (MessageCarrier); byte streams push back into readBuf.
	pendingFrames []Frame

	// stateVer counts published changes to the demux state (mode, active,
	// acceptCh, closed). wakeCh is closed and replaced by every publication,
	// so a waiter that captures it in the same critical section as the state
	// it acted on cannot miss a later change.
	stateVer uint64
	wakeCh   chan struct{}

	recvDone chan struct{}
}

func newSession(f *Framing, conn transport.Conn, car transport.Carrier, kind carrierKind, cfg framing.Config, client bool) *session {
	oneCall := kind == kindDatagram || f.reuse == framing.OneCallPerConn
	s := &session{
		framing:       f,
		conn:          conn,
		carrier:       car,
		kind:          kind,
		cfg:           cfg,
		client:        client,
		openTimeout:   f.openTimeout,
		maxDrainBytes: f.maxDrainBytes,
		reusable:      true,
		oneCall:       oneCall,
		nextCallID:    1,
		wakeCh:        make(chan struct{}),
		recvDone:      make(chan struct{}),
	}
	go s.recvLoop()
	return s
}

// bumpLocked publishes a demux-state change. Caller holds s.mu.
func (s *session) bumpLocked() {
	s.stateVer++
	close(s.wakeCh)
	s.wakeCh = make(chan struct{})
}

// waitUntil blocks until pred reports true or the session closes. pred is
// evaluated under s.mu in the same critical section that captures the wake
// channel, so a change published after the evaluation always reaches us.
func (s *session) waitUntil(pred func() bool) {
	for {
		s.mu.Lock()
		if s.closed || pred() {
			s.mu.Unlock()
			return
		}
		ch := s.wakeCh
		s.mu.Unlock()
		<-ch
	}
}

// waitUntilOrTick is waitUntil with a polling tick, for the client idle watch.
func (s *session) waitUntilOrTick(pred func() bool, d time.Duration) {
	for {
		s.mu.Lock()
		if s.closed || pred() {
			s.mu.Unlock()
			return
		}
		ch := s.wakeCh
		s.mu.Unlock()
		t := time.NewTimer(d)
		select {
		case <-ch:
			t.Stop()
			return
		case <-t.C:
			return
		}
	}
}

func (s *session) markBadLocked() {
	s.reusable = false
	s.ioError = true
}

func (s *session) markBad() {
	s.mu.Lock()
	already := s.ioError
	s.markBadLocked()
	car := s.carrier
	s.mu.Unlock()
	if !already && car != nil {
		_ = car.Abort()
	}
}

// detachCall releases the demux consumer. If poison is true (initiator closed
// without a protocol terminal), Reusable becomes false.
func (s *session) detachCall(c *call, poison bool) {
	s.mu.Lock()
	if s.active == c {
		s.active = nil
		s.mode = modeIdle
		s.lastCallID = c.callID
		if poison {
			s.reusable = false
		}
	}
	s.bumpLocked()
	s.mu.Unlock()
}

func (s *session) Reusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.oneCall && s.spent {
		return false
	}
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
	s.active = nil
	s.mode = modeIdle
	s.acceptCh = nil
	s.acceptCtx = nil
	s.bumpLocked()
	s.mu.Unlock()

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
	// Unblock recvLoop Reads before joining; otherwise a stuck Read deadlocks Close.
	err := s.conn.Close()
	<-s.recvDone
	s.clearReadDeadline()
	return err
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
		ver := s.stateVer
		s.mu.Unlock()

		switch mode {
		case modeIdle:
			// Sequential idle watchdog (§2.6): client pooled sessions only, and
			// only after at least one call (lastCallID≠0). Server AcceptCall owns
			// the next read; birth-time client Reads race OpenCall/AcceptCall.
			if s.client {
				s.mu.Lock()
				armed := s.lastCallID != 0
				s.mu.Unlock()
				if armed {
					s.watchIdle(ver)
					continue
				}
			}
			s.waitUntil(func() bool { return s.stateVer != ver })
			continue

		case modeAccepting:
			if s.kind == kindDatagram {
				s.acceptDatagram(acceptCtx)
				continue
			}
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
				s.waitAcceptRetired(ch)
				continue
			}
			s.mu.Lock()
			ch := s.acceptCh
			s.mu.Unlock()
			if ch != nil {
				// Blocking send: AcceptCall is waiting. Never use the default
				// branch — a wake race used to re-enter modeAccepting, read the
				// next END into a stale acceptCh, and lose it forever.
				ch <- acceptResult{frame: f}
			} else {
				s.pushFront(f)
			}
			// Do not read again until this accept attempt is retired: mode
			// returns to modeAccepting for the next one, so its value cannot
			// distinguish "still mine" from "a new attempt".
			s.waitAcceptRetired(ch)

		case modeInCall:
			if active == nil {
				s.waitUntil(func() bool { return s.stateVer != ver })
				continue
			}
			if s.kind == kindDatagram {
				if s.client {
					s.recvClientDatagram(active)
				} else {
					// Server datagram: request frames already delivered at accept.
					s.waitVer(ver)
				}
				continue
			}
			f, err := s.readFrame()
			if err != nil {
				if s.isClosed() {
					return
				}
				if isTimeoutErr(err) {
					// Deadline wake from Close/Accept cancel — not a hard I/O fault.
					s.clearReadDeadline()
					continue
				}
				s.mu.Lock()
				cur := s.active
				mode := s.mode
				s.mu.Unlock()
				s.clearReadDeadline()
				if cur == nil || mode != modeInCall {
					s.waitVer(ver)
					continue
				}
				cur.abortRecv(err)
				s.markBad()
				s.waitVer(ver)
				continue
			}
			s.mu.Lock()
			cur := s.active
			s.mu.Unlock()
			if cur == nil {
				s.pushFront(f)
				s.waitVer(ver)
				continue
			}
			if f.CallID != cur.callID {
				if f.Type == TypeOpen {
					// The peer opened the next call without waiting for this
					// call's STATUS. Sequential reuse makes that wait, not fail
					// (§4.5), so hand the OPEN to the next AcceptCall instead of
					// reporting a call-ID violation and aborting the carrier.
					s.pushFront(f)
					s.waitVer(ver)
					continue
				}
				cur.abortRecv(fmt.Errorf("envelope: unexpected call ID %d want %d", f.CallID, cur.callID))
				s.markBad()
				s.waitVer(ver)
				continue
			}
			cur.deliver(f)
			if cur.recvFinished() {
				s.waitVer(ver)
			}
		}
	}
}

func (s *session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// watchIdle runs one idle-watch iteration on the sole recvLoop reader (client).
// Peer EOF/hard I/O → reusable=false / markBad. Unexpected bytes are parked and
// mark the session not reusable without parsing (avoids racing residual STATUS/END).
// ver is recvLoop's snapshot, so a change published while we decide what to do
// here still releases the wait.
func (s *session) watchIdle(ver uint64) {
	s.mu.Lock()
	if s.closed || s.mode != modeIdle || !s.client {
		s.mu.Unlock()
		return
	}
	if len(s.readBuf) > 0 || len(s.frameBuf) > 0 || len(s.pendingFrames) > 0 {
		s.mu.Unlock()
		s.waitVer(ver)
		return
	}
	kind := s.kind
	s.mu.Unlock()

	switch kind {
	case kindByteStream:
		s.watchIdleByteStream(ver)
	default:
		s.waitUntilOrTick(func() bool { return s.stateVer != ver }, 50*time.Millisecond)
	}
}

func (s *session) watchIdleByteStream(ver uint64) {
	bs, ok := s.carrier.(transport.ByteStreamCarrier)
	if !ok {
		s.waitVer(ver)
		return
	}
	for {
		s.mu.Lock()
		if s.closed || s.mode != modeIdle {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		s.setReadDeadline(time.Now().Add(50 * time.Millisecond))
		var one [1]byte
		n, err := bs.Read(one[:])
		s.clearReadDeadline()

		s.mu.Lock()
		closed := s.closed
		mode := s.mode
		s.mu.Unlock()
		if closed {
			return
		}

		if isTimeoutErr(err) {
			if mode != modeIdle {
				return
			}
			continue
		}

		if mode != modeIdle {
			if n > 0 {
				s.mu.Lock()
				s.readBuf = append([]byte{one[0]}, s.readBuf...)
				s.mu.Unlock()
			} else if errors.Is(err, io.EOF) {
				s.mu.Lock()
				s.reusable = false
				s.mu.Unlock()
			} else if err != nil {
				s.markBad()
			}
			return
		}

		if n > 0 {
			// §2.6: unexpected idle traffic → not reusable; park for demux/Close.
			s.mu.Lock()
			s.readBuf = append(s.readBuf, one[0])
			s.reusable = false
			s.mu.Unlock()
			s.waitVer(ver)
			return
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			s.mu.Lock()
			s.reusable = false
			s.mu.Unlock()
		} else {
			s.markBad()
		}
		s.waitVer(ver)
		return
	}
}

// waitAcceptRetired blocks until the accept attempt that owned ch is no longer
// installed, or the session closes. Identity, not mode value: the attempt is
// what the reader handed a frame to, and only its retirement licenses another read.
func (s *session) waitAcceptRetired(ch chan acceptResult) {
	s.waitUntil(func() bool { return s.acceptCh != ch })
}

// waitVer blocks until the demux state advances past ver, or the session closes.
func (s *session) waitVer(ver uint64) {
	s.waitUntil(func() bool { return s.stateVer != ver })
}

// waitWakeOrClosed blocks until the next state change after entry, or close.
func (s *session) waitWakeOrClosed() {
	s.mu.Lock()
	ver := s.stateVer
	s.mu.Unlock()
	s.waitVer(ver)
}

// waitModeChange blocks until session.mode differs from from, or the session closes.
func (s *session) waitModeChange(from demuxMode) {
	s.waitUntil(func() bool { return s.mode != from })
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
			max := s.maxDrainBytes
			if max > 0 && drained > max {
				return Frame{}, fmt.Errorf("envelope: residual drain exceeded MaxDrainBytes (%d)", max)
			}
			continue
		}
		return f, nil
	}
}

func frameWireSize(f Frame, kind carrierKind) (int64, error) {
	switch kind {
	case kindMessage, kindDatagram:
		body, err := MarshalFrameBody(f)
		return int64(len(body)), err
	default:
		raw, err := MarshalFrame(f)
		return int64(len(raw)), err
	}
}

func (s *session) readFrameFirstByteThenTimeout(ctx context.Context) (Frame, error) {
	if f, ok := s.popPending(); ok {
		return f, nil
	}
	if s.kind == kindMessage {
		return s.readMessageFrame(ctx, true)
	}
	if s.kind == kindDatagram {
		return Frame{}, fmt.Errorf("envelope: datagram AcceptCall uses acceptDatagram")
	}
	// Byte stream: wait for ≥1 byte with only accept ctx, then OpenTimeout.
	if err := s.waitFirstByte(ctx); err != nil {
		return Frame{}, err
	}
	to := s.openTimeout
	if to <= 0 {
		return s.readFrame()
	}
	defer s.clearReadDeadline()
	// One OpenTimeout budget from the first byte through the parsed OPEN.
	// Anchoring it outside the loop is what makes the retry below safe: a
	// retry cannot extend the peer's budget, and the loop always terminates.
	budget := time.Now().Add(to)
	for {
		s.setReadDeadline(budget)
		f, err := s.readFrame()
		if err == nil || !isTimeoutErr(err) {
			return f, err
		}
		// Our OpenTimeout expiring and someone else's wakeRead() both surface
		// as a read timeout. Only the budget tells them apart: wakeRead moves
		// the deadline into the past to bounce the reader, so the budget still
		// has time left. A foreign wake is our own control event, not a peer
		// fault — reporting it fails a legal accept and marks a healthy
		// session unusable.
		//
		// The retry is local on purpose. Returning to recvLoop would re-enter
		// the accept branch and park, and it is only safe at all because a
		// frame read now resumes instead of losing what it already took.
		if !time.Now().Before(budget) {
			return Frame{}, err
		}
		if ctx == nil {
			return Frame{}, err
		}
		select {
		case <-ctx.Done():
			return Frame{}, ctx.Err()
		default:
		}
	}
}

func (s *session) waitFirstByte(ctx context.Context) error {
	if len(s.frameBuf) > 0 {
		return nil
	}
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
		if s.isClosed() {
			return io.EOF
		}
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
	if f, ok := s.popPending(); ok {
		return f, nil
	}
	if s.kind == kindMessage {
		return s.readMessageFrame(context.Background(), false)
	}
	return s.readPrefixedFrame()
}

// popPending takes the next frame the demux handed back, if any. Such frames
// were already size- and metadata-checked when they were first read.
func (s *session) popPending() (Frame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pendingFrames) == 0 {
		return Frame{}, false
	}
	f := s.pendingFrames[0]
	s.pendingFrames = s.pendingFrames[1:]
	if len(s.pendingFrames) == 0 {
		s.pendingFrames = nil
	}
	return f, true
}

// checkInboundMeta rejects metadata larger than the configured inbound limit.
// MaxMetadataSize only constrains what we send, so without this a peer decides
// how much metadata we parse and retain.
func (s *session) checkInboundMeta(f Frame) error {
	max := s.cfg.MaxInboundMetadataSize
	if max <= 0 || len(f.Headers) == 0 {
		return nil
	}
	sz, err := metadataWireSize(f.Headers)
	if err != nil {
		return err
	}
	if sz > max {
		return fmt.Errorf("%w: %d > inbound limit %d", ErrMetaTooLarge, sz, max)
	}
	return nil
}

func (s *session) checkInboundMetas(frames []Frame) error {
	for _, f := range frames {
		if err := s.checkInboundMeta(f); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) readPrefixedFrame() (Frame, error) {
	bs, ok := s.carrier.(transport.ByteStreamCarrier)
	if !ok {
		return Frame{}, fmt.Errorf("envelope: ByteStreamCarrier required")
	}
	// An oversize body from an earlier frame may still be on the wire.
	if err := s.runDiscard(bs); err != nil {
		return Frame{}, err
	}
	if err := s.fillFrame(bs, lenPrefix, "length prefix"); err != nil {
		return Frame{}, err
	}
	n := int64(binary.BigEndian.Uint32(s.frameBuf[:lenPrefix]))
	if n < headerSize {
		s.dropFrame(lenPrefix)
		return Frame{}, fmt.Errorf("%w: body length %d", ErrInvalidLength, n)
	}
	if max := s.cfg.MaxFrameSize; max > 0 && n > max {
		// Never buffer an oversize frame; drop its body off the carrier.
		s.dropFrame(lenPrefix)
		s.discardRest = n
		if err := s.runDiscard(bs); err != nil {
			return Frame{}, err
		}
		return Frame{}, fmt.Errorf("%w: body %d > max %d", ErrFrameTooLarge, n, max)
	}
	if max := s.cfg.MaxMessageSize; max > 0 && n-headerSize > max {
		// Peek the type so an oversize DATA payload is discarded instead of
		// allocated. Other types are under MaxFrameSize and parse normally.
		if err := s.fillFrame(bs, lenPrefix+1, "frame type"); err != nil {
			return Frame{}, err
		}
		if Type(s.frameBuf[lenPrefix]) == TypeData {
			s.dropFrame(lenPrefix + 1)
			s.discardRest = n - 1
			if err := s.runDiscard(bs); err != nil {
				return Frame{}, err
			}
			return Frame{}, fmt.Errorf("%w: DATA payload %d > max %d",
				ErrMessageTooLarge, n-headerSize, max)
		}
	}
	total := lenPrefix + int(n)
	if err := s.fillFrame(bs, total, "frame body"); err != nil {
		return Frame{}, err
	}
	f, err := ParseFrameBody(s.frameBuf[lenPrefix:total])
	s.dropFrame(total)
	if err != nil {
		return Frame{}, err
	}
	if err := s.checkInboundMeta(f); err != nil {
		return Frame{}, err
	}
	return f, nil
}

// fillFrame grows frameBuf to need bytes, spending pushed-back bytes first and
// then reading from the carrier.
//
// Whatever arrives stays in frameBuf even when the read fails. wakeRead()
// expires the carrier deadline from other goroutines at arbitrary points, so a
// frame read has to be resumable: discarding what an aborted read already took
// off the carrier misaligns every later frame boundary, and a length field
// parsed from the middle of a frame parks the reader on bytes that never come.
func (s *session) fillFrame(bs transport.ByteStreamCarrier, need int, what string) error {
	s.takeReadBuf()
	for len(s.frameBuf) < need {
		if cap(s.frameBuf) < need {
			size := need
			if grown := 2 * cap(s.frameBuf); grown > size {
				size = grown
			}
			buf := make([]byte, len(s.frameBuf), size)
			copy(buf, s.frameBuf)
			s.frameBuf = buf
		}
		have := len(s.frameBuf)
		n, err := bs.Read(s.frameBuf[have:need])
		if n > 0 {
			s.frameBuf = s.frameBuf[:have+n]
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("%w: %s", ErrTruncated, what)
			}
			return err
		}
	}
	return nil
}

// takeReadBuf moves pushed-back bytes into frameBuf. readBuf is shared with
// AcceptCall's cancel path; frameBuf belongs to recvLoop alone.
func (s *session) takeReadBuf() {
	s.mu.Lock()
	if len(s.readBuf) > 0 {
		s.frameBuf = append(s.frameBuf, s.readBuf...)
		s.readBuf = nil
	}
	s.mu.Unlock()
}

// dropFrame removes n consumed bytes from the front of frameBuf, keeping the
// allocation for the next frame.
func (s *session) dropFrame(n int) {
	s.frameBuf = s.frameBuf[:copy(s.frameBuf, s.frameBuf[n:])]
}

// runDiscard drops what is left of an oversize body. discardRest survives a
// failed read, so the discard resumes rather than restarting mid-body.
func (s *session) runDiscard(bs transport.ByteStreamCarrier) error {
	var scratch []byte
	for s.discardRest > 0 {
		s.takeReadBuf()
		if len(s.frameBuf) > 0 {
			n := int64(len(s.frameBuf))
			if n > s.discardRest {
				n = s.discardRest
			}
			s.dropFrame(int(n))
			s.discardRest -= n
			continue
		}
		if scratch == nil {
			scratch = make([]byte, 32<<10)
		}
		chunk := scratch
		if int64(len(chunk)) > s.discardRest {
			chunk = chunk[:s.discardRest]
		}
		n, err := bs.Read(chunk)
		s.discardRest -= int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("%w: discard", ErrTruncated)
			}
			return err
		}
	}
	return nil
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
	if s.cfg.MaxFrameSize > 0 && int64(len(res.msg)) > s.cfg.MaxFrameSize {
		return Frame{}, fmt.Errorf("%w: body %d > max %d", ErrFrameTooLarge, len(res.msg), s.cfg.MaxFrameSize)
	}
	if s.cfg.MaxMessageSize > 0 && len(res.msg) > headerSize &&
		Type(res.msg[0]) == TypeData &&
		int64(len(res.msg)-headerSize) > s.cfg.MaxMessageSize {
		return Frame{}, fmt.Errorf("%w: DATA payload %d > max %d",
			ErrMessageTooLarge, len(res.msg)-headerSize, s.cfg.MaxMessageSize)
	}
	f, err := ParseFrameBody(res.msg)
	if err != nil {
		return Frame{}, err
	}
	if err := s.checkInboundMeta(f); err != nil {
		return Frame{}, err
	}
	return f, nil
}

func (s *session) pushFront(f Frame) {
	if s.kind == kindMessage {
		// A message carrier cannot unread bytes, so keep the frame itself.
		// Aborting here instead used to kill the connection whenever the demux
		// handed a frame back — a pipelined OPEN, or an accept cancelled after
		// the reader had already published its result.
		s.mu.Lock()
		s.pendingFrames = append([]Frame{f}, s.pendingFrames...)
		s.mu.Unlock()
		return
	}
	raw, err := MarshalFrame(f)
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
	case kindDatagram:
		return fmt.Errorf("envelope: datagram path must use sendDatagramBatch")
	default:
		return fmt.Errorf("envelope: unknown carrier kind")
	}
}

// sendDatagramBatch marshals frames into one datagram and sends it.
func (s *session) sendDatagramBatch(frames []Frame) error {
	for _, f := range frames {
		if err := f.Validate(s.cfg.MaxFrameSize, s.cfg.MaxMetadataSize); err != nil {
			return err
		}
	}
	raw, err := MarshalDatagram(frames)
	if err != nil {
		return err
	}
	if s.maxDatagram > 0 && int64(len(raw)) > s.maxDatagram {
		return status.Error(status.ResourceExhausted,
			fmt.Sprintf("envelope: datagram %d bytes exceeds limit %d", len(raw), s.maxDatagram))
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	dc, ok := s.carrier.(transport.DatagramCarrier)
	if !ok {
		return fmt.Errorf("envelope: DatagramCarrier required")
	}
	return dc.SendDatagram(raw)
}

// acceptDatagram reads one request datagram for AcceptCall.
func (s *session) acceptDatagram(ctx context.Context) {
	dc, ok := s.carrier.(transport.DatagramCarrier)
	if !ok {
		s.failAccept(fmt.Errorf("envelope: DatagramCarrier required"))
		s.waitModeChange(modeAccepting)
		return
	}

	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		data, err := dc.RecvDatagram()
		ch <- result{data, err}
	}()

	var res result
	if ctx != nil {
		select {
		case <-ctx.Done():
			select {
			case res = <-ch:
			case <-time.After(50 * time.Millisecond):
				s.failAccept(ctx.Err())
				s.waitModeChange(modeAccepting)
				return
			}
			if res.err != nil && res.data == nil {
				s.failAccept(ctx.Err())
				s.waitModeChange(modeAccepting)
				return
			}
		case res = <-ch:
		}
	} else {
		res = <-ch
	}

	if res.err != nil {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		if !errors.Is(res.err, context.Canceled) && !errors.Is(res.err, context.DeadlineExceeded) {
			if !errors.Is(res.err, io.EOF) {
				s.markBadLocked()
			} else {
				s.reusable = false
			}
		}
		acceptCh := s.acceptCh
		s.mu.Unlock()
		if acceptCh != nil {
			select {
			case acceptCh <- acceptResult{err: res.err}:
			default:
			}
		}
		s.waitModeChange(modeAccepting)
		return
	}

	frames, err := ParseDatagram(res.data, s.cfg.MaxFrameSize, s.cfg.MaxMessageSize)
	if err != nil {
		s.failAccept(err)
		s.markBad()
		s.waitModeChange(modeAccepting)
		return
	}
	if err := s.checkInboundMetas(frames); err != nil {
		s.failAccept(err)
		s.markBad()
		s.waitModeChange(modeAccepting)
		return
	}
	if len(frames) == 0 || frames[0].Type != TypeOpen {
		s.failAccept(fmt.Errorf("envelope: datagram expected OPEN, got %v", frames))
		s.markBad()
		s.waitModeChange(modeAccepting)
		return
	}

	s.mu.Lock()
	acceptCh := s.acceptCh
	s.mu.Unlock()
	if acceptCh == nil {
		s.waitModeChange(modeAccepting)
		return
	}
	// Deliver OPEN; remaining frames ride on acceptResult via a side channel
	// on the session until AcceptCall attaches the call.
	s.mu.Lock()
	s.datagramRest = frames[1:]
	s.mu.Unlock()
	acceptCh <- acceptResult{frame: frames[0]}
	s.waitModeChange(modeAccepting)
}

func (s *session) failAccept(err error) {
	s.mu.Lock()
	ch := s.acceptCh
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- acceptResult{err: err}:
		default:
		}
	}
}

// recvClientDatagram reads response datagrams until call-ID match or fatal error.
// Mismatched call IDs are discarded (§4.5).
func (s *session) recvClientDatagram(active *call) {
	dc, ok := s.carrier.(transport.DatagramCarrier)
	if !ok {
		active.abortRecv(fmt.Errorf("envelope: DatagramCarrier required"))
		s.markBad()
		s.waitWakeOrClosed()
		return
	}

	for {
		if s.isClosed() {
			return
		}
		s.mu.Lock()
		cur := s.active
		mode := s.mode
		s.mu.Unlock()
		if cur != active || mode != modeInCall {
			s.waitWakeOrClosed()
			return
		}

		data, err := dc.RecvDatagram()
		if err != nil {
			if s.isClosed() {
				return
			}
			if isTimeoutErr(err) {
				s.clearReadDeadline()
				continue
			}
			s.mu.Lock()
			cur = s.active
			mode = s.mode
			s.mu.Unlock()
			if cur != active || mode != modeInCall {
				s.waitWakeOrClosed()
				return
			}
			cur.abortRecv(err)
			s.markBad()
			s.waitWakeOrClosed()
			return
		}

		frames, err := ParseDatagram(data, s.cfg.MaxFrameSize, s.cfg.MaxMessageSize)
		if err != nil {
			active.abortRecv(err)
			s.markBad()
			s.waitWakeOrClosed()
			return
		}
		if err := s.checkInboundMetas(frames); err != nil {
			active.abortRecv(err)
			s.markBad()
			s.waitWakeOrClosed()
			return
		}
		if len(frames) == 0 {
			continue
		}
		// Ownership check: any mismatched call ID → discard whole datagram.
		mismatch := false
		for _, f := range frames {
			if f.CallID != active.callID {
				mismatch = true
				break
			}
		}
		if mismatch {
			continue // discard; keep waiting
		}
		for _, f := range frames {
			active.deliver(f)
		}
		if active.recvFinished() {
			s.waitWakeOrClosed()
			return
		}
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
	if s.oneCall && s.spent {
		s.mu.Unlock()
		return nil, framing.ErrSessionSpent
	}
	if s.mode != modeIdle || s.active != nil {
		s.mu.Unlock()
		return nil, framing.ErrSessionBusy
	}
	if s.kind == kindDatagram && m.Shape() != descriptor.Unary {
		s.mu.Unlock()
		return nil, status.Error(status.Unimplemented, fmt.Sprintf(
			"envelope: shape %v unsupported on DatagramCarrier (Framing=envelope); need Unary", m.Shape()))
	}
	id := s.nextCallID
	s.nextCallID++
	if s.oneCall {
		s.spent = true
	}
	s.mu.Unlock()

	if spec.Metadata != nil {
		_ = metadata.FreezeOutgoingHeaders(spec.Metadata)
	}

	b, _ := budget.FromContext(ctx)
	c := newCall(s.session, id, m.FullName(), true, spec.Metadata, m.Shape(), b)

	// Attach before writing OPEN so a fast response cannot race the demux.
	s.mu.Lock()
	s.active = c
	s.mode = modeInCall
	s.lastCallID = id
	s.bumpLocked()
	s.mu.Unlock()
	s.wakeRead() // unblock idle watchdog Read

	if s.kind == kindDatagram {
		// OPEN is deferred until HalfClose (single request datagram).
		return c, nil
	}

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
		s.bumpLocked()
		s.mu.Unlock()
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
	if s.oneCall && s.spent {
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
	s.bumpLocked()
	s.mu.Unlock()

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
		s.finishAccept(nil)
		s.clearReadDeadline()
		if res.err == nil {
			res.err = ctx.Err()
		}
		return nil, res.err
	case res = <-ch:
	}

	if res.err != nil {
		s.finishAccept(nil)
		if errors.Is(res.err, io.EOF) {
			return nil, io.EOF
		}
		return nil, res.err
	}
	f := res.frame
	if f.Type != TypeOpen {
		s.finishAccept(nil)
		s.markBad()
		return nil, fmt.Errorf("envelope: expected OPEN, got type %d", f.Type)
	}
	if f.Method == "" {
		// Hand the call back alongside the error: the composition layer writes
		// the status through it (server.handleRejected), and attaching it as the
		// active call makes the demux consume the peer's remaining DATA/END
		// frames. Returning a nil call left those bytes queued, so the next
		// AcceptCall parsed an END frame as its OPEN and died with a
		// connection-level error — killing an unrelated, perfectly legal call.
		b, _ := budget.FromContext(ctx)
		c := newCall(s.session, f.CallID, f.Method, false, spec.Metadata, 0, b)
		c.openSeen = true
		s.finishAccept(c)
		return &serverCall{call: c}, fmt.Errorf("%w: %w", framing.ErrCallRejected,
			status.Error(status.InvalidArgument, "envelope: empty method in OPEN"))
	}

	if spec.Metadata != nil {
		_ = metadata.SetIncomingHeaders(spec.Metadata, headersToMD(f.Headers))
	}

	b, _ := budget.FromContext(ctx)
	c := newCall(s.session, f.CallID, f.Method, false, spec.Metadata, 0, b)
	c.openSeen = true
	if f.Flags&FlagOpenEnd != 0 {
		c.peerHalfClosed = true
		c.deliverEnd()
	}

	s.mu.Lock()
	rest := s.datagramRest
	s.datagramRest = nil
	if s.oneCall {
		s.spent = true
	}
	s.mu.Unlock()

	s.finishAccept(c)

	// Deliver remaining request frames from the same datagram (DATA/END).
	for _, rf := range rest {
		if rf.CallID != c.callID {
			c.abortRecv(fmt.Errorf("envelope: unexpected call ID %d want %d", rf.CallID, c.callID))
			s.markBad()
			break
		}
		c.deliver(rf)
	}
	return &serverCall{call: c}, nil
}

// finishAccept clears accept state. If c != nil, attaches it as the in-call
// demux consumer (AcceptCall success). Otherwise returns to idle.
func (s *serverSession) finishAccept(c *call) {
	s.mu.Lock()
	s.acceptCh = nil
	s.acceptCtx = nil
	if c != nil {
		s.active = c
		s.lastCallID = c.callID
		s.mode = modeInCall
	} else if s.mode == modeAccepting {
		s.mode = modeIdle
	}
	s.bumpLocked()
	s.mu.Unlock()
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
