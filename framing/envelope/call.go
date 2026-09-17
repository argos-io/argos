package envelope

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// Local concurrency / lifecycle errors (errors.Is).
var (
	ErrConcurrentSend = errors.New("envelope: concurrent Send")
	ErrConcurrentRecv = errors.New("envelope: concurrent Recv")
	ErrSendFinished   = errors.New("envelope: send already finished")
	ErrCallClosed     = errors.New("envelope: call closed")
)

type recvEvent struct {
	payload []byte
	release func() // budget release for buffered payload; idempotent
	err     error
	headers bool // HEADERS arrived (payload ignored); Recv does not surface these
	data    bool // occupies a read-ahead DATA slot
}

// call is the shared Call implementation for initiator and responder.
type call struct {
	sess      *session
	callID    uint64
	method    string
	md        metadata.CallMetadata
	shape     descriptor.Shape
	initiator bool
	budget    budget.Budget

	// inbox holds DATA and control events in wire order.
	// Capacity is ReadAheadMessages+2 so END/STATUS can be enqueued while the
	// DATA gate is full (backpressure stops further DATA reads, not control).
	inbox chan recvEvent
	// dataGate limits buffered complete DATA to ReadAheadMessages.
	// Send a token when buffering DATA; receive when Recv/Close drains it.
	dataGate chan struct{}

	mu sync.Mutex

	openSent       bool // initiator: OPEN written
	openSeen       bool // responder: OPEN already accepted
	halfClosed     bool
	headersSent    bool
	headersArrived bool // initiator saw HEADERS (or implied by DATA/STATUS)
	finished       bool
	closed         bool
	peerHalfClosed bool // responder saw END / OPEN|END
	sawStatus      bool
	terminalRead   bool  // initiator read STATUS (or EOF after OK)
	statusErr      error // non-nil when STATUS code != OK; returned once from Recv
	statusReturned bool
	recvDone       bool // demux finished (STATUS or fatal)
	detachCh       chan struct{}
	detached       atomic.Bool // detachCh closed; Call.Close and Session.Close both do it
	detachErr      error       // what a parked Recv reports when the session detached us

	// sendErr is the failure that ended the send direction. A partial write
	// cannot be rolled back, so no further frame may be written after it: Send
	// and HalfClose keep returning it. When it is a transport.SendError with
	// ReceiveOpen() true the receive direction still works and Recv delivers the
	// response plus the remote status.
	sendErr error

	sending atomic.Bool
	recving atomic.Bool

	enqueueMu sync.Mutex // with Close/session drain; see enqueueData

	headersOnce sync.Once
	headersErr  error

	// Datagram unary staging (DatagramCarrier only).
	pendingData    []byte
	hasPendingData bool

	// peakBuffered is the high-water mark of charged payload bytes (testing).
	peakBuffered atomic.Int64
	curBuffered  atomic.Int64
}

func (c *call) SetBudget(b budget.Budget) {
	c.mu.Lock()
	c.budget = b
	c.mu.Unlock()
}

func newCall(s *session, id uint64, method string, initiator bool, md metadata.CallMetadata, shape descriptor.Shape, b budget.Budget) *call {
	ra := s.cfg.ReadAheadMessages
	if ra < 1 {
		ra = 1
	}
	return &call{
		sess:      s,
		callID:    id,
		method:    method,
		md:        md,
		shape:     shape,
		initiator: initiator,
		budget:    b,
		inbox:     make(chan recvEvent, ra+2),
		dataGate:  make(chan struct{}, ra),
		detachCh:  make(chan struct{}),
	}
}

func (c *call) Method() string { return c.method }

func (c *call) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *call) deliver(f Frame) {
	switch f.Type {
	case TypeHeaders:
		c.mu.Lock()
		c.headersArrived = true
		c.mu.Unlock()
		if c.md != nil {
			_ = metadata.SetIncomingHeaders(c.md, headersToMD(f.Headers))
		}
		// Metadata is applied synchronously; inbox wake is best-effort.
		select {
		case c.inbox <- recvEvent{headers: true}:
		default:
		}
	case TypeData:
		c.ensureImpliedHeaders()
		payload := f.Data
		if payload == nil {
			payload = []byte{}
		}
		if max := c.cfgMaxMessage(); max > 0 && int64(len(payload)) > max {
			c.abortRecv(fmt.Errorf("%w: DATA payload %d > max %d", ErrMessageTooLarge, len(payload), max))
			return
		}
		c.enqueueData(payload)
	case TypeEnd:
		c.deliverEnd()
	case TypeStatus:
		c.ensureImpliedHeaders()
		c.mu.Lock()
		c.sawStatus = true
		c.recvDone = true
		code := status.Code(f.Code)
		if code != status.OK {
			c.statusErr = status.Error(code, f.Message)
		} else {
			c.statusErr = nil
		}
		c.mu.Unlock()
		if c.md != nil && len(f.Headers) > 0 {
			_ = metadata.SetIncomingTrailers(c.md, headersToMD(f.Headers))
		} else if c.md != nil {
			_ = metadata.SetIncomingTrailers(c.md, metadata.Metadata{})
		}
		c.enqueueCtrl(recvEvent{err: errStatusPending})
	case TypeOpen:
		c.abortRecv(fmt.Errorf("envelope: unexpected OPEN on active call"))
	default:
		c.abortRecv(fmt.Errorf("envelope: unknown frame type %d", f.Type))
	}
}

func (c *call) enqueueData(payload []byte) {
	// Acquire a read-ahead slot (backpressure when full).
	select {
	case c.dataGate <- struct{}{}:
	case <-c.detachCh:
		return
	}

	rel, err := c.chargePayload(payload)
	if err != nil {
		c.releaseDataSlot()
		c.abortRecv(err)
		return
	}

	ev := recvEvent{payload: payload, release: rel, data: true}
	c.enqueueMu.Lock()
	defer c.enqueueMu.Unlock()
	if c.detached.Load() {
		if rel != nil {
			rel()
			c.noteRelease(int64(cap(payload)))
		}
		c.releaseDataSlot()
		return
	}
	select {
	case c.inbox <- ev:
	case <-c.detachCh:
		if rel != nil {
			rel()
			c.noteRelease(int64(cap(payload)))
		}
		c.releaseDataSlot()
	}
}

func (c *call) enqueueCtrl(ev recvEvent) {
	select {
	case c.inbox <- ev:
	case <-c.detachCh:
	}
}

func (c *call) releaseDataSlot() {
	select {
	case <-c.dataGate:
	default:
	}
}

func (c *call) chargePayload(payload []byte) (func(), error) {
	c.mu.Lock()
	b := c.budget
	c.mu.Unlock()
	if b == nil {
		return nil, nil
	}
	var (
		rel func()
		err error
	)
	if sb, ok := b.(budget.SliceBudget); ok {
		rel, err = sb.TryAcquireSlice(payload)
	} else {
		rel, err = b.TryAcquire(int64(cap(payload)))
	}
	if err != nil {
		return nil, err
	}
	n := int64(cap(payload))
	if n > 0 {
		cur := c.curBuffered.Add(n)
		for {
			peak := c.peakBuffered.Load()
			if cur <= peak || c.peakBuffered.CompareAndSwap(peak, cur) {
				break
			}
		}
	}
	return rel, nil
}

func (c *call) noteRelease(n int64) {
	if n > 0 {
		c.curBuffered.Add(-n)
	}
}

// ensureImpliedHeaders marks the response's initial metadata as arrived when
// the peer sent DATA/STATUS without a HEADERS frame. It is initiator-only: a
// responder's incoming headers are the request's OPEN headers, applied by
// AcceptCall, and HEADERS is a response frame it never receives. Running this
// on the responder overwrote the just-parsed request metadata with an empty
// map, so server filters and handlers saw no request metadata at all once the
// first DATA frame was delivered.
func (c *call) ensureImpliedHeaders() {
	if !c.initiator {
		return
	}
	c.mu.Lock()
	arrived := c.headersArrived
	if !arrived {
		c.headersArrived = true
	}
	c.mu.Unlock()
	if !arrived && c.md != nil {
		_ = metadata.SetIncomingHeaders(c.md, metadata.Metadata{})
	}
}

var errStatusPending = errors.New("envelope: status pending")

// sendFailure reports whether err is a transport send failure on an exchange
// whose receive direction is still open. Framing must keep such a carrier
// readable instead of aborting it: the peer may still deliver the response and
// the terminal status. A SendError with ReceiveOpen() false (or a local error)
// means the exchange is unrecoverable.
func sendFailure(err error) bool {
	se, ok := transport.AsSendError(err)
	return ok && se.ReceiveOpen()
}

// sendFailed records the error that ended the send direction and returns it to
// the caller unchanged, so stream.Wrap can map a still-receivable exchange to
// ErrSendClosed. Only a failure the transport reports as unrecoverable poisons
// the session; "send direction finished" is not a terminal state for the call.
func (c *call) sendFailed(err error) error {
	c.mu.Lock()
	if c.sendErr == nil {
		c.sendErr = err
	}
	c.mu.Unlock()
	if !sendFailure(err) {
		c.sess.markBad()
	}
	return err
}

func (c *call) deliverEnd() {
	c.mu.Lock()
	c.peerHalfClosed = true
	c.mu.Unlock()
	c.enqueueCtrl(recvEvent{err: io.EOF})
}

func (c *call) abortRecv(err error) {
	c.mu.Lock()
	c.recvDone = true
	c.mu.Unlock()
	c.enqueueCtrl(recvEvent{err: err})
}

func (c *call) recvFinished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recvDone
}

// SendHeaders is responder-only. Writes a HEADERS frame even when empty.
func (c *call) SendHeaders() error {
	if c.initiator {
		return status.Error(status.Unimplemented, "envelope: SendHeaders unsupported for initiator")
	}
	if c.sess.kind == kindDatagram {
		return status.Error(status.Unimplemented, "envelope: SendHeaders unsupported on DatagramCarrier")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCallClosed
	}
	if c.headersSent {
		c.mu.Unlock()
		return metadata.ErrHeadersAlreadySent
	}
	if c.finished {
		c.mu.Unlock()
		return ErrSendFinished
	}
	c.mu.Unlock()
	return c.writeResponseHeadersOnce()
}

func (c *call) writeResponseHeadersOnce() error {
	c.headersOnce.Do(func() {
		var hdrs []Header
		if c.md != nil {
			hdrs = mdToHeaders(c.md.OutgoingHeaders())
		}
		if err := c.sess.writeFrame(Frame{
			Type:    TypeHeaders,
			CallID:  c.callID,
			Headers: hdrs,
		}); err != nil {
			c.headersErr = err
			return
		}
		c.mu.Lock()
		c.headersSent = true
		c.mu.Unlock()
		if c.md != nil {
			_ = metadata.FreezeOutgoingHeaders(c.md)
		}
	})
	if c.headersErr != nil {
		return c.sendFailed(c.headersErr)
	}
	c.mu.Lock()
	sent := c.headersSent
	c.mu.Unlock()
	if !sent {
		return metadata.ErrHeadersAlreadySent
	}
	return nil
}

func (c *call) maybeAutoHeaders() error {
	if c.initiator {
		return nil
	}
	if c.sess.kind == kindDatagram {
		// Headers ride with the response datagram at Finish.
		return nil
	}
	c.mu.Lock()
	if c.headersSent {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	var md metadata.Metadata
	if c.md != nil {
		md = c.md.OutgoingHeaders()
	}
	if len(md) == 0 {
		return nil
	}
	return c.SendHeaders()
}

func (c *call) Send(payload []byte) error {
	if !c.sending.CompareAndSwap(false, true) {
		return ErrConcurrentSend
	}
	defer c.sending.Store(false)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCallClosed
	}
	if c.sendErr != nil {
		// The send direction already failed; a partial write cannot be rolled
		// back, so this call stays finished for sending. Recv still works.
		err := c.sendErr
		c.mu.Unlock()
		return err
	}
	if c.initiator && c.halfClosed {
		c.mu.Unlock()
		return ErrSendFinished
	}
	if !c.initiator && c.finished {
		c.mu.Unlock()
		return ErrSendFinished
	}
	c.mu.Unlock()

	data := payload
	if data == nil {
		data = []byte{}
	}
	if max := c.cfgMaxMessage(); max > 0 && int64(len(data)) > max {
		return fmt.Errorf("%w: Send payload %d > max %d", ErrMessageTooLarge, len(data), max)
	}
	// Copy so caller may reuse the buffer after return.
	cp := append([]byte(nil), data...)

	if c.sess.kind == kindDatagram {
		c.mu.Lock()
		if c.hasPendingData {
			c.mu.Unlock()
			return status.Error(status.Unimplemented,
				"envelope: DatagramCarrier allows at most one DATA per call")
		}
		c.pendingData = cp
		c.hasPendingData = true
		c.mu.Unlock()
		return nil
	}

	if c.initiator {
		if err := c.ensureOpen(); err != nil {
			return err
		}
	} else {
		if err := c.maybeAutoHeaders(); err != nil {
			return err
		}
	}

	if err := c.sess.writeFrame(Frame{
		Type:   TypeData,
		CallID: c.callID,
		Data:   cp,
	}); err != nil {
		return c.sendFailed(err)
	}
	return nil
}

func (c *call) cfgMaxMessage() int64 {
	return c.sess.cfg.MaxMessageSize
}

func (c *call) ensureOpen() error {
	c.mu.Lock()
	if c.openSent {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	var hdrs []Header
	if c.md != nil {
		hdrs = mdToHeaders(c.md.OutgoingHeaders())
	}
	if err := c.sess.writeFrame(Frame{
		Type:    TypeOpen,
		CallID:  c.callID,
		Method:  c.method,
		Headers: hdrs,
	}); err != nil {
		return c.sendFailed(err)
	}
	c.mu.Lock()
	c.openSent = true
	c.mu.Unlock()
	return nil
}

func (c *call) HalfClose() error {
	if !c.initiator {
		return status.Error(status.Unimplemented, "envelope: HalfClose unsupported for responder")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCallClosed
	}
	if c.sendErr != nil {
		// HalfClose is idempotent, including its failure: the caller that
		// already saw the send direction end sees the same result again.
		err := c.sendErr
		c.mu.Unlock()
		return err
	}
	if c.halfClosed {
		c.mu.Unlock()
		return nil
	}
	openSent := c.openSent
	hasData := c.hasPendingData
	pending := c.pendingData
	c.mu.Unlock()

	if c.sess.kind == kindDatagram {
		var hdrs []Header
		if c.md != nil {
			hdrs = mdToHeaders(c.md.OutgoingHeaders())
		}
		var frames []Frame
		if !hasData {
			frames = []Frame{{
				Type:    TypeOpen,
				CallID:  c.callID,
				Method:  c.method,
				Flags:   FlagOpenEnd,
				Headers: hdrs,
			}}
		} else {
			frames = []Frame{
				{Type: TypeOpen, CallID: c.callID, Method: c.method, Headers: hdrs},
				{Type: TypeData, CallID: c.callID, Data: pending},
				{Type: TypeEnd, CallID: c.callID},
			}
		}
		if err := c.sess.sendDatagramBatch(frames); err != nil {
			return c.sendFailed(err)
		}
		c.mu.Lock()
		c.openSent = true
		c.halfClosed = true
		c.hasPendingData = false
		c.pendingData = nil
		c.mu.Unlock()
		return nil
	}

	if !openSent {
		// Deferred OPEN path: zero-message OPEN|END.
		var hdrs []Header
		if c.md != nil {
			hdrs = mdToHeaders(c.md.OutgoingHeaders())
		}
		if err := c.sess.writeFrame(Frame{
			Type:    TypeOpen,
			CallID:  c.callID,
			Method:  c.method,
			Flags:   FlagOpenEnd,
			Headers: hdrs,
		}); err != nil {
			return c.sendFailed(err)
		}
		c.mu.Lock()
		c.openSent = true
		c.halfClosed = true
		c.mu.Unlock()
		return nil
	}

	if err := c.sess.writeFrame(Frame{
		Type:   TypeEnd,
		CallID: c.callID,
	}); err != nil {
		return c.sendFailed(err)
	}
	c.mu.Lock()
	c.halfClosed = true
	c.mu.Unlock()
	return nil
}

func (c *call) Finish(err error) error {
	if c.initiator {
		return status.Error(status.Unimplemented, "envelope: Finish unsupported for initiator")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCallClosed
	}
	if c.finished {
		c.mu.Unlock()
		return ErrSendFinished
	}
	c.mu.Unlock()

	if c.sess.kind == kindDatagram {
		return c.finishDatagram(err)
	}

	if err := c.maybeAutoHeaders(); err != nil {
		return err
	}

	code := status.OK
	msg := ""
	if err != nil {
		code = status.CodeOf(err)
		msg = err.Error()
	}
	var trailers []Header
	if c.md != nil {
		trailers = mdToHeaders(c.md.OutgoingTrailers())
		_ = metadata.FreezeOutgoingTrailers(c.md)
		_ = metadata.FreezeOutgoingHeaders(c.md)
	}
	if werr := c.sess.writeFrame(Frame{
		Type:    TypeStatus,
		CallID:  c.callID,
		Code:    uint32(code),
		Message: msg,
		Headers: trailers,
	}); werr != nil {
		return c.sendFailed(werr)
	}
	c.mu.Lock()
	c.finished = true
	c.headersSent = true
	c.mu.Unlock()
	// Detach demux after STATUS so a Sequential peer may write the next OPEN
	// before handler return / Call.Close. Residuals drain on next AcceptCall.
	c.sess.detachCall(c, false)
	return nil
}

func (c *call) finishDatagram(err error) error {
	c.mu.Lock()
	hasData := c.hasPendingData
	pending := c.pendingData
	headersSent := c.headersSent
	c.mu.Unlock()

	code := status.OK
	msg := ""
	if err != nil {
		code = status.CodeOf(err)
		msg = err.Error()
	}

	var frames []Frame
	if !headersSent && c.md != nil {
		hdrs := mdToHeaders(c.md.OutgoingHeaders())
		if len(hdrs) > 0 {
			frames = append(frames, Frame{
				Type:    TypeHeaders,
				CallID:  c.callID,
				Headers: hdrs,
			})
		}
	}
	if hasData {
		frames = append(frames, Frame{
			Type:   TypeData,
			CallID: c.callID,
			Data:   pending,
		})
	}
	var trailers []Header
	if c.md != nil {
		trailers = mdToHeaders(c.md.OutgoingTrailers())
		_ = metadata.FreezeOutgoingTrailers(c.md)
		_ = metadata.FreezeOutgoingHeaders(c.md)
	}
	frames = append(frames, Frame{
		Type:    TypeStatus,
		CallID:  c.callID,
		Code:    uint32(code),
		Message: msg,
		Headers: trailers,
	})

	if werr := c.sess.sendDatagramBatch(frames); werr != nil {
		return c.sendFailed(werr)
	}
	c.mu.Lock()
	c.finished = true
	c.headersSent = true
	c.hasPendingData = false
	c.pendingData = nil
	c.mu.Unlock()
	c.sess.detachCall(c, false)
	return nil
}

func (c *call) Recv() (payload []byte, release func(), err error) {
	if !c.recving.CompareAndSwap(false, true) {
		return nil, nil, ErrConcurrentRecv
	}
	defer c.recving.Store(false)

	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, nil, ErrCallClosed
		}
		// After non-OK STATUS was returned once → EOF.
		if c.statusReturned {
			c.terminalRead = true
			c.mu.Unlock()
			return nil, nil, io.EOF
		}
		if c.terminalRead {
			c.mu.Unlock()
			return nil, nil, io.EOF
		}
		c.mu.Unlock()

		var ev recvEvent
		var ok bool
		select {
		case ev, ok = <-c.inbox:
			if !ok {
				c.mu.Lock()
				c.terminalRead = true
				c.mu.Unlock()
				return nil, nil, io.EOF
			}
		case <-c.detachCh:
			// Close closes detachCh; Finish only detaches demux and leaves it open.
			return nil, nil, c.detachReason()
		}

		if ev.headers {
			continue
		}
		if ev.data {
			c.releaseDataSlot()
		}
		if ev.err != nil {
			if ev.release != nil {
				ev.release()
				c.noteRelease(int64(cap(ev.payload)))
			}
			if errors.Is(ev.err, errStatusPending) {
				c.mu.Lock()
				stErr := c.statusErr
				c.sawStatus = true
				if stErr != nil {
					c.statusReturned = true
					c.mu.Unlock()
					return nil, nil, stErr
				}
				c.statusReturned = true
				c.terminalRead = true
				c.mu.Unlock()
				return nil, nil, io.EOF
			}
			if errors.Is(ev.err, io.EOF) {
				// Peer END: request stream finished. Keep receiving STATUS on initiator;
				// on responder, EOF is the end of request messages.
				if c.initiator {
					// END is not expected on initiator receive path (server sends STATUS).
					continue
				}
				c.mu.Lock()
				c.statusReturned = true
				c.terminalRead = true
				c.mu.Unlock()
				return nil, nil, io.EOF
			}
			c.sess.markBad()
			return nil, nil, ev.err
		}
		p := ev.payload
		capN := int64(cap(p))
		budRel := ev.release
		return p, func() {
			if budRel != nil {
				budRel()
				c.noteRelease(capN)
			}
		}, nil
	}
}

func (c *call) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	terminalOK := false
	if c.initiator {
		terminalOK = c.sawStatus || c.terminalRead
	} else {
		// Responder Close after Finish is fine; residual peer frames drained on next Accept.
		terminalOK = true
	}
	// Wake blocked DATA enqueue / Recv before draining.
	c.detachLocked()
	c.mu.Unlock()

	c.enqueueMu.Lock()
	c.drainInbox()
	c.enqueueMu.Unlock()

	poison := c.initiator && !terminalOK
	c.sess.detachCall(c, poison)
	c.sess.wakeRead()
	return nil
}

// detachLocked closes detachCh exactly once. It is the single escape hatch for
// every park that can outlive the call: both enqueue paths, abortRecv, and a
// blocked Recv.
func (c *call) detachLocked() {
	if !c.detached.Swap(true) {
		close(c.detachCh)
	}
}

// detachForSessionClose is Session.Close's half of the teardown. The demux
// parks on channel sends, which closing the Conn cannot influence, so the
// session has to free them here or its recvLoop join never completes. err is
// what a Recv parked on this call reports, keeping the reason abortRecv used
// to enqueue.
func (c *call) detachForSessionClose(err error) {
	c.mu.Lock()
	c.recvDone = true
	if c.detachErr == nil {
		c.detachErr = err
	}
	c.detachLocked()
	c.mu.Unlock()
}

func (c *call) detachReason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.detachErr != nil {
		return c.detachErr
	}
	return ErrCallClosed
}

func (c *call) drainInbox() {
	for {
		select {
		case ev := <-c.inbox:
			if ev.data {
				c.releaseDataSlot()
			}
			if ev.release != nil {
				ev.release()
				c.noteRelease(int64(cap(ev.payload)))
			}
		default:
			return
		}
	}
}

// serverCall wraps call with Accept.
type serverCall struct {
	*call
}

func (c *serverCall) Accept(m descriptor.Method) error {
	if m.IsZero() {
		return status.Error(status.InvalidArgument, "envelope: zero Method")
	}
	switch c.sess.kind {
	case kindByteStream, kindMessage:
		return nil
	case kindDatagram:
		if m.Shape() != descriptor.Unary {
			return status.Error(status.Unimplemented, fmt.Sprintf(
				"envelope: shape %v unsupported on DatagramCarrier (Framing=envelope Shape=%v); need Unary",
				m.Shape(), m.Shape()))
		}
		return nil
	default:
		return status.Error(status.Unimplemented, fmt.Sprintf(
			"envelope: shape %v unsupported on carrier kind %d", m.Shape(), c.sess.kind))
	}
}
