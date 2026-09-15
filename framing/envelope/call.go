package envelope

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
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
	err     error
	headers bool // HEADERS arrived (payload ignored); Recv does not surface these
}

// call is the shared Call implementation for initiator and responder.
type call struct {
	sess     *session
	callID   uint64
	method   string
	md       metadata.CallMetadata
	shape    descriptor.Shape
	initiator bool

	inbox chan recvEvent

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
	terminalRead   bool // initiator read STATUS (or EOF after OK)
	statusErr      error // non-nil when STATUS code != OK; returned once from Recv
	statusReturned bool
	recvDone       bool // demux finished (STATUS or fatal)
	detachCh       chan struct{}

	sending atomic.Bool
	recving atomic.Bool
}

func newCall(s *session, id uint64, method string, initiator bool, md metadata.CallMetadata, shape descriptor.Shape) *call {
	return &call{
		sess:      s,
		callID:    id,
		method:    method,
		md:        md,
		shape:     shape,
		initiator: initiator,
		inbox:     make(chan recvEvent, 64),
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
		c.enqueue(recvEvent{payload: payload})
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
		c.enqueue(recvEvent{err: errStatusPending})
	case TypeOpen:
		c.abortRecv(fmt.Errorf("envelope: unexpected OPEN on active call"))
	default:
		c.abortRecv(fmt.Errorf("envelope: unknown frame type %d", f.Type))
	}
}

func (c *call) enqueue(ev recvEvent) {
	select {
	case c.inbox <- ev:
	case <-c.detachCh:
	}
}

func (c *call) ensureImpliedHeaders() {
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

func (c *call) deliverEnd() {
	c.mu.Lock()
	c.peerHalfClosed = true
	c.mu.Unlock()
	c.enqueue(recvEvent{err: io.EOF})
}

func (c *call) abortRecv(err error) {
	c.mu.Lock()
	c.recvDone = true
	c.mu.Unlock()
	c.enqueue(recvEvent{err: err})
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

	var hdrs []Header
	if c.md != nil {
		hdrs = mdToHeaders(c.md.OutgoingHeaders())
	}
	if err := c.sess.writeFrame(Frame{
		Type:    TypeHeaders,
		CallID:  c.callID,
		Headers: hdrs,
	}); err != nil {
		c.sess.markBad()
		return err
	}
	c.mu.Lock()
	c.headersSent = true
	c.mu.Unlock()
	if c.md != nil {
		_ = metadata.FreezeOutgoingHeaders(c.md)
	}
	return nil
}

func (c *call) maybeAutoHeaders() error {
	if c.initiator {
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
	if c.initiator && c.halfClosed {
		c.mu.Unlock()
		return ErrSendFinished
	}
	if !c.initiator && c.finished {
		c.mu.Unlock()
		return ErrSendFinished
	}
	c.mu.Unlock()

	if c.initiator {
		if err := c.ensureOpen(); err != nil {
			return err
		}
	} else {
		if err := c.maybeAutoHeaders(); err != nil {
			return err
		}
	}

	data := payload
	if data == nil {
		data = []byte{}
	}
	// Copy so caller may reuse the buffer after return.
	cp := append([]byte(nil), data...)
	if err := c.sess.writeFrame(Frame{
		Type:   TypeData,
		CallID: c.callID,
		Data:   cp,
	}); err != nil {
		c.sess.markBad()
		return err
	}
	if c.cfgMaxMessage() > 0 && int64(len(cp)) > c.cfgMaxMessage() {
		// Already on the wire; mark bad for oversized (Validate on frame uses MaxFrameSize).
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
		c.sess.markBad()
		return err
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
	if c.halfClosed {
		c.mu.Unlock()
		return nil
	}
	openSent := c.openSent
	c.mu.Unlock()

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
			c.sess.markBad()
			return err
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
		c.sess.markBad()
		return err
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
		c.sess.markBad()
		return werr
	}
	c.mu.Lock()
	c.finished = true
	c.headersSent = true
	c.mu.Unlock()
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
		// STATUS OK already observed and pending DATA drained → EOF.
		if c.sawStatus && c.statusErr == nil {
			// Still may have buffered DATA ahead of the status event; fall through
			// to inbox unless status event already consumed.
		}
		c.mu.Unlock()

		ev, ok := <-c.inbox
		if !ok {
			c.mu.Lock()
			c.terminalRead = true
			c.mu.Unlock()
			return nil, nil, io.EOF
		}
		if ev.headers {
			continue
		}
		if ev.err != nil {
			if errors.Is(ev.err, errStatusPending) {
				c.mu.Lock()
				stErr := c.statusErr
				c.sawStatus = true
				if stErr != nil {
					c.statusReturned = true
					c.mu.Unlock()
					return nil, nil, stErr
				}
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
				c.terminalRead = true
				c.mu.Unlock()
				return nil, nil, io.EOF
			}
			c.sess.markBad()
			return nil, nil, ev.err
		}
		p := ev.payload
		return p, func() {}, nil
	}
}

func (c *call) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	terminalOK := c.terminalRead || (c.initiator && c.sawStatus) || (!c.initiator && c.finished)
	if c.initiator {
		terminalOK = c.sawStatus || c.terminalRead
	} else {
		// Responder Close after Finish is fine; residual peer frames drained on next Accept.
		terminalOK = true
	}
	c.mu.Unlock()

	// Wake blocked Recv.
	select {
	case c.inbox <- recvEvent{err: ErrCallClosed}:
	default:
	}

	c.sess.mu.Lock()
	if c.sess.active == c {
		c.sess.active = nil
		c.sess.mode = modeIdle
		if c.initiator && !terminalOK {
			c.sess.reusable = false
		}
	}
	c.sess.mu.Unlock()
	c.sess.wakeRead()
	c.sess.signal()
	c.sess.clearReadDeadline()

	select {
	case <-c.detachCh:
	default:
		close(c.detachCh)
	}
	return nil
}

// serverCall wraps call with Accept.
type serverCall struct {
	*call
}

func (c *serverCall) Accept(m descriptor.Method) error {
	if m.IsZero() {
		return status.Error(status.InvalidArgument, "envelope: zero Method")
	}
	// Envelope over stream carriers supports all four shapes.
	switch c.sess.kind {
	case kindByteStream, kindMessage:
		return nil
	default:
		return status.Error(status.Unimplemented, fmt.Sprintf(
			"envelope: shape %v unsupported on carrier kind %d", m.Shape(), c.sess.kind))
	}
}
