package httpunary

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/httpstatus"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// call is one httpunary Call (client or server). Unary only: at most one
// buffered request/response body, committed on HalfClose (client) or Finish
// (server via UnaryResponseWriter).
type call struct {
	client *clientSession
	server *serverSession

	carrier transport.Carrier
	body    transport.ByteStreamCarrier
	method  string
	md      metadata.CallMetadata
	cfg     framing.Config
	maxMsg  int64
	codec   string

	contentType func(codecName string) string
	encodeError func(err error) (contentType string, body []byte)

	initiator bool
	localCar  bool

	mu sync.Mutex

	reqBuf      []byte // client: buffered request; server unused for send
	respBuf     []byte // server: buffered success body
	reqSent     bool   // client HalfClose done / server request consumed
	respSeen    bool   // client: response body delivered (or error returned)
	halfClosed  bool
	finished    bool
	closed      bool
	sawTerminal bool

	// sendErr is the failure that ended the send direction (the request body on
	// the initiator, the response on the responder). It is sticky: a partial
	// body write cannot be rolled back. A transport.SendError with
	// ReceiveOpen() true leaves the exchange readable, so the initiator still
	// gets the response and the remote status.
	sendErr error

	budget budget.Budget

	sending atomic.Bool
	recving atomic.Bool
}

func (c *call) SetBudget(b budget.Budget) {
	c.mu.Lock()
	c.budget = b
	c.mu.Unlock()
}

func (c *call) chargePayload(payload []byte) (func(), error) {
	c.mu.Lock()
	b := c.budget
	c.mu.Unlock()
	return budget.ChargeSlice(b, payload)
}

func (c *call) Method() string { return c.method }

func (c *call) Deadline() (time.Time, bool) { return time.Time{}, false }

// checkInboundMeta rejects request/response metadata larger than the configured
// inbound limit. MaxMetadataSize only constrains what we send.
func checkInboundMeta(cfg framing.Config, md metadata.Metadata) error {
	max := cfg.MaxInboundMetadataSize
	if max <= 0 || len(md) == 0 {
		return nil
	}
	if sz := metadata.WireSize(md); sz > max {
		return status.Error(status.ResourceExhausted,
			fmt.Sprintf("framing/httpunary: inbound metadata %d exceeds limit %d", sz, max))
	}
	return nil
}

// markBadUnlessLimit poisons the session unless err is a call-scoped inbound
// limit. Reuse() is Concurrent, so one response exceeding MaxMessageSize or
// MaxInboundMetadataSize must not drop the endpoint handle out from under the
// other calls sharing the Conn: the peer is not at fault for the limit we set,
// and the connection is undamaged.
func (c *call) markBadUnlessLimit(err error) {
	if status.CodeOf(err) == status.ResourceExhausted {
		return
	}
	c.markBad()
}

func (c *call) markBad() {
	if c.client != nil {
		c.client.markBad()
	}
}

// sendFailure reports whether err is a transport send failure on an exchange
// whose receive direction is still open. Framing must keep such a carrier
// readable: the peer may still deliver the response and the remote status.
func sendFailure(err error) bool {
	se, ok := transport.AsSendError(err)
	return ok && se.ReceiveOpen()
}

// sendFailed records the error that ended the send direction and returns it to
// the caller unchanged, so stream.Wrap can map a still-receivable exchange to
// ErrSendClosed. The client session (a handle shared by concurrent calls on
// one HTTP endpoint) is only dropped when the transport reports the exchange
// unrecoverable.
func (c *call) sendFailed(err error) error {
	c.mu.Lock()
	if c.sendErr == nil {
		c.sendErr = err
	}
	c.mu.Unlock()
	if !sendFailure(err) {
		c.markBad()
	}
	return err
}

// wakeRead unblocks a Recv parked in a body Read by expiring the carrier's read
// deadline. Call.Close must unblock in-flight Recv/Send (framing.Call), and
// poisoning the deadline is the only way to interrupt a body read.
func (c *call) wakeRead() {
	type deadliner interface {
		SetReadDeadline(time.Time) error
	}
	if d, ok := c.carrier.(deadliner); ok {
		_ = d.SetReadDeadline(time.Now())
		return
	}
	type both interface {
		SetDeadline(time.Time) error
	}
	if d, ok := c.carrier.(both); ok {
		_ = d.SetDeadline(time.Now())
	}
}

// SendHeaders is unsupported on httpunary/http1: early commit would freeze a
// 200 that Finish could no longer change (§4.6 / §9-8).
func (c *call) SendHeaders() error {
	if c.initiator {
		return status.Error(status.Unimplemented, "framing/httpunary: SendHeaders unsupported for initiator")
	}
	return status.Error(status.Unimplemented,
		"framing/httpunary: SendHeaders unsupported (UnaryResponseWriter commits at Finish)")
}

func (c *call) Send(payload []byte) error {
	if !c.sending.CompareAndSwap(false, true) {
		return errConcurrentSend
	}
	defer c.sending.Store(false)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.initiator && c.halfClosed {
		c.mu.Unlock()
		return errSendFinished
	}
	if !c.initiator && c.finished {
		c.mu.Unlock()
		return errSendFinished
	}
	if c.initiator && c.reqBuf != nil {
		c.mu.Unlock()
		return status.Error(status.Internal, "framing/httpunary: unary allows one request message")
	}
	if !c.initiator && c.respBuf != nil {
		c.mu.Unlock()
		return status.Error(status.Internal, "framing/httpunary: unary allows one response message")
	}
	c.mu.Unlock()

	data := payload
	if data == nil {
		data = []byte{}
	}
	if c.maxMsg > 0 && int64(len(data)) > c.maxMsg {
		return status.Error(status.ResourceExhausted,
			fmt.Sprintf("framing/httpunary: Send payload %d > max %d", len(data), c.maxMsg))
	}
	cp := append([]byte(nil), data...)
	rel, err := c.chargePayload(cp)
	if err != nil {
		return err
	}
	if rel != nil {
		defer rel()
	}

	c.mu.Lock()
	if c.initiator {
		c.reqBuf = cp
	} else {
		c.respBuf = cp
	}
	c.mu.Unlock()
	return nil
}

func (c *call) HalfClose() error {
	if !c.initiator {
		return status.Error(status.Unimplemented, "framing/httpunary: HalfClose unsupported for responder")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
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
	buf := c.reqBuf
	if buf == nil {
		buf = []byte{}
	}
	c.halfClosed = true
	c.mu.Unlock()

	if len(buf) > 0 {
		if _, err := c.body.Write(buf); err != nil {
			return c.sendFailed(err)
		}
	}
	sc, ok := c.carrier.(transport.SendCloser)
	if !ok {
		return status.Error(status.Unimplemented, "framing/httpunary: carrier has no CloseSend")
	}
	if err := sc.CloseSend(); err != nil {
		return c.sendFailed(err)
	}
	return nil
}

func (c *call) Finish(err error) error {
	if c.initiator {
		return status.Error(status.Unimplemented, "framing/httpunary: Finish unsupported for initiator")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.finished {
		c.mu.Unlock()
		return errSendFinished
	}
	successBody := c.respBuf
	c.mu.Unlock()

	urw, ok := c.carrier.(transport.UnaryResponseWriter)
	if !ok {
		return status.Error(status.Unimplemented, "framing/httpunary: carrier has no UnaryResponseWriter")
	}

	var userMD metadata.Metadata
	if c.md != nil {
		userMD = c.md.OutgoingHeaders()
		_ = metadata.FreezeOutgoingHeaders(c.md)
		_ = metadata.FreezeOutgoingTrailers(c.md)
	}

	var (
		httpSt int
		body   []byte
		ctype  string
	)
	if err == nil {
		httpSt = http.StatusOK
		body = successBody
		if body == nil {
			body = []byte{}
		}
		if c.contentType != nil {
			ctype = c.contentType(c.codec)
		} else {
			ctype = ContentType(c.codec)
		}
	} else {
		httpSt = httpstatus.ToHTTP(status.CodeOf(err))
		if c.encodeError != nil {
			ctype, body = c.encodeError(err)
		} else {
			body = EncodeErrorBody(err)
			ctype = "application/json"
		}
	}

	hs := transport.Headers{{Name: "content-type", Value: ctype}}
	hs = append(hs, EncodeMetadata(userMD)...)

	if werr := urw.WriteResponse(httpSt, hs, body); werr != nil {
		return c.sendFailed(werr)
	}
	c.mu.Lock()
	c.finished = true
	c.sawTerminal = true
	c.mu.Unlock()
	return nil
}

func (c *call) Recv() (payload []byte, release func(), err error) {
	if !c.recving.CompareAndSwap(false, true) {
		return nil, nil, errConcurrentRecv
	}
	defer c.recving.Store(false)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, errCallClosed
	}
	if c.initiator && c.respSeen {
		c.mu.Unlock()
		return nil, nil, io.EOF
	}
	if !c.initiator && c.reqSent {
		c.mu.Unlock()
		return nil, nil, io.EOF
	}
	c.mu.Unlock()

	if c.initiator {
		return c.recvClient()
	}
	return c.recvServer()
}

func (c *call) recvServer() ([]byte, func(), error) {
	limit := c.maxMsg
	if limit <= 0 {
		limit = 4 << 20
	}
	data, err := readAllLimited(c.body, limit)
	if err != nil {
		c.markBad()
		return nil, nil, err
	}
	c.mu.Lock()
	c.reqSent = true
	c.mu.Unlock()
	rel, err := c.chargePayload(data)
	if err != nil {
		return nil, nil, err
	}
	if rel == nil {
		rel = func() {}
	}
	return data, rel, nil
}

func (c *call) recvClient() ([]byte, func(), error) {
	rh, ok := c.carrier.(transport.ResponseHeaderReader)
	if !ok {
		return nil, nil, fmt.Errorf("framing/httpunary: carrier missing ResponseHeaderReader")
	}
	httpSt, err := rh.ResponseStatus()
	if err != nil {
		c.markBad()
		return nil, nil, err
	}
	hs, err := rh.ResponseHeaders()
	if err != nil {
		c.markBad()
		return nil, nil, err
	}
	inMD := DecodeMetadata(hs)
	if err := checkInboundMeta(c.cfg, inMD); err != nil {
		c.markBadUnlessLimit(err)
		return nil, nil, err
	}
	if c.md != nil {
		_ = metadata.SetIncomingHeaders(c.md, inMD)
	}

	limit := c.maxMsg
	if limit <= 0 {
		limit = 4 << 20
	}
	if httpSt != http.StatusOK {
		if limit > maxErrorBody {
			limit = maxErrorBody
		}
		body, rerr := readAllLimited(c.body, limit)
		if rerr != nil && rerr != io.EOF {
			c.markBadUnlessLimit(rerr)
			return nil, nil, rerr
		}
		stErr := DecodeErrorBody(body, httpstatus.FromHTTP(httpSt))
		c.mu.Lock()
		c.respSeen = true
		c.sawTerminal = true
		c.mu.Unlock()
		return nil, nil, stErr
	}

	data, err := readAllLimited(c.body, limit)
	if err != nil {
		c.markBadUnlessLimit(err)
		return nil, nil, err
	}
	c.mu.Lock()
	c.respSeen = true
	c.sawTerminal = true
	c.mu.Unlock()
	rel, err := c.chargePayload(data)
	if err != nil {
		return nil, nil, err
	}
	if rel == nil {
		rel = func() {}
	}
	return data, rel, nil
}

func (c *call) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	terminal := c.sawTerminal || c.finished
	initiator := c.initiator
	c.mu.Unlock()

	// Wake a blocked body read before anything else: Close owes the caller an
	// unblocked Recv/Send regardless of how the call ended.
	c.wakeRead()

	if initiator && !terminal {
		c.markBad()
	}
	if c.localCar {
		_ = c.carrier.Abort()
	}
	if c.client != nil {
		c.client.endFlight()
	}
	return nil
}

type serverCall struct {
	*call
}

func (c *serverCall) Accept(m descriptor.Method) error {
	if m.IsZero() {
		return status.Error(status.InvalidArgument, "framing/httpunary: zero Method")
	}
	if m.Shape() != descriptor.Unary {
		return status.Error(status.Unimplemented, fmt.Sprintf(
			"framing/httpunary: shape %v unsupported (only Unary)", m.Shape()))
	}
	return nil
}

func readAllLimited(r io.Reader, limit int64) ([]byte, error) {
	lr := io.LimitReader(r, limit+1)
	b, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		// ResourceExhausted so callers can tell a limit we chose from damage to
		// the connection; see markBadUnlessLimit.
		return nil, status.Error(status.ResourceExhausted,
			fmt.Sprintf("framing/httpunary: message exceeds max size %d", limit))
	}
	return b, nil
}

var (
	errConcurrentSend = fmt.Errorf("framing/httpunary: concurrent Send")
	errConcurrentRecv = fmt.Errorf("framing/httpunary: concurrent Recv")
	errSendFinished   = fmt.Errorf("framing/httpunary: send already finished")
	errCallClosed     = fmt.Errorf("framing/httpunary: call closed")
)
