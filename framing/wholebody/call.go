package wholebody

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/httpstatus"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// call is one wholebody Call (client or server). Unary only: at most one
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

	initiator bool
	localCar  bool

	mu sync.Mutex

	reqBuf     []byte // client: buffered request; server unused for send
	respBuf    []byte // server: buffered success body
	reqSent    bool   // client HalfClose done / server request consumed
	respSeen   bool   // client: response body delivered (or error returned)
	halfClosed bool
	finished   bool
	closed     bool
	sawTerminal bool

	sending atomic.Bool
	recving atomic.Bool
}

func (c *call) Method() string { return c.method }

func (c *call) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *call) markBad() {
	if c.client != nil {
		c.client.markBad()
	}
}

// SendHeaders is unsupported on wholebody/http1: early commit would freeze a
// 200 that Finish could no longer change (§4.6 / §9-8).
func (c *call) SendHeaders() error {
	if c.initiator {
		return status.Error(status.Unimplemented, "framing/wholebody: SendHeaders unsupported for initiator")
	}
	return status.Error(status.Unimplemented,
		"framing/wholebody: SendHeaders unsupported (Framing=wholebody; UnaryResponseWriter commits at Finish)")
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
		return status.Error(status.Internal, "framing/wholebody: unary allows one request message")
	}
	if !c.initiator && c.respBuf != nil {
		c.mu.Unlock()
		return status.Error(status.Internal, "framing/wholebody: unary allows one response message")
	}
	c.mu.Unlock()

	data := payload
	if data == nil {
		data = []byte{}
	}
	if c.maxMsg > 0 && int64(len(data)) > c.maxMsg {
		return fmt.Errorf("framing/wholebody: Send payload %d > max %d", len(data), c.maxMsg)
	}
	cp := append([]byte(nil), data...)

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
		return status.Error(status.Unimplemented, "framing/wholebody: HalfClose unsupported for responder")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
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
			c.markBad()
			return err
		}
	}
	sc, ok := c.carrier.(transport.SendCloser)
	if !ok {
		return status.Error(status.Unimplemented, "framing/wholebody: carrier has no CloseSend")
	}
	if err := sc.CloseSend(); err != nil {
		c.markBad()
		return err
	}
	return nil
}

func (c *call) Finish(err error) error {
	if c.initiator {
		return status.Error(status.Unimplemented, "framing/wholebody: Finish unsupported for initiator")
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
		return status.Error(status.Unimplemented, "framing/wholebody: carrier has no UnaryResponseWriter")
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
		ctype = ContentType(c.codec)
	} else {
		httpSt = httpstatus.ToHTTP(status.CodeOf(err))
		body = EncodeErrorBody(err)
		ctype = "application/json"
	}

	hs := transport.Headers{{Name: "content-type", Value: ctype}}
	hs = append(hs, EncodeMetadata(userMD)...)

	if werr := urw.WriteResponse(httpSt, hs, body); werr != nil {
		c.markBad()
		return werr
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
	return data, func() {}, nil
}

func (c *call) recvClient() ([]byte, func(), error) {
	rh, ok := c.carrier.(transport.ResponseHeaderReader)
	if !ok {
		return nil, nil, fmt.Errorf("framing/wholebody: carrier missing ResponseHeaderReader")
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
	if c.md != nil {
		_ = metadata.SetIncomingHeaders(c.md, DecodeMetadata(hs))
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
			c.markBad()
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
		c.markBad()
		return nil, nil, err
	}
	c.mu.Lock()
	c.respSeen = true
	c.sawTerminal = true
	c.mu.Unlock()
	return data, func() {}, nil
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
		return status.Error(status.InvalidArgument, "framing/wholebody: zero Method")
	}
	if m.Shape() != descriptor.Unary {
		return status.Error(status.Unimplemented, fmt.Sprintf(
			"framing/wholebody: shape %v unsupported (Framing=wholebody Shape=%v; only Unary)",
			m.Shape(), m.Shape()))
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
		return nil, fmt.Errorf("framing/wholebody: message exceeds max size %d", limit)
	}
	return b, nil
}

var (
	errConcurrentSend = fmt.Errorf("framing/wholebody: concurrent Send")
	errConcurrentRecv = fmt.Errorf("framing/wholebody: concurrent Recv")
	errSendFinished   = fmt.Errorf("framing/wholebody: send already finished")
	errCallClosed     = fmt.Errorf("framing/wholebody: call closed")
)
