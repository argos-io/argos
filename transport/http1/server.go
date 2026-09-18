package http1

import (
	"bytes"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos/transport"
)

// serverConn is one inbound HTTP/1.1 request as a CarrierConn.
type serverConn struct {
	carrier *serverCarrier
	closed  atomic.Bool
}

func newServerConn(w http.ResponseWriter, r *http.Request) *serverConn {
	return &serverConn{carrier: newServerCarrier(w, r)}
}

// Carrier returns the per-request exchange carrier.
func (c *serverConn) Carrier() transport.Carrier { return c.carrier }

// markHandlerDone records that the net/http handler returned and the response
// writer is retired.
func (c *serverConn) markHandlerDone() { c.carrier.handlerDone.Store(true) }

// Close aborts the exchange. Idempotent.
func (c *serverConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.carrier.Abort()
}

// serverCarrier is the server-side byte-stream + UnaryResponseWriter carrier.
//
// Write buffers response body bytes without calling WriteHeader — that is the
// whole point of UnaryResponseWriter: framing/httpunary can "Send" into the
// buffer, then Finish via WriteResponse with either success or error status.
type serverCarrier struct {
	w http.ResponseWriter
	r *http.Request

	mu sync.Mutex

	buf      bytes.Buffer
	finished bool
	aborted  bool

	// handlerDone is set once the net/http handler returned. The response
	// writer must not be touched after that (WriteHeader panics or logs).
	handlerDone atomic.Bool

	reqHeaders transport.Headers
	target     string
	method     string
}

func newServerCarrier(w http.ResponseWriter, r *http.Request) *serverCarrier {
	target := r.URL.RequestURI()
	if target == "" {
		target = r.URL.Path
	}
	return &serverCarrier{
		w:          w,
		r:          r,
		reqHeaders: headersFromHTTP(r.Header),
		target:     target,
		method:     r.Method,
	}
}

// RequestTarget returns the HTTP request target (:path / RequestURI).
func (c *serverCarrier) RequestTarget() string { return c.target }

// RequestMethod returns the HTTP request method.
func (c *serverCarrier) RequestMethod() string { return c.method }

// RequestHeaders returns opaque inbound request headers.
func (c *serverCarrier) RequestHeaders() transport.Headers { return c.reqHeaders }

// Read reads from the request body.
func (c *serverCarrier) Read(p []byte) (int, error) {
	return c.r.Body.Read(p)
}

// Write appends to an in-memory response buffer. It must not call WriteHeader
// or write to the ResponseWriter — otherwise a later WriteResponse(500, …)
// could not change the status (probe 0.10 / §4.6).
func (c *serverCarrier) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted || c.finished {
		return 0, errAborted
	}
	return c.buf.Write(p)
}

// WriteResponse commits status, headers, and body in one shot. The body
// argument is the final response body (framing/httpunary passes it at Finish);
// any prior Write buffer is discarded so a handler "Send" then error path can
// replace a buffered success body with an error body.
//
// Idempotent after the first successful commit: a second call returns
// errFinished and does not change what is already on the wire.
func (c *serverCarrier) WriteResponse(status int, headers transport.Headers, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return errAborted
	}
	if c.finished {
		return errFinished
	}
	applyHeaders(c.w.Header(), headers)
	c.w.WriteHeader(status)
	if len(body) > 0 {
		if _, err := c.w.Write(body); err != nil {
			c.finished = true
			c.buf.Reset()
			return err
		}
	}
	c.finished = true
	c.buf.Reset()
	return nil
}

// Abort interrupts in-flight I/O on this request. Idempotent.
func (c *serverCarrier) Abort() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return nil
	}
	c.aborted = true
	if c.r != nil && c.r.Body != nil {
		_ = c.r.Body.Close()
	}
	if !c.finished && !c.handlerDone.Load() {
		c.w.WriteHeader(http.StatusRequestTimeout)
		c.finished = true
	}
	return nil
}

var errFinished = errString("http1: already finished")

type errString string

func (e errString) Error() string { return string(e) }
