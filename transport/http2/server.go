package http2

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos/transport"
)

// serverConn is one inbound HTTP/2 request as a CarrierConn.
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

// serverCarrier is the server-side byte-stream + HTTP control-plane carrier.
type serverCarrier struct {
	w http.ResponseWriter
	r *http.Request

	mu sync.Mutex

	headersWritten bool
	finished       bool
	aborted        bool

	// handlerDone is set once the net/http handler returned. The response
	// writer is retired at that point and any write to it panics.
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

// Write writes response body bytes. Implicitly sends 200 if headers were not
// written yet (net/http behavior). Flushes so streaming peers (e.g. grpc-go
// bidi) observe DATA without waiting for the handler to return.
func (c *serverCarrier) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.aborted || c.finished {
		c.mu.Unlock()
		return 0, errAborted
	}
	if !c.headersWritten {
		c.headersWritten = true
	}
	c.mu.Unlock()
	n, err := c.w.Write(p)
	if f, ok := c.w.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}

// WriteHeaders submits and flushes initial response headers.
func (c *serverCarrier) WriteHeaders(status int, headers transport.Headers) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return errAborted
	}
	if c.headersWritten || c.finished {
		return errHeadersWritten
	}
	applyHeaders(c.w.Header(), headers)
	c.w.WriteHeader(status)
	if f, ok := c.w.(http.Flusher); ok {
		f.Flush()
	}
	c.headersWritten = true
	return nil
}

// Finish completes the response. If initial headers were never written, status,
// initial, and trailers are sent together as a single HEADERS+END_STREAM
// (trailers-only). Otherwise only trailers are written.
func (c *serverCarrier) Finish(status int, initial, trailers transport.Headers) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return errAborted
	}
	if c.finished {
		return errFinished
	}
	if !c.headersWritten {
		applyHeaders(c.w.Header(), initial)
		applyHeaders(c.w.Header(), trailers)
		c.w.WriteHeader(status)
		c.headersWritten = true
		c.finished = true
		return nil
	}
	for _, h := range trailers {
		c.w.Header().Add(http.TrailerPrefix+h.Name, h.Value)
	}
	c.finished = true
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
	// Writing the deadline status is only meaningful while the handler still
	// holds the exchange: after it returned, net/http has retired the response
	// writer and WriteHeader panics.
	if !c.headersWritten && !c.finished && !c.handlerDone.Load() {
		c.w.WriteHeader(http.StatusRequestTimeout)
		c.headersWritten = true
		c.finished = true
	}
	return nil
}

var (
	errHeadersWritten = errString("http2: headers already written")
	errFinished       = errString("http2: already finished")
)

type errString string

func (e errString) Error() string { return string(e) }
