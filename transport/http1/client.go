package http1

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos/transport"
)

// streamConn is a client endpoint handle (StreamConn). Close releases the
// handle and aborts in-flight streams; it does not close the Transport-owned
// http.Client.
type streamConn struct {
	client *http.Client
	base   string
	// detach removes this handle from the Transport's tracking map when it
	// closes, so the map never retains a closed endpoint handle.
	detach func()

	mu       sync.Mutex
	closed   atomic.Bool
	carriers map[*clientCarrier]struct{}
}

func newStreamConn(client *http.Client, base string) *streamConn {
	return &streamConn{
		client:   client,
		base:     base,
		carriers: make(map[*clientCarrier]struct{}),
	}
}

// OpenStream starts one HTTP/1.1 request and returns a writable Carrier before
// response headers arrive.
func (c *streamConn) OpenStream(ctx context.Context, p transport.RequestPreface) (transport.Carrier, error) {
	if c.closed.Load() {
		return nil, errAborted
	}

	target := p.RequestTarget
	if target == "" {
		target = "/"
	}
	u, err := streamURL(c.base, target)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	reqCtx, cancel := context.WithCancel(ctx)
	car := &clientCarrier{
		conn:   c,
		pw:     pw,
		pr:     pr,
		cancel: cancel,
		ready:  make(chan struct{}),
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, u, pr)
	if err != nil {
		cancel()
		_ = pw.Close()
		_ = pr.Close()
		return nil, err
	}
	applyHeaders(req.Header, p.Headers)

	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		cancel()
		_ = pw.Close()
		_ = pr.Close()
		return nil, errAborted
	}
	c.carriers[car] = struct{}{}
	car.tracked.Store(true)
	c.mu.Unlock()

	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			_ = pr.CloseWithError(err)
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}
		if !car.finish(resp, err) && resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()

	return car, nil
}

// Close marks the endpoint handle closed and aborts in-flight streams.
// Idempotent. Does not shut down the shared http.Client.
func (c *streamConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.mu.Lock()
	carriers := make([]*clientCarrier, 0, len(c.carriers))
	for car := range c.carriers {
		carriers = append(carriers, car)
	}
	c.carriers = nil
	c.mu.Unlock()
	for _, car := range carriers {
		_ = car.Abort()
	}
	if c.detach != nil {
		c.detach()
	}
	return nil
}

// trackedCarrierCount reports in-flight OpenStream carriers (tests).
func (c *streamConn) trackedCarrierCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.carriers)
}

// streamURL joins an endpoint base URL with an HTTP request target (:path).
func streamURL(base, target string) (string, error) {
	bu, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if target == "" || target[0] != '/' {
		target = "/" + target
	}
	if tu, err := url.ParseRequestURI(target); err == nil {
		bu.Path = tu.Path
		bu.RawPath = tu.RawPath
		bu.RawQuery = tu.RawQuery
	} else {
		bu.Path = target
		bu.RawQuery = ""
	}
	bu.Fragment = ""
	return bu.String(), nil
}

// clientCarrier is one client HTTP/1.1 request/response exchange.
type clientCarrier struct {
	conn   *streamConn
	pw     *io.PipeWriter
	pr     *io.PipeReader
	cancel context.CancelFunc

	ready      chan struct{}
	finishOnce sync.Once
	tracked    atomic.Bool // true while present in streamConn.carriers

	mu      sync.Mutex
	resp    *http.Response
	respErr error
	aborted bool

	sendClosed atomic.Bool
}

// untrack drops this carrier from its endpoint handle; a carrier is tracked
// only while its exchange can still be aborted.
func (c *clientCarrier) untrack() {
	if c.conn != nil {
		c.conn.untrack(c)
	}
}

func (c *streamConn) untrack(car *clientCarrier) {
	if car == nil || !car.tracked.Swap(false) {
		return
	}
	c.mu.Lock()
	delete(c.carriers, car)
	c.mu.Unlock()
}

// finish records the RoundTrip result once. Reports whether this call won.
func (c *clientCarrier) finish(resp *http.Response, err error) bool {
	won := false
	c.finishOnce.Do(func() {
		won = true
		c.mu.Lock()
		c.resp, c.respErr = resp, err
		c.mu.Unlock()
		close(c.ready)
		if err != nil {
			// No response will arrive: the exchange is over.
			c.untrack()
		}
	})
	return won
}

func (c *clientCarrier) waitReady() error {
	<-c.ready
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.respErr != nil {
		return c.respErr
	}
	if c.resp == nil {
		return errAborted
	}
	return nil
}

// Write writes to the request body. Safe before response headers arrive.
//
// A failed body write is a send-direction failure: net/http stops reading the
// request body when the response arrives first, when the peer refuses the body,
// or when the round trip itself failed. Only the last case proves no response
// can arrive; the others leave the response readable, so framing must be able
// to tell them apart (transport.SendError).
func (c *clientCarrier) Write(p []byte) (int, error) {
	n, err := c.pw.Write(p)
	if err != nil {
		return n, transport.WrapSendError(err, c.receiveOpen())
	}
	return n, nil
}

// receiveOpen reports whether a response can still be read on this exchange.
// Unknown states answer true: a subsequent Read produces the definitive result.
func (c *clientCarrier) receiveOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted {
		return false
	}
	// A round trip that failed with no response will never produce one.
	return !(c.respErr != nil && c.resp == nil)
}

// Read reads the response body, waiting for headers first if needed. The end
// of the body ends the exchange, so the carrier stops being tracked there.
func (c *clientCarrier) Read(p []byte) (int, error) {
	if err := c.waitReady(); err != nil {
		return 0, err
	}
	n, err := c.resp.Body.Read(p)
	if err != nil {
		_ = c.resp.Body.Close()
		c.untrack()
	}
	return n, err
}

// CloseSend ends the request body (HTTP/1.1 end of chunked/body stream).
// Failure semantics match Write: a half-close that cannot be delivered finishes
// the send direction without necessarily ending the exchange.
func (c *clientCarrier) CloseSend() error {
	if c.sendClosed.Swap(true) {
		return nil
	}
	return transport.WrapSendError(c.pw.Close(), c.receiveOpen())
}

// ResponseStatus waits for response headers and returns the HTTP status code.
func (c *clientCarrier) ResponseStatus() (int, error) {
	if err := c.waitReady(); err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resp.StatusCode, nil
}

// ResponseHeaders waits for response headers.
func (c *clientCarrier) ResponseHeaders() (transport.Headers, error) {
	if err := c.waitReady(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return headersFromHTTP(c.resp.Header), nil
}

// Abort cancels the request and unblocks I/O. Idempotent.
func (c *clientCarrier) Abort() error {
	c.mu.Lock()
	if c.aborted {
		c.mu.Unlock()
		return nil
	}
	c.aborted = true
	resp := c.resp
	c.mu.Unlock()

	c.cancel()
	_ = c.pw.CloseWithError(errAborted)
	_ = c.pr.CloseWithError(errAborted)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	_ = c.finish(nil, errAborted)
	c.untrack()
	return nil
}
