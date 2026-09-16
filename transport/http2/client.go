package http2

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

// OpenStream starts one HTTP/2 request and returns a writable Carrier before
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

// streamURL joins an endpoint base URL with an HTTP request target (:path).
func streamURL(base, target string) (string, error) {
	bu, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if target == "" || target[0] != '/' {
		target = "/" + target
	}
	// Preserve query in target if Framing put one in RequestTarget.
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

// clientCarrier is one client HTTP/2 stream.
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

func (c *streamConn) untrack(car *clientCarrier) {
	if car == nil || !car.tracked.Swap(false) {
		return
	}
	c.mu.Lock()
	delete(c.carriers, car)
	c.mu.Unlock()
}

// trackedCarrierCount reports in-flight OpenStream carriers (tests).
func (c *streamConn) trackedCarrierCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.carriers)
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
			// The exchange is over for the caller. Without this the carrier
			// stayed in the in-flight map until the whole StreamConn closed.
			c.untrackSelf()
		}
	})
	return won
}

// untrackSelf removes this carrier from the connection's in-flight map.
func (c *clientCarrier) untrackSelf() {
	if c.conn != nil {
		c.conn.untrack(c)
	}
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
func (c *clientCarrier) Write(p []byte) (int, error) {
	n, err := c.pw.Write(p)
	return n, c.sendErr(err)
}

// sendErr classifies a request-body write failure. A failed write says nothing
// about the response direction: an HTTP/2 stream is independent, so a server
// that rejected the call early (or answered before the body finished) leaves
// trailers readable. Only a carrier we already tore down is known dead.
func (c *clientCarrier) sendErr(err error) error {
	if err == nil {
		return nil
	}
	c.mu.Lock()
	aborted := c.aborted
	respErr := c.respErr
	haveResp := c.resp != nil
	c.mu.Unlock()
	if aborted {
		return transport.WrapSendError(err, false)
	}
	if respErr != nil && !haveResp {
		return transport.WrapSendError(err, false)
	}
	return transport.WrapSendError(err, true)
}

// Read reads the response body, waiting for headers first if needed. The end of
// the body ends the exchange, so the carrier closes the body and stops being
// tracked there; net/http needs the Close to release the stream.
func (c *clientCarrier) Read(p []byte) (int, error) {
	if err := c.waitReady(); err != nil {
		return 0, err
	}
	n, err := c.resp.Body.Read(p)
	if err != nil {
		_ = c.resp.Body.Close()
		c.untrackSelf()
	}
	return n, err
}

// CloseSend ends the request body (HTTP/2 END_STREAM on the request side).
func (c *clientCarrier) CloseSend() error {
	if c.sendClosed.Swap(true) {
		return nil
	}
	return c.sendErr(c.pw.Close())
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

// ResponseTrailers drains any unread body then returns trailers.
func (c *clientCarrier) ResponseTrailers() (transport.Headers, error) {
	if err := c.waitReady(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	body := c.resp.Body
	c.mu.Unlock()
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
	if c.conn != nil {
		c.conn.untrack(c)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return headersFromHTTP(c.resp.Trailer), nil
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
	if c.conn != nil {
		c.conn.untrack(c)
	}
	return nil
}
