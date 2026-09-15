package fake

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/transport"
)

// Compile-time interface checks.
var (
	_ transport.Conn              = (*ByteConn)(nil)
	_ transport.CarrierConn       = (*ByteConn)(nil)
	_ transport.ByteStreamCarrier = (*ByteConn)(nil)
	_ transport.SendCloser        = (*ByteConn)(nil)

	_ transport.Conn           = (*MessageConn)(nil)
	_ transport.CarrierConn    = (*MessageConn)(nil)
	_ transport.MessageCarrier = (*MessageConn)(nil)

	_ transport.Conn       = (*HTTPClientConn)(nil)
	_ transport.StreamConn = (*HTTPClientConn)(nil)

	_ transport.Conn              = (*HTTPServerConn)(nil)
	_ transport.CarrierConn       = (*HTTPServerConn)(nil)
	_ transport.ByteStreamCarrier = (*httpCarrier)(nil)
	_ transport.RequestHeaderReader = (*httpCarrier)(nil)
	_ transport.ResponseHeaderReader = (*httpCarrier)(nil)
	_ transport.ResponseWriter      = (*httpCarrier)(nil)
	_ transport.SendCloser          = (*httpCarrier)(nil)
)

// ---------------------------------------------------------------------------
// Byte-stream CarrierConn (tcp-like)
// ---------------------------------------------------------------------------

// ByteConn is an in-process byte-stream CarrierConn.
// Close is tracked so tests can assert Call.Close did not close the Conn.
// When DetectReentry is true, ConcurrentHits reports overlapping Reads.
type ByteConn struct {
	nc net.Conn

	DetectReentry bool
	reentry       ReentryDetector

	closed  atomic.Bool
	closes  atomic.Int64
	aborted atomic.Bool
}

// BytePipe returns a connected client/server ByteConn pair (net.Pipe).
func BytePipe() (client, server *ByteConn) {
	a, b := net.Pipe()
	return NewByteConn(a), NewByteConn(b)
}

// NewByteConn wraps an existing net.Conn as a ByteStream CarrierConn.
// Prefer this over BytePipe when tests need a buffered transport (e.g. TCP).
func NewByteConn(nc net.Conn) *ByteConn {
	c := &ByteConn{nc: nc}
	c.reentry.R = nc
	return c
}

// Carrier returns the Conn itself as ByteStreamCarrier.
func (c *ByteConn) Carrier() transport.Carrier { return c }

// Close implements transport.Conn. Idempotent; increments CloseCount.
func (c *ByteConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.closes.Add(1)
	return c.nc.Close()
}

// Closed reports whether Close has been called.
func (c *ByteConn) Closed() bool { return c.closed.Load() }

// CloseCount is the number of times Close ran (at most 1 after idempotent guard).
func (c *ByteConn) CloseCount() int64 { return c.closes.Load() }

// Abort implements transport.Carrier. Poisons the connection (deadline in the past).
func (c *ByteConn) Abort() error {
	c.aborted.Store(true)
	return c.nc.SetDeadline(time.Now())
}

// Aborted reports whether Abort was called.
func (c *ByteConn) Aborted() bool { return c.aborted.Load() }

// ConcurrentHits returns overlapping Read counts when DetectReentry is set.
func (c *ByteConn) ConcurrentHits() int64 { return c.reentry.ConcurrentHits() }

// Read implements io.Reader.
func (c *ByteConn) Read(p []byte) (int, error) {
	if c.DetectReentry {
		return c.reentry.Read(p)
	}
	return c.nc.Read(p)
}

// Write implements io.Writer.
func (c *ByteConn) Write(p []byte) (int, error) { return c.nc.Write(p) }

// CloseSend half-closes the write side when the underlying conn supports it.
func (c *ByteConn) CloseSend() error {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := c.nc.(closeWriter); ok {
		return cw.CloseWrite()
	}
	// net.Pipe has no half-close; treat as no-op success for fake Framing.
	return nil
}

// SetReadDeadline exposes the underlying deadline for Call.Close wakeups.
func (c *ByteConn) SetReadDeadline(t time.Time) error {
	return c.nc.SetReadDeadline(t)
}

// SetDeadline sets both read and write deadlines.
func (c *ByteConn) SetDeadline(t time.Time) error { return c.nc.SetDeadline(t) }

// ---------------------------------------------------------------------------
// Message CarrierConn (ws-like)
// ---------------------------------------------------------------------------

// MessageConn is an in-process message CarrierConn.
type MessageConn struct {
	in  chan []byte
	out chan []byte

	mu      sync.Mutex
	closed  bool
	closes  int64
	aborted bool
}

// MessagePipe returns a crossed MessageConn pair.
func MessagePipe() (client, server *MessageConn) {
	ab := make(chan []byte, 16)
	ba := make(chan []byte, 16)
	client = &MessageConn{in: ba, out: ab}
	server = &MessageConn{in: ab, out: ba}
	return client, server
}

// Carrier returns the Conn itself as MessageCarrier.
func (c *MessageConn) Carrier() transport.Carrier { return c }

// Close implements transport.Conn.
func (c *MessageConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.closes++
	close(c.out)
	return nil
}

// Closed reports whether Close has been called.
func (c *MessageConn) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// CloseCount returns how many times Close ran.
func (c *MessageConn) CloseCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// Abort implements transport.Carrier.
func (c *MessageConn) Abort() error {
	c.mu.Lock()
	c.aborted = true
	c.mu.Unlock()
	return c.Close()
}

// RecvMessage implements transport.MessageCarrier.
func (c *MessageConn) RecvMessage() ([]byte, error) {
	msg, ok := <-c.in
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

// SendMessage implements transport.MessageCarrier.
func (c *MessageConn) SendMessage(p []byte) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	cp := append([]byte(nil), p...)
	select {
	case c.out <- cp:
		return nil
	default:
		// Unbuffered fallback path if peer closed.
		c.mu.Lock()
		closed = c.closed
		c.mu.Unlock()
		if closed {
			return io.ErrClosedPipe
		}
		c.out <- cp
		return nil
	}
}

// ---------------------------------------------------------------------------
// HTTP-like StreamConn (client) + per-request CarrierConn (server)
// ---------------------------------------------------------------------------

// HTTPClientConn is a StreamConn endpoint handle. OpenStream creates a
// request/response body pair delivered to the paired HTTPListener.
type HTTPClientConn struct {
	listener *HTTPListener

	mu     sync.Mutex
	closed bool
	closes int64
}

// HTTPListener accepts server-side per-request CarrierConns.
type HTTPListener struct {
	ch     chan *HTTPServerConn
	closed atomic.Bool
}

// HTTPServerConn is one inbound HTTP-like request (CarrierConn).
type HTTPServerConn struct {
	carrier *httpCarrier

	closed  atomic.Bool
	closes  atomic.Int64
	aborted atomic.Bool
}

type httpCarrier struct {
	role string // "client" or "server"

	pr *io.PipeReader
	pw *io.PipeWriter

	preface transport.RequestPreface

	mu       sync.Mutex
	hdrDone  chan struct{}
	status   int
	headers  transport.Headers
	hdrErr   error
	aborted  atomic.Bool
	peer     *httpCarrier // opposite direction for header signaling
}

// HTTPLoopback returns a client StreamConn and a listener for server Conns.
func HTTPLoopback() (client *HTTPClientConn, lis *HTTPListener) {
	lis = &HTTPListener{ch: make(chan *HTTPServerConn, 8)}
	client = &HTTPClientConn{listener: lis}
	return client, lis
}

// Close implements transport.Conn.
func (c *HTTPClientConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.closes++
	c.listener.close()
	return nil
}

// Closed reports whether Close has been called.
func (c *HTTPClientConn) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// CloseCount returns how many times Close ran.
func (c *HTTPClientConn) CloseCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// OpenStream implements transport.StreamConn.
func (c *HTTPClientConn) OpenStream(ctx context.Context, p transport.RequestPreface) (transport.Carrier, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, errors.New("fake: HTTP client closed")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c2sR, c2sW := io.Pipe()
	s2cR, s2cW := io.Pipe()

	clientCar := &httpCarrier{
		role:    "client",
		pr:      s2cR,
		pw:      c2sW,
		preface: p,
		hdrDone: make(chan struct{}),
		status:  200,
	}
	serverCar := &httpCarrier{
		role:    "server",
		pr:      c2sR,
		pw:      s2cW,
		preface: p,
		hdrDone: make(chan struct{}),
		status:  200,
	}
	clientCar.peer = serverCar
	serverCar.peer = clientCar

	srv := &HTTPServerConn{carrier: serverCar}
	if err := c.listener.offer(ctx, srv); err != nil {
		_ = c2sW.Close()
		_ = s2cW.Close()
		return nil, err
	}
	return clientCar, nil
}

func (l *HTTPListener) offer(ctx context.Context, srv *HTTPServerConn) error {
	if l.closed.Load() {
		return errors.New("fake: HTTP listener closed")
	}
	select {
	case l.ch <- srv:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *HTTPListener) close() {
	if l.closed.Swap(true) {
		return
	}
	close(l.ch)
}

// Accept waits for the next server-side request Conn.
func (l *HTTPListener) Accept(ctx context.Context) (*HTTPServerConn, error) {
	select {
	case srv, ok := <-l.ch:
		if !ok {
			return nil, io.EOF
		}
		return srv, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Carrier implements transport.CarrierConn.
func (c *HTTPServerConn) Carrier() transport.Carrier { return c.carrier }

// Close implements transport.Conn.
func (c *HTTPServerConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.closes.Add(1)
	_ = c.carrier.Abort()
	return nil
}

// Closed reports whether Close has been called.
func (c *HTTPServerConn) Closed() bool { return c.closed.Load() }

// CloseCount returns how many times Close ran.
func (c *HTTPServerConn) CloseCount() int64 { return c.closes.Load() }

func (h *httpCarrier) Abort() error {
	h.aborted.Store(true)
	_ = h.pr.CloseWithError(errors.New("fake: aborted"))
	_ = h.pw.CloseWithError(errors.New("fake: aborted"))
	return nil
}

func (h *httpCarrier) Read(p []byte) (int, error)  { return h.pr.Read(p) }
func (h *httpCarrier) Write(p []byte) (int, error) { return h.pw.Write(p) }

func (h *httpCarrier) CloseSend() error { return h.pw.Close() }

func (h *httpCarrier) RequestTarget() string           { return h.preface.RequestTarget }
func (h *httpCarrier) RequestHeaders() transport.Headers { return h.preface.Headers }

func (h *httpCarrier) ResponseStatus() (int, error) {
	h.waitHeaders()
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status, h.hdrErr
}

func (h *httpCarrier) ResponseHeaders() (transport.Headers, error) {
	h.waitHeaders()
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.headers, h.hdrErr
}

func (h *httpCarrier) waitHeaders() {
	if h.role != "client" {
		return
	}
	<-h.hdrDone
}

func (h *httpCarrier) WriteHeaders(status int, headers transport.Headers) error {
	if h.peer == nil {
		return errors.New("fake: no peer")
	}
	h.peer.mu.Lock()
	h.peer.status = status
	h.peer.headers = append(transport.Headers(nil), headers...)
	h.peer.mu.Unlock()
	select {
	case <-h.peer.hdrDone:
	default:
		close(h.peer.hdrDone)
	}
	return nil
}

func (h *httpCarrier) Finish(status int, initial, trailers transport.Headers) error {
	_ = trailers
	if err := h.WriteHeaders(status, initial); err != nil {
		return err
	}
	return h.pw.Close()
}
