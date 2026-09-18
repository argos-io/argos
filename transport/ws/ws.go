// Package ws implements a message-oriented Transport over WebSocket.
//
// Conn is a CarrierConn whose Carrier is itself: MessageCarrier.
// One WebSocket binary message is one complete message payload; this package
// moves raw bytes only and does not import framing, descriptor, codec, or
// status.
package ws

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"

	"github.com/argos-io/argos/transport"
)

// Compile-time interface checks.
var (
	_ transport.Pipe           = (*Transport)(nil)
	_ transport.Conn           = (*Conn)(nil)
	_ transport.CarrierConn    = (*Conn)(nil)
	_ transport.MessageCarrier = (*Conn)(nil)
)

// DefaultMaxReadBytes is the inbound message limit applied when
// WithMaxReadBytes is not given: the framework's default 4 MiB maximum frame
// plus 1 MiB of framing headroom. A larger configured MaxFrameSize needs a
// matching WithMaxReadBytes.
const DefaultMaxReadBytes int64 = 5 << 20

// Option configures a Transport; the set is sealed so it can grow without
// breaking the constructor.
type Option interface {
	apply(*options)
}

type options struct {
	maxReadBytes int64
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithMaxReadBytes bounds one inbound WebSocket message on both Serve and Dial.
// The peer of an oversized message is closed with status 1009; a negative n
// removes the limit. The zero value means DefaultMaxReadBytes.
func WithMaxReadBytes(n int64) Option {
	return optionFunc(func(o *options) { o.maxReadBytes = n })
}

// Transport is a WebSocket Transport producing CarrierConn connections.
type Transport struct {
	opts options

	mu sync.Mutex

	listener net.Listener
	server   *http.Server
	conns    map[*Conn]struct{}

	serving  bool
	shutdown bool
	closed   bool

	// onConnWG counts in-flight onConn callbacks.
	onConnWG sync.WaitGroup

	// serveDone is closed when Serve returns.
	serveDone chan struct{}
}

// New returns a WebSocket Transport.
func New(opts ...Option) transport.Pipe {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	if o.maxReadBytes == 0 {
		o.maxReadBytes = DefaultMaxReadBytes
	}
	return &Transport{
		opts:  o,
		conns: make(map[*Conn]struct{}),
	}
}

// Addr returns the listener address after Serve has bound, or nil.
func (t *Transport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listener == nil {
		return nil
	}
	return t.listener.Addr()
}

// Serve listens on the address from WithListenAddress, upgrades HTTP to
// WebSocket, and passes each connection to onConn.
func (t *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), opts ...transport.ServerOption) error {
	settings := transport.ApplyServerOptions(opts...)
	if settings.ListenAddress == "" {
		return errors.New("ws: listen address required (use transport.WithListenAddress)")
	}

	ln, err := net.Listen("tcp", settings.ListenAddress)
	if err != nil {
		return fmt.Errorf("ws: listen %s: %w", settings.ListenAddress, err)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("ws: transport closed")
	}
	if t.serving {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("ws: already serving")
	}
	t.listener = ln
	t.serving = true
	t.shutdown = false
	t.serveDone = make(chan struct{})
	serveDone := t.serveDone

	srv := &http.Server{
		ReadHeaderTimeout: settings.HTTPReadHeaderTimeout,
		IdleTimeout:       settings.HTTPIdleTimeout,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.handleUpgrade(ctx, w, r, onConn)
		}),
	}
	t.server = srv
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		t.serving = false
		if t.listener == ln {
			t.listener = nil
		}
		if t.server == srv {
			t.server = nil
		}
		close(serveDone)
		t.mu.Unlock()
		_ = srv.Close()
		_ = ln.Close()
	}()

	// Close the listener when ctx is cancelled so Serve unblocks.
	stopAccept := context.AfterFunc(ctx, func() { _ = srv.Close() })
	defer stopAccept()

	err = srv.Serve(ln)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		t.mu.Lock()
		shuttingDown := t.shutdown || t.closed
		t.mu.Unlock()
		if shuttingDown || ctx.Err() != nil {
			if ctx.Err() != nil && !shuttingDown {
				return ctx.Err()
			}
			return nil
		}
		return nil
	}
	t.mu.Lock()
	shuttingDown := t.shutdown || t.closed
	t.mu.Unlock()
	if shuttingDown || ctx.Err() != nil {
		if ctx.Err() != nil && !shuttingDown {
			return ctx.Err()
		}
		return nil
	}
	return fmt.Errorf("ws: serve: %w", err)
}

func (t *Transport) handleUpgrade(ctx context.Context, w http.ResponseWriter, r *http.Request, onConn func(context.Context, transport.Conn)) {
	wc, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	wc.SetReadLimit(t.opts.maxReadBytes)

	c := newConn(wc)

	t.mu.Lock()
	if t.shutdown || t.closed {
		t.mu.Unlock()
		_ = c.Close()
		return
	}
	t.conns[c] = struct{}{}
	t.onConnWG.Add(1)
	t.mu.Unlock()

	defer t.onConnWG.Done()
	defer t.untrack(c)
	onConn(ctx, c)
}

// Dial connects to spec.Endpoint and returns a Conn wrapping the WebSocket.
// Endpoint may be host:port, ws://…, wss://…, http://…, or https://….
func (t *Transport) Dial(ctx context.Context, spec transport.DialSpec, _ ...transport.ClientOption) (transport.Conn, error) {
	if spec.Endpoint == "" {
		return nil, errors.New("ws: empty DialSpec.Endpoint")
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, errors.New("ws: transport closed")
	}

	url := dialURL(spec.Endpoint)
	wc, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("ws: dial %s: %w", url, err)
	}
	wc.SetReadLimit(t.opts.maxReadBytes)

	c := newConn(wc)
	// A dialed connection is tracked only while it is open: Session recycles
	// connections on idle/lifetime timeouts, and the transport must not retain
	// closed ones until Close.
	c.detach = func() { t.untrack(c) }

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("ws: transport closed")
	}
	t.conns[c] = struct{}{}
	t.mu.Unlock()
	return c, nil
}

func dialURL(endpoint string) string {
	switch {
	case strings.HasPrefix(endpoint, "ws://"),
		strings.HasPrefix(endpoint, "wss://"),
		strings.HasPrefix(endpoint, "http://"),
		strings.HasPrefix(endpoint, "https://"):
		return endpoint
	default:
		return "ws://" + endpoint
	}
}

// Shutdown stops accepting new connections and waits for in-flight onConn
// callbacks. If ctx expires before they finish, remaining connections are
// closed to unblock them.
func (t *Transport) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.shutdown = true
	srv := t.server
	ln := t.listener
	serveDone := t.serveDone
	t.mu.Unlock()

	if srv != nil {
		_ = srv.Close()
	} else if ln != nil {
		_ = ln.Close()
	}

	done := make(chan struct{})
	go func() {
		t.onConnWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		if serveDone != nil {
			select {
			case <-serveDone:
			case <-ctx.Done():
			}
		}
		return nil
	case <-ctx.Done():
		t.closeAllConns()
		// A callback that ignores Conn.Close must not hold Shutdown open past
		// its deadline.
		select {
		case <-done:
		case <-ctx.Done():
		}
		return ctx.Err()
	}
}

// Close closes the listener and all tracked connections. Idempotent.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.shutdown = true
	srv := t.server
	ln := t.listener
	t.mu.Unlock()

	if srv != nil {
		_ = srv.Close()
	} else if ln != nil {
		_ = ln.Close()
	}
	t.closeAllConns()
	return nil
}

func (t *Transport) untrack(c *Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

func (t *Transport) closeAllConns() {
	t.mu.Lock()
	conns := make([]*Conn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// Conn wraps a WebSocket connection as a CarrierConn / MessageCarrier.
type Conn struct {
	ws     *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
	// detach removes this Conn from the Transport's tracking map when it
	// closes, so the map never retains a closed connection.
	detach func()
}

func newConn(wc *websocket.Conn) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{ws: wc, ctx: ctx, cancel: cancel}
}

// Carrier returns the Conn itself as the exchange carrier.
func (c *Conn) Carrier() transport.Carrier { return c }

// RecvMessage reads one WebSocket binary message.
func (c *Conn) RecvMessage() ([]byte, error) {
	typ, data, err := c.ws.Read(c.ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("ws: expected binary message, got %v", typ)
	}
	return data, nil
}

// SendMessage writes one WebSocket binary message. A failed write is a
// send-direction failure only: WebSocket delivers messages in both directions
// independently, so the peer may still send its response. ReceiveOpen stays
// true unless this Conn was closed — the conservative reading of
// transport.SendError.
func (c *Conn) SendMessage(p []byte) error {
	err := c.ws.Write(c.ctx, websocket.MessageBinary, p)
	if err != nil {
		return transport.WrapSendError(err, !c.closed.Load())
	}
	return nil
}

// Abort cancels in-flight I/O and closes the connection.
func (c *Conn) Abort() error {
	c.cancel()
	return c.Close()
}

// Close closes the underlying WebSocket. Idempotent.
func (c *Conn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.cancel()
	err := c.ws.CloseNow()
	if c.detach != nil {
		c.detach()
	}
	return err
}
