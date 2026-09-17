// Package http1 implements a StreamConn Transport over HTTP/1.1.
//
// Client Dial returns a StreamConn endpoint handle; OpenStream starts one
// request and returns a writable Carrier before response headers arrive.
// Server Serve delivers each inbound request as a one-shot CarrierConn whose
// Carrier implements UnaryResponseWriter: status, headers, and body are
// committed once via WriteResponse (never via an early WriteHeader(200)).
//
// This package moves bytes and opaque headers only. It does not import
// framing, descriptor, codec, or any gRPC packages. Cleartext is the default;
// WithServerTLS / WithClientTLS enable TLS (HTTP/1.1 only — HTTP/2 upgrade
// is disabled on the client, matching pre-TLS behaviour).
package http1

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/argos-io/argos/transport"
)

// Compile-time interface checks.
var (
	_ transport.Transport   = (*Transport)(nil)
	_ transport.Conn        = (*streamConn)(nil)
	_ transport.StreamConn  = (*streamConn)(nil)
	_ transport.Conn        = (*serverConn)(nil)
	_ transport.CarrierConn = (*serverConn)(nil)

	_ transport.ByteStreamCarrier    = (*clientCarrier)(nil)
	_ transport.SendCloser           = (*clientCarrier)(nil)
	_ transport.ResponseHeaderReader = (*clientCarrier)(nil)
	_ transport.ByteStreamCarrier    = (*serverCarrier)(nil)
	_ transport.RequestHeaderReader  = (*serverCarrier)(nil)
	_ transport.UnaryResponseWriter  = (*serverCarrier)(nil)
)

// Option configures a Transport. WithServerTLS / WithClientTLS select TLS
// versus cleartext HTTP/1.1.
type Option interface {
	apply(*options)
}

type options struct {
	serverTLS *tls.Config
	clientTLS *tls.Config
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithServerTLS wraps the Serve listener with TLS. When nil (default), Serve
// uses cleartext HTTP/1.1. The config is cloned; MinVersion defaults to TLS 1.2.
func WithServerTLS(cfg *tls.Config) Option {
	return optionFunc(func(o *options) { o.serverTLS = cfg })
}

// WithClientTLS sets TLS for client OpenStream dials. When nil (default), Dial
// uses cleartext. The config is cloned; MinVersion defaults to TLS 1.2.
func WithClientTLS(cfg *tls.Config) Option {
	return optionFunc(func(o *options) { o.clientTLS = cfg })
}

func cloneTLS(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	c := cfg.Clone()
	if c.MinVersion == 0 {
		c.MinVersion = tls.VersionTLS12
	}
	return c
}

// Transport is an HTTP/1.1 Transport. The shared http.Client (and its
// connection pool) is owned by Transport; StreamConn.Close must not shut it
// down.
type Transport struct {
	opts options

	mu sync.Mutex

	listener net.Listener
	server   *http.Server
	client   *http.Client

	streamConns map[*streamConn]struct{}
	serverConns map[*serverConn]struct{}

	serving  bool
	shutdown bool
	closed   bool

	onConnWG  sync.WaitGroup
	serveDone chan struct{}
}

// New returns an HTTP/1.1 Transport.
func New(opts ...Option) transport.Transport {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	t := &Transport{
		opts:        o,
		streamConns: make(map[*streamConn]struct{}),
		serverConns: make(map[*serverConn]struct{}),
	}
	t.client = t.newHTTPClient()
	return t
}

func (t *Transport) newHTTPClient() *http.Client {
	tr := &http.Transport{
		ForceAttemptHTTP2: false,
		// Non-nil empty TLSNextProto disables automatic HTTP/2 (net/http docs).
		TLSNextProto: map[string]func(authority string, c *tls.Conn) http.RoundTripper{},
		Proxy:        http.ProxyFromEnvironment,
		DialContext:  (&net.Dialer{}).DialContext,
	}
	if t.opts.clientTLS != nil {
		tr.TLSClientConfig = cloneTLS(t.opts.clientTLS)
	}
	return &http.Client{Transport: tr}
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

// Serve listens with cleartext or TLS HTTP/1.1 and passes each inbound request
// to onConn as a CarrierConn.
func (t *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), opts ...transport.ServerOption) error {
	settings := transport.ApplyServerOptions(opts...)
	if settings.ListenAddress == "" {
		return errors.New("http1: listen address required (use transport.WithListenAddress)")
	}

	ln, err := net.Listen("tcp", settings.ListenAddress)
	if err != nil {
		return fmt.Errorf("http1: listen %s: %w", settings.ListenAddress, err)
	}
	if t.opts.serverTLS != nil {
		ln = tls.NewListener(ln, cloneTLS(t.opts.serverTLS))
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("http1: transport closed")
	}
	if t.serving {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("http1: already serving")
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
			t.handleRequest(ctx, w, r, onConn)
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
	return fmt.Errorf("http1: serve: %w", err)
}

func (t *Transport) handleRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, onConn func(context.Context, transport.Conn)) {
	c := newServerConn(w, r)

	t.mu.Lock()
	if t.shutdown || t.closed {
		t.mu.Unlock()
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	t.serverConns[c] = struct{}{}
	t.onConnWG.Add(1)
	t.mu.Unlock()

	defer t.onConnWG.Done()
	defer t.untrackServer(c)
	defer c.markHandlerDone()
	onConn(ctx, c)
}

// Dial returns a StreamConn endpoint handle. It does not perform network I/O;
// the shared client pool connects on OpenStream.
func (t *Transport) Dial(ctx context.Context, spec transport.DialSpec, _ ...transport.ClientOption) (transport.Conn, error) {
	_ = ctx
	if spec.Endpoint == "" {
		return nil, errors.New("http1: empty DialSpec.Endpoint")
	}
	t.mu.Lock()
	closed := t.closed
	client := t.client
	t.mu.Unlock()
	if closed {
		return nil, errors.New("http1: transport closed")
	}
	if client == nil {
		return nil, errors.New("http1: no client")
	}

	base := dialBaseURL(spec.Endpoint, t.opts.clientTLS != nil)
	c := newStreamConn(client, base)
	// An endpoint handle is tracked only while it is open: Session recycles
	// handles on idle/lifetime timeouts, and the transport must not retain
	// closed ones until Close.
	c.detach = func() { t.untrackStreamConn(c) }

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("http1: transport closed")
	}
	t.streamConns[c] = struct{}{}
	t.mu.Unlock()
	return c, nil
}

func dialBaseURL(endpoint string, preferHTTPS bool) string {
	switch {
	case strings.HasPrefix(endpoint, "http://"),
		strings.HasPrefix(endpoint, "https://"):
		return strings.TrimRight(endpoint, "/")
	default:
		scheme := "http"
		if preferHTTPS {
			scheme = "https"
		}
		return scheme + "://" + endpoint
	}
}

// Shutdown stops accepting new requests and waits for in-flight onConn
// callbacks. If ctx expires, remaining server connections are aborted.
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
		t.closeAllServerConns()
		// A callback that ignores Conn.Close must not hold Shutdown open past
		// its deadline.
		select {
		case <-done:
		case <-ctx.Done():
		}
		return ctx.Err()
	}
}

// Close closes the listener, aborts tracked connections, and releases the
// shared client pool. Idempotent.
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
	client := t.client
	t.mu.Unlock()

	if srv != nil {
		_ = srv.Close()
	} else if ln != nil {
		_ = ln.Close()
	}
	t.closeAllServerConns()
	t.closeAllStreamConns()
	if client != nil {
		client.CloseIdleConnections()
	}
	return nil
}

func (t *Transport) untrackServer(c *serverConn) {
	t.mu.Lock()
	delete(t.serverConns, c)
	t.mu.Unlock()
}

func (t *Transport) untrackStreamConn(c *streamConn) {
	t.mu.Lock()
	delete(t.streamConns, c)
	t.mu.Unlock()
}

func (t *Transport) closeAllServerConns() {
	t.mu.Lock()
	conns := make([]*serverConn, 0, len(t.serverConns))
	for c := range t.serverConns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (t *Transport) closeAllStreamConns() {
	t.mu.Lock()
	conns := make([]*streamConn, 0, len(t.streamConns))
	for c := range t.streamConns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func headersFromHTTP(h http.Header) transport.Headers {
	if len(h) == 0 {
		return nil
	}
	out := make(transport.Headers, 0, len(h))
	for k, vs := range h {
		for _, v := range vs {
			out = append(out, transport.Header{Name: k, Value: v})
		}
	}
	return out
}

func applyHeaders(dst http.Header, src transport.Headers) {
	for _, h := range src {
		dst.Add(h.Name, h.Value)
	}
}

var errAborted = errors.New("http1: aborted")
