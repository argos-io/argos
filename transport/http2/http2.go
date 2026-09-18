// Package http2 implements a multiplexed Transport over HTTP/2 (h2c).
//
// Client Dial returns a StreamConn endpoint handle; OpenStream starts one
// request and returns a writable Carrier before response headers arrive.
// Server Serve delivers each inbound request as a one-shot CarrierConn.
//
// HTTP/2 comes from net/http itself: the client http.Transport and the server
// http.Server select it through their Protocols field (TLS+ALPN for TLS
// listeners, prior-knowledge h2c for cleartext ones). This package does not
// import golang.org/x/net/http2.
//
// This package moves bytes and opaque headers only. It does not import
// framing, descriptor, codec, or any gRPC packages.
package http2

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

	_ transport.ByteStreamCarrier     = (*clientCarrier)(nil)
	_ transport.SendCloser            = (*clientCarrier)(nil)
	_ transport.ResponseHeaderReader  = (*clientCarrier)(nil)
	_ transport.ResponseTrailerReader = (*clientCarrier)(nil)
	_ transport.ByteStreamCarrier     = (*serverCarrier)(nil)
	_ transport.RequestHeaderReader   = (*serverCarrier)(nil)
	_ transport.ResponseWriter        = (*serverCarrier)(nil)
)

// Option configures a Transport. WithServerTLS / WithClientTLS select TLS+ALPN
// (h2) versus cleartext h2c.
type Option interface {
	apply(*options)
}

type options struct {
	serverTLS *tls.Config
	clientTLS *tls.Config
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithServerTLS enables TLS+ALPN("h2") on Serve. When nil (default), Serve uses
// cleartext h2c. The config is cloned; NextProtos is ensured to include "h2".
func WithServerTLS(cfg *tls.Config) Option {
	return optionFunc(func(o *options) { o.serverTLS = cfg })
}

// WithClientTLS enables TLS+ALPN("h2") on Dial. When nil (default), Dial uses
// h2c. The config is cloned; NextProtos is ensured to include "h2".
func WithClientTLS(cfg *tls.Config) Option {
	return optionFunc(func(o *options) { o.clientTLS = cfg })
}

// Transport is an HTTP/2 Transport. The shared http.Client (and its connection
// pool) is owned by Transport; StreamConn.Close must not shut it down.
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

// New returns an HTTP/2 Transport.
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
	tr := &http.Transport{Protocols: clientProtocols()}
	if t.opts.clientTLS != nil {
		// net/http clones this per connection and derives ServerName from the
		// dialed authority when the config leaves it empty, so SNI works when
		// callers only set RootCAs.
		tr.TLSClientConfig = ensureH2ALPN(t.opts.clientTLS.Clone())
	}
	return &http.Client{Transport: tr}
}

// clientProtocols is the client protocol set: HTTP/2 over TLS+ALPN for
// https:// targets, and HTTP/2 with prior knowledge (h2c) for http:// targets.
// HTTP/1 is deliberately absent — this transport speaks HTTP/2 only, as
// http2.Transport{AllowHTTP: true} did.
func clientProtocols() *http.Protocols {
	p := &http.Protocols{}
	p.SetHTTP2(true)
	p.SetUnencryptedHTTP2(true)
	return p
}

// serverProtocols is the server protocol set. A cleartext listener accepts
// HTTP/2 with prior knowledge (h2c) alongside HTTP/1.1, which is what the
// deprecated h2c handler did for traffic that is not an h2c exchange; a TLS
// listener negotiates HTTP/2 through ALPN instead.
func serverProtocols(serverTLS bool) *http.Protocols {
	p := &http.Protocols{}
	p.SetHTTP1(true)
	if serverTLS {
		p.SetHTTP2(true)
	} else {
		p.SetUnencryptedHTTP2(true)
	}
	return p
}

// ensureH2ALPN clones cfg semantics: NextProtos must advertise "h2" for ALPN.
func ensureH2ALPN(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	if !strSliceContains(cfg.NextProtos, "h2") {
		cfg.NextProtos = append([]string{"h2"}, cfg.NextProtos...)
	}
	if cfg.MinVersion == 0 {
		cfg.MinVersion = tls.VersionTLS12
	}
	return cfg
}

func strSliceContains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
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

// Serve listens with h2c (or TLS when WithServerTLS is set) and passes each
// inbound request to onConn as a CarrierConn.
func (t *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), opts ...transport.ServerOption) error {
	settings := transport.ApplyServerOptions(opts...)
	if settings.ListenAddress == "" {
		return errors.New("http2: listen address required (use transport.WithListenAddress)")
	}

	ln, err := net.Listen("tcp", settings.ListenAddress)
	if err != nil {
		return fmt.Errorf("http2: listen %s: %w", settings.ListenAddress, err)
	}

	var serverTLS *tls.Config
	if t.opts.serverTLS != nil {
		serverTLS = ensureH2ALPN(t.opts.serverTLS.Clone())
		ln = tls.NewListener(ln, serverTLS)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("http2: transport closed")
	}
	if t.serving {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("http2: already serving")
	}
	t.listener = ln
	t.serving = true
	t.shutdown = false
	t.serveDone = make(chan struct{})
	serveDone := t.serveDone

	// Registered before any early return below: Shutdown waits on serveDone, so
	// a path that skipped this close would stall that wait (forever under a
	// non-cancelable context).
	defer close(serveDone)

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.handleRequest(ctx, w, r, onConn)
		}),
		ReadHeaderTimeout: settings.HTTPReadHeaderTimeout,
		IdleTimeout:       settings.HTTPIdleTimeout,
		Protocols:         serverProtocols(serverTLS != nil),
	}
	if serverTLS != nil {
		// ALPN "h2" is already in NextProtos, which is also how net/http is
		// told to serve HTTP/2 over the TLS listener below.
		srv.TLSConfig = serverTLS
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
	return fmt.Errorf("http2: serve: %w", err)
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
	// Deferred so a panicking onConn counts as finished too.
	defer c.markHandlerDone()
	onConn(ctx, c)
}

// Dial returns a StreamConn endpoint handle. It does not perform network I/O;
// the shared client pool connects on OpenStream.
func (t *Transport) Dial(ctx context.Context, spec transport.DialSpec, _ ...transport.ClientOption) (transport.Conn, error) {
	_ = ctx
	if spec.Endpoint == "" {
		return nil, errors.New("http2: empty DialSpec.Endpoint")
	}
	t.mu.Lock()
	closed := t.closed
	client := t.client
	t.mu.Unlock()
	if closed {
		return nil, errors.New("http2: transport closed")
	}
	if client == nil {
		return nil, errors.New("http2: no client")
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
		return nil, errors.New("http2: transport closed")
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
		// Releases the pool's idle HTTP/2 connections too.
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

var (
	errAborted       = errors.New("http2: aborted")
	errNoRequestBody = errors.New("http2: request has no body")
)
