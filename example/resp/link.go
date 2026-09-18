package resp

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

const (
	defaultService     = "redis.Redis"
	defaultOpenTimeout = 10 * time.Second
)

// Axis implements transport.Transport for a minimal RESP2 subset over TCP: bytes,
// framing, HELLO/AUTH handshake, sequential reuse and the connection pool are
// all inside it. One call per connection at a time.
//
// TCP is an implementation detail here, not a separate axis: the pool holds
// connections that have already said HELLO, and only this package can tell
// whether one of them is still good.
type Transport struct {
	service  string
	password string

	openTimeout time.Duration
	pool        sessionpool.Options

	tr   transport.Pipe
	conn *sessionpool.Pool

	// hellos counts client-side HELLO commands; dials counts TCP dials. Both
	// are what the reuse tests assert on, and both belong to whoever owns
	// connectivity.
	hellos atomic.Int64
	dials  atomic.Int64
}

// Compile-time interface checks.
var (
	_ transport.Transport  = (*Transport)(nil)
	_ transport.ClientConn = (*clientConn)(nil)
	_ transport.ServerConn = (*serverConn)(nil)
	_ transport.Call       = (*call)(nil)
	_ transport.ServerCall = (*serverCall)(nil)
)

// Option configures New.
type Option func(*Transport)

// WithService sets the IDL service name used when AcceptCall maps a Redis
// command to descriptor.Method (default "redis.Redis").
func WithService(name string) Option {
	return func(a *Transport) {
		if name != "" {
			a.service = name
		}
	}
}

// WithPassword enables AUTH after HELLO on the client and requires AUTH during
// the server handshake.
func WithPassword(pw string) Option {
	return func(a *Transport) { a.password = pw }
}

// WithOpenTimeout sets the AcceptCall open-parse budget counted from the first
// inbound byte of a call. Non-positive values are ignored.
func WithOpenTimeout(d time.Duration) Option {
	return func(a *Transport) {
		if d > 0 {
			a.openTimeout = d
		}
	}
}

// WithPool sets the client-side connection limits. They are axis options
// rather than Options fields because connectivity is the axis's own business:
// one axis may be shared by several Clients, and they cannot each want a
// different pool.
func WithPool(maxPerEndpoint, maxIdle int, idleTimeout, maxLifetime time.Duration) Option {
	return func(a *Transport) {
		a.pool.MaxSessionsPerEndpoint = maxPerEndpoint
		a.pool.MaxIdleSessions = maxIdle
		a.pool.SessionIdleTimeout = idleTimeout
		a.pool.MaxSessionLifetime = maxLifetime
	}
}

// WithHandshakeTimeout bounds one dial plus HELLO/AUTH.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(a *Transport) {
		if d > 0 {
			a.pool.HandshakeTimeout = d
		}
	}
}

// New returns a RESP2 axis. CallConcurrency() is transport.Sequential.
func New(opts ...Option) *Transport {
	a := &Transport{
		service:     defaultService,
		openTimeout: defaultOpenTimeout,
		tr:          tcp.New(),
		// Limits live on the axis, so their defaults do too, from the shared
		// baseline: WithPool(_, 0, ...) is the caller asking to keep no idle
		// connection, which is a different thing from saying nothing.
		pool: sessionpool.DefaultOptions(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	a.pool.MaxCallsPerConn = 1 // sequential reuse
	a.conn = sessionpool.New(a.dial, a.pool)
	return a
}

func init() {
	transport.Register("resp", func() (transport.Transport, error) { return New(), nil })
}

// CallConcurrency implements transport.Transport. RESP2 carries one call at a time.
func (a *Transport) CallConcurrency() transport.Concurrency { return transport.Sequential }

// OpenCall implements transport.Transport: it reuses a pooled connection or dials
// and handshakes a new one. The returned Call gives the connection back when it
// closes.
func (a *Transport) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	return a.conn.OpenCallReleasing(ctx, endpoint, m, spec)
}

// Serve implements transport.Transport. Each accepted connection arrives before
// handshake: the composition layer owns the handshake timeout and the handshake
// error report, so it calls ServerConn.Handshake itself.
func (a *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.ServerConn), opts ...transport.ServerOption) error {
	return a.tr.Serve(ctx, func(connCtx context.Context, c transport.Conn) {
		onConn(connCtx, &serverConn{axis: a, conn: c})
	}, opts...)
}

// dial is the pool's DialFunc: TCP plus HELLO/AUTH, so nothing half-built ever
// reaches the pool.
func (a *Transport) dial(ctx context.Context, endpoint string) (transport.ClientConn, error) {
	c, err := a.tr.Dial(ctx, transport.DialSpec{Endpoint: endpoint})
	if err != nil {
		return nil, err
	}
	a.dials.Add(1)
	car, err := assertByteStream(c)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	s := newSession(a, c, car, true)
	if err := s.clientHandshake(ctx); err != nil {
		_ = s.closeSession()
		return nil, err
	}
	return &clientConn{session: s}, nil
}

// Service returns the configured service name.
func (a *Transport) Service() string { return a.service }

// ClientHellos returns how many HELLO commands this axis has sent. Used by
// tests to assert handshake-once-per-connection.
func (a *Transport) ClientHellos() int64 { return a.hellos.Load() }

// Dials returns how many TCP connections this axis has opened. Used by tests
// to assert reuse.
func (a *Transport) Dials() int64 { return a.dials.Load() }

// Addr returns the listen address after Serve has bound, or nil.
func (a *Transport) Addr() net.Addr {
	if h, ok := a.tr.(interface{ Addr() net.Addr }); ok {
		return h.Addr()
	}
	return nil
}

// Shutdown drains the listener and in-flight connections. It exists while the
// concrete transports still have a lifecycle of their own; once Serve's ctx is
// the only lifetime, it goes away.
func (a *Transport) Shutdown(ctx context.Context) error { return a.tr.Shutdown(ctx) }

// CodecName implements transport.Transport. RESP2 carries no codec field on the
// wire, so there is nothing to advertise and nothing to compare.
func (a *Transport) CodecName() string { return "" }

// Limits reports the session limits this axis enforces. RESP2 has no metadata
// and no message cap, so those dimensions report zero — this protocol does not
// bound them — rather than inventing numbers it never checks.
func (a *Transport) Limits() transport.Limits {
	return transport.Limits{OpenTimeout: a.openTimeout}
}

// PoolLimits reports the client-side pool limits this axis enforces.
func (a *Transport) PoolLimits() transport.PoolLimits {
	if a.conn != nil {
		return transportbind.ToPoolLimits(a.conn.Options())
	}
	return transportbind.ToPoolLimits(a.pool)
}

// Close releases the listener, the pooled connections and the transport. It
// belongs to whoever constructed this axis: the composition layer does not own
// the instance and never calls it.
func (a *Transport) Close() error {
	var first error
	if a.conn != nil {
		if err := a.conn.Close(); err != nil {
			first = err
		}
	}
	if err := a.tr.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

func methodFullName(service, cmd string) string {
	return fmt.Sprintf("%s.%s", service, cmd)
}
