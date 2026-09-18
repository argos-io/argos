package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/http2"
)

// Axis implements transport.Transport for gRPC over HTTP/2: multiplexed streams,
// concurrent reuse on one connection, and the client pool live here.
type Transport struct {
	tr   transport.Pipe
	fr   session.Framing
	spec session.SessionSpec

	pool sessionpool.Options
	conn *sessionpool.Pool

	extraCompressors []compressor.Compressor
	sendCompressor   string
}

// Compile-time interface checks.
var (
	_ transport.Transport  = (*Transport)(nil)
	_ transport.ClientConn = (*linkClientConn)(nil)
	_ transport.ServerConn = (*linkServerConn)(nil)
)

// TransportOption configures NewTransport.
type TransportOption func(*Transport)

// WithCodecName sets the codec identity passed in SessionSpec at dial (default
// "protobuf"). The wire content subtype is derived from it — see contenttype.go
// — so this names the codec, not the subtype.
func WithCodecName(name string) TransportOption {
	return func(a *Transport) {
		if name != "" {
			a.spec.CodecName = name
		}
	}
}

// WithLimits sets the session limits this axis enforces for its whole life.
// This is the only place they are set: argos.Options does not carry them, so an
// axis built without this option enforces the built-in defaults.
func WithLimits(l transport.Limits) TransportOption {
	return func(a *Transport) { a.spec.Options = transportbind.FromLimits(l) }
}

// WithHTTP2Pipe sets the underlying byte transport (default transport/http2).
func WithHTTP2Pipe(p transport.Pipe) TransportOption {
	return func(a *Transport) {
		if p != nil {
			a.tr = p
		}
	}
}

// WithCompressors registers message compressors (identity is always enabled).
func WithCompressors(cs ...compressor.Compressor) TransportOption {
	return func(a *Transport) {
		a.extraCompressors = append(a.extraCompressors, cs...)
	}
}

// WithSendCompressor selects the outbound compression algorithm by name.
func WithSendCompressor(name string) TransportOption {
	return func(a *Transport) { a.sendCompressor = name }
}

// WithPool sets client-side connection limits for this axis instance.
func WithPool(maxPerEndpoint, maxIdle int, idleTimeout, maxLifetime time.Duration) TransportOption {
	return func(a *Transport) {
		a.pool.MaxSessionsPerEndpoint = maxPerEndpoint
		a.pool.MaxIdleSessions = maxIdle
		a.pool.SessionIdleTimeout = idleTimeout
		a.pool.MaxSessionLifetime = maxLifetime
	}
}

// WithHandshakeTimeout bounds one dial plus NewClientSession.
func WithHandshakeTimeout(d time.Duration) TransportOption {
	return func(a *Transport) {
		if d > 0 {
			a.pool.HandshakeTimeout = d
		}
	}
}

// NewTransport returns gRPC over HTTP/2 as transport.Transport.
func NewTransport(opts ...TransportOption) (*Transport, error) {
	a := &Transport{
		tr: http2.New(),
		spec: session.SessionSpec{
			CodecName: "protobuf",
			Options:   session.DefaultOptions(),
		},
		pool: sessionpool.DefaultOptions(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	fr, err := newFraming(a.extraCompressors, a.sendCompressor)
	if err != nil {
		return nil, err
	}
	a.fr = fr
	a.pool.MaxCallsPerConn = -1
	a.conn = sessionpool.New(a.dial, a.pool)
	return a, nil
}

// CallConcurrency implements transport.Transport.
func (a *Transport) CallConcurrency() transport.Concurrency { return transport.Concurrent }

// OpenCall implements transport.Transport.
func (a *Transport) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	return a.conn.OpenCallReleasing(ctx, endpoint, m, spec)
}

// Serve implements transport.Transport. Each accepted Conn is wrapped as ServerConn
// before handshake; the composition layer calls Handshake.
func (a *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.ServerConn), opts ...transport.ServerOption) error {
	return a.tr.Serve(ctx, func(connCtx context.Context, c transport.Conn) {
		onConn(connCtx, &linkServerConn{axis: a, conn: c})
	}, opts...)
}

func (a *Transport) dial(ctx context.Context, endpoint string) (transport.ClientConn, error) {
	c, err := a.tr.Dial(ctx, transport.DialSpec{Endpoint: endpoint})
	if err != nil {
		return nil, err
	}
	sess, err := a.fr.NewClientSession(ctx, c, a.spec)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	return &linkClientConn{sess: sess}, nil
}

// Addr returns the listen address after Serve has bound, or nil.
func (a *Transport) Addr() net.Addr {
	if h, ok := a.tr.(interface{ Addr() net.Addr }); ok {
		return h.Addr()
	}
	return nil
}

// Shutdown drains the listener and in-flight connections on the http2 transport.
func (a *Transport) Shutdown(ctx context.Context) error { return a.tr.Shutdown(ctx) }

// CodecName implements transport.Transport.
func (a *Transport) CodecName() string { return a.spec.CodecName }

// Limits reports the session limits this axis enforces, which are fixed when
// it is constructed. It is introspection — nothing downstream consults it,
// because the axis is the only place these numbers live. There is deliberately
// no setter: a shared axis must not be reconfigured under its other users,
// which is what last-bind-wins used to do.
func (a *Transport) Limits() transport.Limits { return transportbind.ToLimits(a.spec.Options) }

// PoolLimits reports the client-side pool limits this axis enforces.
func (a *Transport) PoolLimits() transport.PoolLimits {
	c := a.pool
	if a.conn != nil {
		c = a.conn.Options()
	}
	return transportbind.ToPoolLimits(c)
}

// Close releases the pool and the underlying transport. It belongs to whoever
// constructed this axis — the composition layer does not own the instance and
// never calls it. Idempotent.
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

type linkClientConn struct {
	sess session.ClientSession
}

func (c *linkClientConn) OpenCall(ctx context.Context, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	call, err := c.sess.OpenCall(ctx, m, session.CallSpec{Metadata: spec.Metadata})
	switch {
	case err == nil:
		return call, nil
	case errors.Is(err, session.ErrSessionBusy):
		return nil, transport.ErrConnBusy
	case errors.Is(err, session.ErrSessionSpent):
		return nil, transport.ErrConnSpent
	default:
		return nil, err
	}
}

func (c *linkClientConn) Reusable() bool { return c.sess.Reusable() }

func (c *linkClientConn) Close() error { return c.sess.Close() }

type linkServerConn struct {
	axis *Transport
	conn transport.Conn
	sess session.ServerSession
}

func (s *linkServerConn) Handshake(ctx context.Context) error {
	sess, err := s.axis.fr.NewServerSession(ctx, s.conn, s.axis.spec)
	if err != nil {
		return err
	}
	s.sess = sess
	return nil
}

func (s *linkServerConn) AcceptCall(ctx context.Context, spec transport.CallSpec) (transport.ServerCall, error) {
	if s.sess == nil {
		return nil, fmt.Errorf("grpc: AcceptCall before Handshake")
	}
	call, err := s.sess.AcceptCall(ctx, session.CallSpec{Metadata: spec.Metadata})
	if err != nil && errors.Is(err, session.ErrCallRejected) {
		// Wrap the rejection itself, not errors.Unwrap(err): the session layer
		// builds it with two %w verbs, so it carries Unwrap() []error and the
		// singular errors.Unwrap returns nil. Dropping it left the composition
		// layer unable to recover the status and every rejection surfaced as
		// InvalidArgument.
		err = fmt.Errorf("%w: %w", transport.ErrCallRejected, err)
	}
	return call, err
}

func (s *linkServerConn) Close() error {
	if s.sess == nil {
		return s.conn.Close()
	}
	return s.sess.Close()
}

func init() {
	transport.Register("grpc", func() (transport.Transport, error) { return NewTransport() })
}
