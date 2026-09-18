package fake

import (
	"context"
	"errors"
	"fmt"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/transport"
)

// Compile-time interface checks.
var (
	_ transport.Transport  = (*Transport)(nil)
	_ transport.ClientConn = clientConn{}
	_ transport.ServerConn = (*serverConn)(nil)
)

// releasingPool is the OpenCall surface of internal/sessionpool.Pool. It is
// defined here so fake does not import sessionpool (sessionpool tests import
// fake).
type releasingPool interface {
	OpenCallReleasing(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error)
	Close() error
}

// Axis implements transport.Transport for the in-process fake protocol: length-
// prefixed frames, optional handshake hooks, and reuse models backed by the
// same Framing type tests already use. Attach a session pool with AttachPool
// (typically sessionpool.New(axis.DialClientConn(), cfg)) before OpenCall.
type Transport struct {
	f    *Framing
	pool releasingPool

	dial func(context.Context, string) (transport.Conn, error)
	tr   transport.Pipe
	spec session.SessionSpec
}

// Option configures New.
type Option func(*Transport)

// WithFraming replaces the default Hygiene Framing (tests that share one Framing).
func WithFraming(f *Framing) Option {
	return func(a *Transport) {
		if f != nil {
			a.f = f
		}
	}
}

// WithDial sets the function that opens transport.Conn values for OpenCall.
// It is required before DialClientConn succeeds.
func WithDial(fn func(context.Context, string) (transport.Conn, error)) Option {
	return func(a *Transport) {
		a.dial = fn
	}
}

// WithTransport sets the transport used by Serve (listener side).
func WithTransport(tr transport.Pipe) Option {
	return func(a *Transport) { a.tr = tr }
}

// WithSessionSpec passes codec limits into NewClientSession / NewServerSession.
func WithSessionSpec(spec session.SessionSpec) Option {
	return func(a *Transport) { a.spec = spec }
}

// New returns a fake protocol axis with Hygiene-enabled Framing. Use
// WithDial, AttachPool, and WithTransport (for Serve) before OpenCall / Serve.
//
// The spec is seeded from session.DefaultOptions so a fake axis reports the same
// baseline the real axes do and assembly-time validation has nothing to report;
// WithSessionSpec overrides it.
func New(model session.ReuseModel, opts ...Option) *Transport {
	a := &Transport{
		f:    NewFraming(model),
		spec: session.SessionSpec{Options: session.DefaultOptions()},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	return a
}

// Framing returns the underlying Framing so tests can set Handshake and hooks.
func (a *Transport) Framing() *Framing { return a.f }

// AttachPool binds the client-side connection pool that carries OpenCall.
func (a *Transport) AttachPool(p releasingPool) { a.pool = p }

// DialClientConn is the sessionpool.DialFunc that dials transport.Conn values
// and wraps them as transport.ClientConn after NewClientSession.
func (a *Transport) DialClientConn() func(context.Context, string) (transport.ClientConn, error) {
	return a.dialClientConn
}

// MaxCallsPerConn reports the pool MaxCallsPerConn for this axis reuse model.
func MaxCallsPerConn(model session.ReuseModel) int {
	if model == session.Concurrent {
		return -1
	}
	return 1
}

// CallConcurrency implements transport.Transport.
func (a *Transport) CallConcurrency() transport.Concurrency {
	switch a.f.Model {
	case session.Concurrent:
		return transport.Concurrent
	case session.Sequential:
		return transport.Sequential
	default:
		return transport.OneCallPerConn
	}
}

// OpenCall implements transport.Transport.
func (a *Transport) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	if a.pool == nil {
		return nil, errors.New("fake: Axis pool not attached (use AttachPool)")
	}
	return a.pool.OpenCallReleasing(ctx, endpoint, m, spec)
}

// Serve implements transport.Transport. Each accepted Conn is wrapped as ServerConn;
// the composition layer calls Handshake before AcceptCall.
func (a *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.ServerConn), opts ...transport.ServerOption) error {
	if a.tr == nil {
		return errors.New("fake: Axis Serve requires WithTransport")
	}
	return a.tr.Serve(ctx, func(connCtx context.Context, c transport.Conn) {
		onConn(connCtx, &serverConn{f: a.f, conn: c, spec: a.spec})
	}, opts...)
}

// CodecName implements transport.Transport. A fake axis names a codec only
// when a test sets one on the spec.
func (a *Transport) CodecName() string { return a.spec.CodecName }

// Limits reports the fake axis' session limits (tests and diagnostics).
func (a *Transport) Limits() transport.Limits { return transportbind.ToLimits(a.spec.Options) }

// PoolLimits reports the client-side pool limits. A fake axis may carry a test
// pool rather than a real one, in which case it reports the zero value ("does
// not pool") instead of inventing limits.
func (a *Transport) PoolLimits() transport.PoolLimits {
	if p, ok := a.pool.(*sessionpool.Pool); ok {
		return transportbind.ToPoolLimits(p.Options())
	}
	return transport.PoolLimits{}
}

// Close closes the attached pool and listener pipe when configured. It belongs
// to whoever constructed the axis, not to the composition layer.
func (a *Transport) Close() error {
	if a.pool != nil {
		if err := a.pool.Close(); err != nil {
			return err
		}
	}
	if a.tr != nil {
		return a.tr.Close()
	}
	return nil
}

// Shutdown drains the listener when a transport was configured.
func (a *Transport) Shutdown(ctx context.Context) error {
	if a.tr != nil {
		return a.tr.Shutdown(ctx)
	}
	return nil
}

func (a *Transport) dialClientConn(ctx context.Context, endpoint string) (transport.ClientConn, error) {
	if a.dial == nil {
		return nil, errors.New("fake: Axis dial not configured (use WithDial)")
	}
	c, err := a.dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	sess, err := a.f.NewClientSession(ctx, c, a.spec)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	return clientConn{sess: sess}, nil
}

// clientConn presents session.ClientSession as transport.ClientConn for the pool.
type clientConn struct{ sess session.ClientSession }

func (c clientConn) OpenCall(ctx context.Context, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
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

func (c clientConn) Reusable() bool { return c.sess.Reusable() }

func (c clientConn) Close() error { return c.sess.Close() }

// ClientSessionFromConn unwraps a ClientConn produced by this package's Axis
// or DialClientConn path.
func ClientSessionFromConn(conn transport.ClientConn) (*ClientSession, bool) {
	c, ok := conn.(clientConn)
	if !ok {
		return nil, false
	}
	fs, ok := c.sess.(*ClientSession)
	return fs, ok
}

type serverConn struct {
	f    *Framing
	conn transport.Conn
	spec session.SessionSpec
	sess session.ServerSession
}

func (c *serverConn) Handshake(ctx context.Context) error {
	sess, err := c.f.NewServerSession(ctx, c.conn, c.spec)
	if err != nil {
		return err
	}
	c.sess = sess
	return nil
}

func (c *serverConn) AcceptCall(ctx context.Context, spec transport.CallSpec) (transport.ServerCall, error) {
	if c.sess == nil {
		return nil, fmt.Errorf("fake: AcceptCall before Handshake")
	}
	call, err := c.sess.AcceptCall(ctx, session.CallSpec{Metadata: spec.Metadata})
	var out transport.ServerCall
	if call != nil {
		out = call
	}
	if err != nil && errors.Is(err, session.ErrCallRejected) {
		return out, fmt.Errorf("%w: %w", transport.ErrCallRejected, err)
	}
	return out, err
}

func (c *serverConn) Close() error {
	if c.sess != nil {
		return c.sess.Close()
	}
	return c.conn.Close()
}
