package synth

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

const (
	defaultOpenTimeout   = 10 * time.Second
	defaultMaxDrainBytes = 1 << 20
	defaultMaxFrameSize  = 4 << 20
	defaultMaxMessage    = 4 << 20
)

// Axis implements transport.Transport for the synthetic protocol over TCP: bytes,
// framing, greeting handshake, sequential reuse and the connection pool are
// all inside it.
type Transport struct {
	greeting      string
	openTimeout   time.Duration
	maxDrainBytes int64
	maxFrame      int64
	maxMessage    int64

	pool sessionpool.Options

	tr   transport.Pipe
	conn *sessionpool.Pool

	dials atomic.Int64
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

// WithGreeting overrides the server greeting string (default DefaultGreeting).
func WithGreeting(g string) Option {
	return func(a *Transport) {
		if g != "" {
			a.greeting = g
		}
	}
}

// WithOpenTimeout sets AcceptCall open-parse timeout from the first byte.
func WithOpenTimeout(d time.Duration) Option {
	return func(a *Transport) {
		if d > 0 {
			a.openTimeout = d
		}
	}
}

// WithLimits sets the session limits this axis enforces for its whole life.
// This is the only place they are set: argos.Options does not carry them, so an
// axis built without this option enforces the built-in defaults.
//
// The synthetic protocol has no metadata, so the metadata fields of
// transport.Limits are ignored here.
func WithLimits(l transport.Limits) Option {
	return func(a *Transport) {
		if l.OpenTimeout > 0 {
			a.openTimeout = l.OpenTimeout
		}
		if l.MaxFrameSize > 0 {
			a.maxFrame = l.MaxFrameSize
		}
		if l.MaxMessageSize > 0 {
			a.maxMessage = l.MaxMessageSize
		}
		if l.MaxDrainBytes > 0 {
			a.maxDrainBytes = l.MaxDrainBytes
		}
	}
}

// WithPool sets client-side connection limits on the axis.
func WithPool(maxPerEndpoint, maxIdle int, idleTimeout, maxLifetime time.Duration) Option {
	return func(a *Transport) {
		a.pool.MaxSessionsPerEndpoint = maxPerEndpoint
		a.pool.MaxIdleSessions = maxIdle
		a.pool.SessionIdleTimeout = idleTimeout
		a.pool.MaxSessionLifetime = maxLifetime
	}
}

// WithHandshakeTimeout bounds one dial plus greeting handshake.
func WithHandshakeTimeout(d time.Duration) Option {
	return func(a *Transport) {
		if d > 0 {
			a.pool.HandshakeTimeout = d
		}
	}
}

// New returns a Sequential synth Axis.
func New(opts ...Option) *Transport {
	a := &Transport{
		greeting:      DefaultGreeting,
		openTimeout:   defaultOpenTimeout,
		maxDrainBytes: defaultMaxDrainBytes,
		maxFrame:      defaultMaxFrameSize,
		maxMessage:    defaultMaxMessage,
		tr:            tcp.New(),
		// Limits live on the axis, so their defaults do too, from the shared
		// baseline.
		pool: sessionpool.DefaultOptions(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	a.pool.MaxCallsPerConn = 1
	a.conn = sessionpool.New(a.dial, a.pool)
	return a
}

// CallConcurrency implements transport.Transport.
func (a *Transport) CallConcurrency() transport.Concurrency { return transport.Sequential }

// OpenCall implements transport.Transport.
func (a *Transport) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	return a.conn.OpenCallReleasing(ctx, endpoint, m, spec)
}

// Serve implements transport.Transport.
func (a *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.ServerConn), opts ...transport.ServerOption) error {
	return a.tr.Serve(ctx, func(connCtx context.Context, c transport.Conn) {
		onConn(connCtx, &serverConn{axis: a, conn: c})
	}, opts...)
}

func (a *Transport) dial(ctx context.Context, endpoint string) (transport.ClientConn, error) {
	c, err := a.tr.Dial(ctx, transport.DialSpec{Endpoint: endpoint})
	if err != nil {
		return nil, err
	}
	a.dials.Add(1)
	car, err := assertByteCarrier(c)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	s := newSession(a, c, car, true, &ConnState{})
	if err := s.clientHandshake(ctx); err != nil {
		_ = s.closeSession()
		return nil, err
	}
	return &clientConn{session: s}, nil
}

// Dials returns how many TCP connections this axis has opened (tests).
func (a *Transport) Dials() int64 { return a.dials.Load() }

// Addr returns the listen address after Serve has bound, or nil.
func (a *Transport) Addr() net.Addr {
	if h, ok := a.tr.(interface{ Addr() net.Addr }); ok {
		return h.Addr()
	}
	return nil
}

// Shutdown drains the listener and in-flight connections.
func (a *Transport) Shutdown(ctx context.Context) error { return a.tr.Shutdown(ctx) }

// CodecName implements transport.Transport. The synthetic protocol carries no
// codec field on the wire, so there is nothing to advertise.
func (a *Transport) CodecName() string { return "" }

// Limits reports the session limits this axis enforces, which are fixed when
// it is constructed. The synthetic protocol has no metadata, so that dimension
// reports zero — it does not bound it — rather than inventing a number it never
// checks.
func (a *Transport) Limits() transport.Limits {
	return transport.Limits{
		OpenTimeout:    a.openTimeout,
		MaxFrameSize:   a.maxFrame,
		MaxMessageSize: a.maxMessage,
		MaxDrainBytes:  a.maxDrainBytes,
	}
}

// PoolLimits reports the client-side pool limits this axis enforces.
func (a *Transport) PoolLimits() transport.PoolLimits {
	if a.conn != nil {
		return transportbind.ToPoolLimits(a.conn.Options())
	}
	return transportbind.ToPoolLimits(a.pool)
}

// Close releases the listener, pooled connections and the transport. It belongs
// to whoever constructed this axis: the composition layer does not own the
// instance and never calls it.
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

func assertByteCarrier(c transport.Conn) (transport.ByteStreamCarrier, error) {
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("synth: Conn %T is not transport.CarrierConn", c)
	}
	car, ok := cc.Carrier().(transport.ByteStreamCarrier)
	if !ok {
		return nil, fmt.Errorf("synth: Carrier %T is not ByteStreamCarrier", cc.Carrier())
	}
	return car, nil
}

type rawCodec struct{}

func (rawCodec) CodecName() string { return "raw" }

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("synth: raw codec: unsupported type %T", v)
	}
}

func (rawCodec) Unmarshal(b []byte, v any) error {
	switch dst := v.(type) {
	case *[]byte:
		*dst = append([]byte(nil), b...)
		return nil
	case *string:
		*dst = string(b)
		return nil
	default:
		return fmt.Errorf("synth: raw codec: unsupported type %T", v)
	}
}

var (
	_ codec.Codec = rawCodec{}
	_ codec.Named = rawCodec{}
)

func init() {
	transport.Register("synth", func() (transport.Transport, error) { return New(), nil })
	codec.Register("raw", func() (codec.Codec, error) { return rawCodec{}, nil })
}
