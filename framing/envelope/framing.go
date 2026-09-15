package envelope

import (
	"context"
	"fmt"
	"time"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

const (
	defaultOpenTimeout   = 10 * time.Second
	defaultMaxDrainBytes = 1 << 20 // 1 MiB
)

// Framing implements framing.Framing for the envelope wire protocol.
// Reuse is always Sequential (one in-flight call per connection).
type Framing struct {
	openTimeout   time.Duration
	maxDrainBytes int64
}

// Option configures New.
type Option func(*Framing)

// WithOpenTimeout sets the AcceptCall open-parse timeout counted from the
// first inbound byte of a call. Non-positive values are ignored.
func WithOpenTimeout(d time.Duration) Option {
	return func(f *Framing) {
		if d > 0 {
			f.openTimeout = d
		}
	}
}

// WithMaxDrainBytes sets the residual-frame drain budget between AcceptCall
// iterations. Non-positive values are ignored.
func WithMaxDrainBytes(n int64) Option {
	return func(f *Framing) {
		if n > 0 {
			f.maxDrainBytes = n
		}
	}
}

// New returns an envelope Framing. Reuse() is always framing.Sequential.
func New(opts ...Option) framing.Framing {
	f := &Framing{
		openTimeout:   defaultOpenTimeout,
		maxDrainBytes: defaultMaxDrainBytes,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}
	return f
}

// Reuse implements framing.Framing.
func (f *Framing) Reuse() framing.ReuseModel { return framing.Sequential }

// NewClientSession implements framing.Framing. No handshake I/O.
func (f *Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	_ = ctx
	car, kind, err := assertCarrier(c)
	if err != nil {
		return nil, err
	}
	s := newSession(f, c, car, kind, spec.Config, true)
	return &clientSession{session: s}, nil
}

// NewServerSession implements framing.Framing. No handshake I/O.
func (f *Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	_ = ctx
	car, kind, err := assertCarrier(c)
	if err != nil {
		return nil, err
	}
	s := newSession(f, c, car, kind, spec.Config, false)
	return &serverSession{session: s}, nil
}

func assertCarrier(c transport.Conn) (transport.Carrier, carrierKind, error) {
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, 0, fmt.Errorf("envelope: Conn %T is not transport.CarrierConn", c)
	}
	car := cc.Carrier()
	if bs, ok := car.(transport.ByteStreamCarrier); ok {
		return bs, kindByteStream, nil
	}
	if mc, ok := car.(transport.MessageCarrier); ok {
		return mc, kindMessage, nil
	}
	return nil, 0, fmt.Errorf("envelope: Carrier %T is neither ByteStreamCarrier nor MessageCarrier", car)
}

// Compile-time checks.
var (
	_ framing.Framing       = (*Framing)(nil)
	_ framing.ClientSession = (*clientSession)(nil)
	_ framing.ServerSession = (*serverSession)(nil)
	_ framing.Call          = (*call)(nil)
	_ framing.ServerCall    = (*serverCall)(nil)
)
