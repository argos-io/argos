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
	// defaultMaxDatagram matches transport/udp.MaxDatagramSize (IPv4 practical max).
	defaultMaxDatagram = 65507
)

// Framing implements framing.Framing for the envelope wire protocol.
// Reuse defaults to Sequential (tcp/ws). Use WithOneCallPerConn for UDP.
type Framing struct {
	openTimeout   time.Duration
	maxDrainBytes int64
	reuse         framing.ReuseModel
	maxDatagram   int64 // runtime datagram cap; 0 → defaultMaxDatagram when one-call
	// maxInboundWire is the largest single inbound wire unit the transport will
	// deliver, when it has one. Declared by the binding, checked in CheckConfig.
	maxInboundWire int64
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

// WithOneCallPerConn sets Reuse() to OneCallPerConn (envelope × udp).
// Also enables datagram size gating against MaxDatagram (default 65507).
func WithOneCallPerConn() Option {
	return func(f *Framing) {
		f.reuse = framing.OneCallPerConn
		if f.maxDatagram <= 0 {
			f.maxDatagram = defaultMaxDatagram
		}
	}
}

// WithMaxInboundWireBytes declares the largest single inbound wire unit the
// transport will deliver (e.g. a WebSocket per-message read limit) so
// CheckConfig can reject a MaxFrameSize the carrier would never hand over.
// Non-positive values are ignored.
func WithMaxInboundWireBytes(n int64) Option {
	return func(f *Framing) {
		if n > 0 {
			f.maxInboundWire = n
		}
	}
}

// WithMaxDatagramSize sets the runtime/startup datagram payload cap used with
// DatagramCarrier and CheckDatagramLimits. Non-positive values are ignored.
func WithMaxDatagramSize(n int64) Option {
	return func(f *Framing) {
		if n > 0 {
			f.maxDatagram = n
		}
	}
}

// New returns an envelope Framing. Reuse() defaults to framing.Sequential.
func New(opts ...Option) framing.Framing {
	f := &Framing{
		openTimeout:   defaultOpenTimeout,
		maxDrainBytes: defaultMaxDrainBytes,
		reuse:         framing.Sequential,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}
	return f
}

// Reuse implements framing.Framing.
func (f *Framing) Reuse() framing.ReuseModel { return f.reuse }

// MaxDatagramSize returns the configured datagram payload cap, or 0 when
// unset (stream carriers). Used by Binding factories with CheckDatagramLimits.
func (f *Framing) MaxDatagramSize() int64 { return f.maxDatagram }

// CheckConfig validates the configured sizes against what the carrier will
// actually deliver: the datagram cap when this Framing is OneCallPerConn (or
// maxDatagram is set), and the transport's per-message read limit when one was
// declared with WithMaxInboundWireBytes.
func (f *Framing) CheckConfig(cfg framing.Config) error {
	if n := f.maxInboundWire; n > 0 {
		// One frame is the length prefix plus the body, and the transport
		// rejects the whole message above its own limit - on ws by closing the
		// connection with 1009, which fails unrelated calls too. Catch the
		// mismatch at start instead of at the first large message.
		if want := cfg.MaxFrameSize + lenPrefix; cfg.MaxFrameSize > 0 && want > n {
			return fmt.Errorf(
				"envelope: MaxFrameSize %d needs %d bytes on the wire but the transport reads at most %d per message",
				cfg.MaxFrameSize, want, n)
		}
	}
	maxD := f.maxDatagram
	if maxD <= 0 && f.reuse == framing.OneCallPerConn {
		maxD = defaultMaxDatagram
	}
	if maxD <= 0 {
		return nil
	}
	return CheckDatagramLimits(cfg.MaxFrameSize, cfg.MaxMessageSize, maxD)
}

// NewClientSession implements framing.Framing. No handshake I/O.
func (f *Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	_ = ctx
	car, kind, err := assertCarrier(c)
	if err != nil {
		return nil, err
	}
	if kind == kindDatagram {
		if err := f.checkDatagramSpec(spec.Config); err != nil {
			return nil, err
		}
	}
	s := newSession(f, c, car, kind, spec.Config, true)
	applySessionSpec(s, f, spec.Config)
	return &clientSession{session: s}, nil
}

// NewServerSession implements framing.Framing. No handshake I/O.
func (f *Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	_ = ctx
	car, kind, err := assertCarrier(c)
	if err != nil {
		return nil, err
	}
	if kind == kindDatagram {
		if err := f.checkDatagramSpec(spec.Config); err != nil {
			return nil, err
		}
	}
	s := newSession(f, c, car, kind, spec.Config, false)
	applySessionSpec(s, f, spec.Config)
	return &serverSession{session: s}, nil
}

func (f *Framing) checkDatagramSpec(cfg framing.Config) error {
	maxD := f.maxDatagram
	if maxD <= 0 {
		maxD = defaultMaxDatagram
	}
	return CheckDatagramLimits(cfg.MaxFrameSize, cfg.MaxMessageSize, maxD)
}

// applySessionSpec overlays SessionSpec.Config limits onto the Framing defaults
// when the composition layer provides non-zero values.
func applySessionSpec(s *session, f *Framing, cfg framing.Config) {
	s.openTimeout = f.openTimeout
	s.maxDrainBytes = f.maxDrainBytes
	if cfg.OpenTimeout > 0 {
		s.openTimeout = cfg.OpenTimeout
	}
	if cfg.MaxDrainBytes > 0 {
		s.maxDrainBytes = cfg.MaxDrainBytes
	}
	maxD := f.maxDatagram
	if maxD <= 0 && (f.reuse == framing.OneCallPerConn || s.kind == kindDatagram) {
		maxD = defaultMaxDatagram
	}
	s.maxDatagram = maxD
}

func assertCarrier(c transport.Conn) (transport.Carrier, carrierKind, error) {
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, 0, fmt.Errorf("envelope: Conn %T is not transport.CarrierConn", c)
	}
	car := cc.Carrier()
	// Prefer DatagramCarrier when present so UDP Conn is not mistaken for a
	// stream carrier if a type implements multiple narrow interfaces.
	if dc, ok := car.(transport.DatagramCarrier); ok {
		return dc, kindDatagram, nil
	}
	if bs, ok := car.(transport.ByteStreamCarrier); ok {
		return bs, kindByteStream, nil
	}
	if mc, ok := car.(transport.MessageCarrier); ok {
		return mc, kindMessage, nil
	}
	return nil, 0, fmt.Errorf("envelope: Carrier %T is neither ByteStreamCarrier, MessageCarrier, nor DatagramCarrier", car)
}

// Compile-time checks.
var (
	_ framing.Framing       = (*Framing)(nil)
	_ framing.ClientSession = (*clientSession)(nil)
	_ framing.ServerSession = (*serverSession)(nil)
	_ framing.Call          = (*call)(nil)
	_ framing.ServerCall    = (*serverCall)(nil)
)
