package synth

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

const (
	defaultOpenTimeout   = 10 * time.Second
	defaultMaxDrainBytes = 1 << 20
	defaultMaxFrameSize  = 4 << 20
	defaultMaxMessage    = 4 << 20
)

// Framing implements framing.Framing for the synthetic protocol.
type Framing struct {
	greeting      string
	openTimeout   time.Duration
	maxDrainBytes int64
}

// Option configures New.
type Option func(*Framing)

// WithGreeting overrides the server greeting string (default DefaultGreeting).
func WithGreeting(g string) Option {
	return func(f *Framing) {
		if g != "" {
			f.greeting = g
		}
	}
}

// WithOpenTimeout sets AcceptCall open-parse timeout from the first byte.
func WithOpenTimeout(d time.Duration) Option {
	return func(f *Framing) {
		if d > 0 {
			f.openTimeout = d
		}
	}
}

// New returns a Sequential synth Framing.
func New(opts ...Option) *Framing {
	f := &Framing{
		greeting:      DefaultGreeting,
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

// Reuse implements framing.Framing — always Sequential.
func (f *Framing) Reuse() framing.ReuseModel { return framing.Sequential }

// NewClientSession reads the server greeting then writes ACK (handshake).
func (f *Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	car, err := assertByteCarrier(c)
	if err != nil {
		return nil, err
	}
	closeOnCancel := func() { _ = c.Close() }

	var greeting string
	if err := readAll(ctx, car, closeOnCancel, func(r io.Reader) error {
		g, e := readGreeting(r)
		greeting = g
		return e
	}); err != nil {
		return nil, fmt.Errorf("synth: read greeting: %w", err)
	}
	if err := writeAll(ctx, car, closeOnCancel, writeAck); err != nil {
		return nil, fmt.Errorf("synth: write ack: %w", err)
	}

	st := &ConnState{}
	st.setGreeting(greeting)
	s := newSession(f, c, car, spec.Config, true, st)
	return &clientSession{session: s}, nil
}

// NewServerSession writes the greeting then reads ACK under HandshakeTimeout.
func (f *Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	car, err := assertByteCarrier(c)
	if err != nil {
		return nil, err
	}
	greeting := f.greeting
	if greeting == "" {
		greeting = DefaultGreeting
	}

	// Prefer a pre-placed ConnState on the connection/handshake ctx (ContextWith).
	// Do not consult the Sequential handoff here — that bridge is for handlers.
	st, _ := ctx.Value(ctxKey{}).(*ConnState)
	if st == nil {
		st = &ConnState{}
	}
	st.setGreeting(greeting)

	closeOnCancel := func() { _ = c.Close() }
	if err := writeAll(ctx, car, closeOnCancel, func(w io.Writer) error {
		return writeGreeting(w, greeting)
	}); err != nil {
		return nil, fmt.Errorf("synth: write greeting: %w", err)
	}
	if err := readAll(ctx, car, closeOnCancel, readAck); err != nil {
		return nil, fmt.Errorf("synth: read ack: %w", err)
	}

	s := newSession(f, c, car, spec.Config, false, st)
	s.handoffGoid = goroutineID()
	installHandoff(s.handoffGoid, st)
	return &serverSession{session: s}, nil
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

// Compile-time checks.
var (
	_ framing.Framing       = (*Framing)(nil)
	_ framing.ClientSession = (*clientSession)(nil)
	_ framing.ServerSession = (*serverSession)(nil)
	_ framing.Call          = (*call)(nil)
	_ framing.ServerCall    = (*serverCall)(nil)
)
