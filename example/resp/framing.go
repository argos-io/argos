package resp

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/framing"
)

const (
	defaultService     = "redis.Redis"
	defaultOpenTimeout = 10 * time.Second
)

// Framing implements framing.Framing for a minimal RESP2 subset over a
// CarrierConn / ByteStreamCarrier. Reuse is always Sequential.
type Framing struct {
	service  string
	password string

	openTimeout time.Duration

	// hellos counts client-side HELLO commands sent from NewClientSession.
	hellos atomic.Int64
}

// Option configures New.
type Option func(*Framing)

// WithService sets the IDL service name used when AcceptCall maps a Redis
// command to descriptor.Method (default "redis.Redis").
func WithService(name string) Option {
	return func(f *Framing) {
		if name != "" {
			f.service = name
		}
	}
}

// WithPassword enables AUTH after HELLO on the client and requires AUTH on
// the server during NewServerSession.
func WithPassword(pw string) Option {
	return func(f *Framing) { f.password = pw }
}

// WithOpenTimeout sets the AcceptCall open-parse budget counted from the first
// inbound byte of a call. Non-positive values are ignored.
func WithOpenTimeout(d time.Duration) Option {
	return func(f *Framing) {
		if d > 0 {
			f.openTimeout = d
		}
	}
}

// New returns a RESP2 Framing. Reuse() is framing.Sequential.
func New(opts ...Option) *Framing {
	f := &Framing{
		service:     defaultService,
		openTimeout: defaultOpenTimeout,
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

// Service returns the configured service name.
func (f *Framing) Service() string { return f.service }

// ClientHellos returns how many HELLO commands this Framing instance has sent
// from NewClientSession. Used by tests to assert handshake-once-per-session.
func (f *Framing) ClientHellos() int64 { return f.hellos.Load() }

func applyOpenTimeout(f *Framing, cfg framing.Config) time.Duration {
	if cfg.OpenTimeout > 0 {
		return cfg.OpenTimeout
	}
	if f.openTimeout > 0 {
		return f.openTimeout
	}
	return defaultOpenTimeout
}

func methodFullName(service, cmd string) string {
	return fmt.Sprintf("%s.%s", service, cmd)
}
