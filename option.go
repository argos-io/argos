package argos

import (
	"time"

	"github.com/argos-io/argos/filter"
)

// Option configures a Config during New or Config.With. The apply method is
// unexported so the set is sealed; options take effect only inside those
// constructors.
type Option interface {
	apply(*Config)
}

type optionFunc func(*Config)

func (f optionFunc) apply(c *Config) { f(c) }

// WithBinding stores a BindingFunc. The factory is not invoked by New.
func WithBinding(fn BindingFunc) Option {
	return optionFunc(func(c *Config) { c.Binding = fn })
}

// WithFilter appends a server-side Filter (outermost first when chained later).
func WithFilter(f filter.Filter) Option {
	return optionFunc(func(c *Config) {
		c.Filters = append(c.Filters, f)
	})
}

// WithOpenFilter appends a client-side OpenFilter (outermost first when chained later).
func WithOpenFilter(f filter.OpenFilter) Option {
	return optionFunc(func(c *Config) {
		c.OpenFilters = append(c.OpenFilters, f)
	})
}

// WithService stores or merges per-service overrides under fullName (IDL full name).
func WithService(fullName string, opts ...ServiceOption) Option {
	return optionFunc(func(c *Config) {
		if c.Services == nil {
			c.Services = make(map[string]ServiceConfig)
		}
		sc := c.Services[fullName]
		for _, o := range opts {
			if o != nil {
				o.applyService(&sc)
			}
		}
		c.Services[fullName] = sc
	})
}

// WithCallErrorObserver sets the per-call local transport error observer (§7.5).
// Nil clears the observer (no-op).
func WithCallErrorObserver(fn func(CallInfo, error)) Option {
	return optionFunc(func(c *Config) { c.callErrorObserver = fn })
}

// WithConnErrorObserver sets the connection-level error observer (§7.5).
// Nil clears the observer (no-op).
//
// Reported errors must not make Transport.Serve return (§3.1-22); that
// contract is enforced by server, not by this Option.
func WithConnErrorObserver(fn func(ConnInfo, error)) Option {
	return optionFunc(func(c *Config) { c.connErrorObserver = fn })
}

// WithMaxFrameSize sets the single on-wire frame/body limit.
func WithMaxFrameSize(n int64) Option {
	return optionFunc(func(c *Config) { c.MaxFrameSize = n })
}

// WithMaxMessageSize sets the uncompressed Codec input/output byte limit.
func WithMaxMessageSize(n int64) Option {
	return optionFunc(func(c *Config) { c.MaxMessageSize = n })
}

// WithMaxMetadataSize sets the decoded metadata byte limit (keys + values).
func WithMaxMetadataSize(n int64) Option {
	return optionFunc(func(c *Config) { c.MaxMetadataSize = n })
}

// WithMaxInboundMetadataSize bounds the metadata a peer may send us. It is
// independent of WithMaxMetadataSize, which only constrains our own outbound
// metadata. 0 is invalid: omit the option to keep the default.
func WithMaxInboundMetadataSize(n int64) Option {
	return optionFunc(func(c *Config) { c.MaxInboundMetadataSize = n })
}

// WithHTTPReadHeaderTimeout bounds how long an HTTP-based peer may take to send
// a request header block or upgrade. It is the only bound that applies before
// onConn runs.
func WithHTTPReadHeaderTimeout(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.HTTPReadHeaderTimeout = d })
}

// WithHTTPIdleTimeout bounds how long a keep-alive HTTP connection may sit idle
// between requests or streams.
func WithHTTPIdleTimeout(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.HTTPIdleTimeout = d })
}

// WithMaxHeaderBytes sets the HTTP header-block limit (http1/http2).
func WithMaxHeaderBytes(n int64) Option {
	return optionFunc(func(c *Config) { c.MaxHeaderBytes = n })
}

// WithReadAheadMessages sets complete DATA messages allowed ahead per receive direction.
func WithReadAheadMessages(n int) Option {
	return optionFunc(func(c *Config) { c.ReadAheadMessages = n })
}

// WithMaxConcurrentCalls sets the in-flight call limit per Client/Server.
func WithMaxConcurrentCalls(n int) Option {
	return optionFunc(func(c *Config) { c.MaxConcurrentCalls = n })
}

// WithMaxBufferedBytes sets the Client/Server buffer-pool ceiling.
func WithMaxBufferedBytes(n int64) Option {
	return optionFunc(func(c *Config) { c.MaxBufferedBytes = n })
}

// WithOpenTimeout sets the server open/initial-headers parse timeout.
func WithOpenTimeout(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.OpenTimeout = d })
}

// WithHandshakeTimeout sets TLS/WS and New*Session handshake timeout.
func WithHandshakeTimeout(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.HandshakeTimeout = d })
}

// WithMaxDrainBytes sets the server residual-frame drain limit.
func WithMaxDrainBytes(n int64) Option {
	return optionFunc(func(c *Config) { c.MaxDrainBytes = n })
}

// WithConnReadBufferSize sets the per-connection cross-call read buffer limit.
func WithConnReadBufferSize(n int64) Option {
	return optionFunc(func(c *Config) { c.ConnReadBufferSize = n })
}

// WithMaxSessionsPerEndpoint sets the client per-endpoint session limit.
func WithMaxSessionsPerEndpoint(n int) Option {
	return optionFunc(func(c *Config) { c.MaxSessionsPerEndpoint = n })
}

// WithMaxIdleSessions sets idle sessions retained per endpoint.
// Zero means keep no idle sessions.
func WithMaxIdleSessions(n int) Option {
	return optionFunc(func(c *Config) { c.MaxIdleSessions = n })
}

// WithSessionIdleTimeout sets idle session lifetime.
// Zero disables the limit.
func WithSessionIdleTimeout(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.SessionIdleTimeout = d })
}

// WithMaxSessionLifetime sets session lifetime from creation.
// Zero disables the limit.
func WithMaxSessionLifetime(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.MaxSessionLifetime = d })
}

// WithMaxInboundConns sets server in-use connections per Binding.
func WithMaxInboundConns(n int) Option {
	return optionFunc(func(c *Config) { c.MaxInboundConns = n })
}

// WithMaxInboundConnIdle sets server idle time between calls.
// Must be positive; there is no disable value.
func WithMaxInboundConnIdle(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.MaxInboundConnIdle = d })
}

// WithMaxInboundConnAge sets server inbound connection age.
// Must be positive; there is no disable value.
func WithMaxInboundConnAge(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.MaxInboundConnAge = d })
}

// WithListenAddress sets the server bind address for Transport.Serve
// (host:port). Client-side Configs ignore it. Empty clears the override.
func WithListenAddress(addr string) Option {
	return optionFunc(func(c *Config) { c.ListenAddress = addr })
}
