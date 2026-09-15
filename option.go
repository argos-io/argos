package argos

import "time"

// Option configures a Config during New. The apply method is unexported so
// the set is sealed; options take effect only inside New.
type Option interface {
	apply(*Config)
}

type optionFunc func(*Config)

func (f optionFunc) apply(c *Config) { f(c) }

// WithBinding stores a BindingFunc. The factory is not invoked by New.
func WithBinding(fn BindingFunc) Option {
	return optionFunc(func(c *Config) { c.Binding = fn })
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

// WithMaxIdleSessions sets idle sessions retained per endpoint (provisional ⚠️).
// Zero means keep no idle sessions.
func WithMaxIdleSessions(n int) Option {
	return optionFunc(func(c *Config) { c.MaxIdleSessions = n })
}

// WithSessionIdleTimeout sets idle session lifetime (provisional ⚠️).
// Zero disables the limit.
func WithSessionIdleTimeout(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.SessionIdleTimeout = d })
}

// WithMaxSessionLifetime sets session lifetime from creation (provisional ⚠️).
// Zero disables the limit.
func WithMaxSessionLifetime(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.MaxSessionLifetime = d })
}

// WithMaxInboundConns sets server in-use connections per Binding (provisional ⚠️).
func WithMaxInboundConns(n int) Option {
	return optionFunc(func(c *Config) { c.MaxInboundConns = n })
}

// WithMaxInboundConnIdle sets server idle time between calls (provisional ⚠️).
// Must be positive; there is no disable value.
func WithMaxInboundConnIdle(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.MaxInboundConnIdle = d })
}

// WithMaxInboundConnAge sets server inbound connection age (provisional ⚠️).
// Must be positive; there is no disable value.
func WithMaxInboundConnAge(d time.Duration) Option {
	return optionFunc(func(c *Config) { c.MaxInboundConnAge = d })
}
