package argos

import (
	"time"

	"github.com/argos-io/argos/filter"
)

// ClientOption configures one Client. The apply method is unexported so the
// set is sealed: options take effect only inside ClientConfig.
type ClientOption interface {
	applyClient(*Config)
}

// ServerOption configures one Server or one of its bindings.
type ServerOption interface {
	applyServer(*Config)
}

// Option is an option that means the same thing on both sides, so it can go
// to either constructor. A side-specific option implements only its own
// interface, which is what makes passing WithListenAddress to client.New (or
// WithServiceName to server.New) a compile error rather than a silently
// ignored line.
type Option interface {
	ClientOption
	ServerOption
}

type option func(*Config)

func (f option) applyClient(c *Config) { f(c) }
func (f option) applyServer(c *Config) { f(c) }

type clientOption func(*Config)

func (f clientOption) applyClient(c *Config) { f(c) }

type serverOption func(*Config)

func (f serverOption) applyServer(c *Config) { f(c) }

// configBase is how ClientConfig / ServerConfig recognise WithConfig before
// applying anything else, so the base is the same wherever it is listed.
type configBase interface {
	baseConfig() *Config
}

type configOption struct{ cfg *Config }

func (o configOption) applyClient(*Config) {}
func (o configOption) applyServer(*Config) {}
func (o configOption) baseConfig() *Config { return o.cfg }

// WithConfig names the Config this Client or Server starts from instead of the
// process default. Every other option layers on top of it, wherever WithConfig
// appears in the list; naming more than one base keeps the last non-nil one.
// The Config is copied, not retained: writing to it afterwards does not reach
// what was built from it, and the service a previous Client selected from it
// is not inherited. A nil Config is ignored.
func WithConfig(cfg *Config) Option { return configOption{cfg: cfg} }

// WithServiceName names the service a Client opens calls for (IDL full name).
// It selects the Services entry that supplies protocol and target.
// Generated stubs pass their own service name first, so an explicit
// WithServiceName from the caller overrides it.
func WithServiceName(fullName string) ClientOption {
	return clientOption(func(c *Config) { c.serviceName = fullName })
}

// WithTarget sets the address of the service this Client opens calls for
// (e.g. "ip://127.0.0.1:7001"). It wins over the Services entry for the
// selected service.
func WithTarget(target string) ClientOption {
	return clientOption(func(c *Config) { c.targetOverride = target })
}

// WithTransport overrides the transport factory for this Client's selected
// service. It wins over the Services entry.
func WithTransport(fn TransportFunc) ClientOption {
	return clientOption(func(c *Config) {
		c.overrideTransport = fn
		c.hasTransportOverride = true
	})
}

// WithFraming overrides the framing factory for this Client's selected service.
func WithFraming(fn FramingFunc) ClientOption {
	return clientOption(func(c *Config) {
		c.overrideFraming = fn
		c.hasFramingOverride = true
	})
}

// WithCodec overrides the codec factory for this Client's selected service.
func WithCodec(fn CodecFunc) ClientOption {
	return clientOption(func(c *Config) {
		c.overrideCodec = fn
		c.hasCodecOverride = true
	})
}

type joinClientOption []ClientOption

func (j joinClientOption) applyClient(c *Config) {
	for _, o := range j {
		if o != nil {
			o.applyClient(c)
		}
	}
}

// JoinClient applies several client options in order.
func JoinClient(opts ...ClientOption) ClientOption {
	return joinClientOption(opts)
}

// WithFilter appends a server-side Filter (outermost first when chained later).
func WithFilter(f filter.Filter) ServerOption {
	return serverOption(func(c *Config) {
		c.Filters = append(c.Filters, f)
	})
}

// WithOpenFilter appends a client-side OpenFilter (outermost first when chained later).
func WithOpenFilter(f filter.OpenFilter) ClientOption {
	return clientOption(func(c *Config) {
		c.OpenFilters = append(c.OpenFilters, f)
	})
}

// WithService stores or merges per-service settings under fullName (IDL full
// name). Clients pick one entry with WithServiceName; server.Run uses the same
// map for listen surfaces of registered services.
func WithService(fullName string, opts ...ServiceOption) Option {
	return option(func(c *Config) {
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

// WithCallErrorObserver sets the per-call local transport error observer
// (§7.5). A nil fn installs no observer, clearing one the base Config carried.
func WithCallErrorObserver(fn func(CallInfo, error)) Option {
	return option(func(c *Config) { c.CallErrorObserver = fn })
}

// WithConnErrorObserver sets the connection-level error observer (§7.5).
// A nil fn installs no observer, clearing one the base Config carried.
//
// Reported errors must not make Transport.Serve return (§3.1-22); that
// contract is enforced by server, not by this Option.
func WithConnErrorObserver(fn func(ConnInfo, error)) Option {
	return option(func(c *Config) { c.ConnErrorObserver = fn })
}

// WithMaxFrameSize sets the single on-wire frame/body limit.
func WithMaxFrameSize(n int64) Option {
	return option(func(c *Config) { c.MaxFrameSize = n })
}

// WithMaxMessageSize sets the uncompressed Codec input/output byte limit.
func WithMaxMessageSize(n int64) Option {
	return option(func(c *Config) { c.MaxMessageSize = n })
}

// WithMaxMetadataSize sets the decoded metadata byte limit (keys + values).
func WithMaxMetadataSize(n int64) Option {
	return option(func(c *Config) { c.MaxMetadataSize = n })
}

// WithMaxInboundMetadataSize bounds the metadata a peer may send us. It is
// independent of WithMaxMetadataSize, which only constrains our own outbound
// metadata. 0 keeps the default.
func WithMaxInboundMetadataSize(n int64) Option {
	return option(func(c *Config) { c.MaxInboundMetadataSize = n })
}

// WithHTTPReadHeaderTimeout bounds how long an HTTP-based peer may take to send
// a request header block or upgrade. It is the only bound that applies before
// onConn runs.
func WithHTTPReadHeaderTimeout(d time.Duration) ServerOption {
	return serverOption(func(c *Config) { c.HTTPReadHeaderTimeout = d })
}

// WithHTTPIdleTimeout bounds how long a keep-alive HTTP connection may sit idle
// between requests or streams.
func WithHTTPIdleTimeout(d time.Duration) ServerOption {
	return serverOption(func(c *Config) { c.HTTPIdleTimeout = d })
}

// WithMaxHeaderBytes sets the HTTP header-block limit (http1/http2).
func WithMaxHeaderBytes(n int64) Option {
	return option(func(c *Config) { c.MaxHeaderBytes = n })
}

// WithReadAheadMessages sets complete DATA messages allowed ahead per receive direction.
func WithReadAheadMessages(n int) Option {
	return option(func(c *Config) { c.ReadAheadMessages = n })
}

// WithMaxConcurrentCalls sets the in-flight call limit per Client/Server.
func WithMaxConcurrentCalls(n int) Option {
	return option(func(c *Config) { c.MaxConcurrentCalls = n })
}

// WithMaxBufferedBytes sets the Client/Server buffer-pool ceiling.
func WithMaxBufferedBytes(n int64) Option {
	return option(func(c *Config) { c.MaxBufferedBytes = n })
}

// WithOpenTimeout sets the server open/initial-headers parse timeout.
func WithOpenTimeout(d time.Duration) Option {
	return option(func(c *Config) { c.OpenTimeout = d })
}

// WithHandshakeTimeout sets TLS/WS and New*Session handshake timeout.
func WithHandshakeTimeout(d time.Duration) Option {
	return option(func(c *Config) { c.HandshakeTimeout = d })
}

// WithMaxDrainBytes sets the server residual-frame drain limit.
func WithMaxDrainBytes(n int64) Option {
	return option(func(c *Config) { c.MaxDrainBytes = n })
}

// WithConnReadBufferSize sets the per-connection cross-call read buffer limit.
func WithConnReadBufferSize(n int64) Option {
	return option(func(c *Config) { c.ConnReadBufferSize = n })
}

// WithMaxSessionsPerEndpoint sets the client per-endpoint session limit.
func WithMaxSessionsPerEndpoint(n int) ClientOption {
	return clientOption(func(c *Config) { c.MaxSessionsPerEndpoint = n })
}

// WithMaxIdleSessions sets idle sessions retained per endpoint.
// argos.Disabled keeps no idle sessions.
func WithMaxIdleSessions(n int) ClientOption {
	return clientOption(func(c *Config) { c.MaxIdleSessions = n })
}

// WithSessionIdleTimeout sets idle session lifetime.
// argos.Disabled turns the limit off.
func WithSessionIdleTimeout(d time.Duration) ClientOption {
	return clientOption(func(c *Config) { c.SessionIdleTimeout = d })
}

// WithMaxSessionLifetime sets session lifetime from creation.
// argos.Disabled turns the limit off.
func WithMaxSessionLifetime(d time.Duration) ClientOption {
	return clientOption(func(c *Config) { c.MaxSessionLifetime = d })
}

// WithMaxInboundConns sets server in-use connections per endpoint.
func WithMaxInboundConns(n int) ServerOption {
	return serverOption(func(c *Config) { c.MaxInboundConns = n })
}

// WithMaxInboundConnIdle sets server idle time between calls.
// Must be positive; there is no off state.
func WithMaxInboundConnIdle(d time.Duration) ServerOption {
	return serverOption(func(c *Config) { c.MaxInboundConnIdle = d })
}

// WithMaxInboundConnAge sets server inbound connection age.
// Must be positive; there is no off state.
func WithMaxInboundConnAge(d time.Duration) ServerOption {
	return serverOption(func(c *Config) { c.MaxInboundConnAge = d })
}

// WithListenAddress sets the server bind address for Transport.Serve
// (host:port). Empty clears the override.
func WithListenAddress(addr string) ServerOption {
	return serverOption(func(c *Config) { c.ListenAddress = addr })
}
