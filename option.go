package argos

import (
	"time"

	"github.com/argos-io/argos/filter"
)

// ClientOption configures one Client. The apply method is unexported so the
// set is sealed: options take effect only inside ClientOptions.
type ClientOption interface {
	applyClient(*Options)
}

// ServerOption configures one Server or one of its bindings.
type ServerOption interface {
	applyServer(*Options)
}

type clientOption func(*Options)

func (f clientOption) applyClient(c *Options) { f(c) }

type serverOption func(*Options)

func (f serverOption) applyServer(c *Options) { f(c) }

// optionsBase is how ClientOptions / ServerOptions recognise With*Options before
// applying anything else, so the base is the same wherever it is listed.
type optionsBase interface {
	baseOptions() *Options
}

type clientOptionsOption struct{ cfg *Options }

func (o clientOptionsOption) applyClient(*Options)  {}
func (o clientOptionsOption) baseOptions() *Options { return o.cfg }

type serverOptionsOption struct{ cfg *Options }

func (o serverOptionsOption) applyServer(*Options)  {}
func (o serverOptionsOption) baseOptions() *Options { return o.cfg }

// WithClientOptions names the Options this Client starts from instead of the
// process default.
func WithClientOptions(cfg *Options) ClientOption { return clientOptionsOption{cfg: cfg} }

// WithServerOptions names the Options this Server starts from instead of the
// process default.
func WithServerOptions(cfg *Options) ServerOption { return serverOptionsOption{cfg: cfg} }

// WithServiceName names the service a Client opens calls for (IDL full name).
func WithServiceName(fullName string) ClientOption {
	return clientOption(func(c *Options) { c.serviceName = fullName })
}

// WithTarget sets the address of the service this Client opens calls for.
func WithTarget(target string) ClientOption {
	return clientOption(func(c *Options) { c.targetOverride = target })
}

// WithTransport overrides the registered transport name for this Client's selected service.
func WithTransport(name string) ClientOption {
	return clientOption(func(c *Options) {
		c.overrideTransport = name
		c.hasTransportOverride = true
	})
}

// WithCodec overrides the registered codec name for this Client's selected service.
func WithCodec(name string) ClientOption {
	return clientOption(func(c *Options) {
		c.overrideCodec = name
		c.hasCodecOverride = true
	})
}

// WithFilter appends a server-side Filter (outermost first when chained later).
func WithFilter(f filter.Filter) ServerOption {
	return serverOption(func(c *Options) {
		c.Filters = append(c.Filters, f)
	})
}

// WithOpenFilter appends a client-side OpenFilter (outermost first when chained later).
func WithOpenFilter(f filter.OpenFilter) ClientOption {
	return clientOption(func(c *Options) {
		c.OpenFilters = append(c.OpenFilters, f)
	})
}

// WithClientCallErrorObserver sets the client per-call local transport error observer (§7.5).
func WithClientCallErrorObserver(fn func(CallInfo, error)) ClientOption {
	return clientOption(func(c *Options) { c.CallErrorObserver = fn })
}

// WithServerCallErrorObserver sets the server per-call local transport error observer (§7.5).
func WithServerCallErrorObserver(fn func(CallInfo, error)) ServerOption {
	return serverOption(func(c *Options) { c.CallErrorObserver = fn })
}

// WithClientConnErrorObserver sets the client connection-level error observer (§7.5).
func WithClientConnErrorObserver(fn func(ConnInfo, error)) ClientOption {
	return clientOption(func(c *Options) { c.ConnErrorObserver = fn })
}

// WithServerConnErrorObserver sets the server connection-level error observer (§7.5).
func WithServerConnErrorObserver(fn func(ConnInfo, error)) ServerOption {
	return serverOption(func(c *Options) { c.ConnErrorObserver = fn })
}

// WithMaxHeaderBytes sets the client HTTP header-block limit.
func WithMaxHeaderBytes(n int64) ClientOption {
	return clientOption(func(c *Options) { c.MaxHeaderBytes = n })
}

// WithServerMaxHeaderBytes sets the server HTTP header-block limit.
func WithServerMaxHeaderBytes(n int64) ServerOption {
	return serverOption(func(c *Options) { c.MaxHeaderBytes = n })
}

// WithMaxConcurrentCalls sets the client in-flight call limit.
func WithMaxConcurrentCalls(n int) ClientOption {
	return clientOption(func(c *Options) { c.MaxConcurrentCalls = n })
}

// WithServerMaxConcurrentCalls sets the server in-flight call limit.
func WithServerMaxConcurrentCalls(n int) ServerOption {
	return serverOption(func(c *Options) { c.MaxConcurrentCalls = n })
}

// WithHandshakeTimeout sets the client TLS/WS and session handshake timeout.
func WithHandshakeTimeout(d time.Duration) ClientOption {
	return clientOption(func(c *Options) { c.HandshakeTimeout = d })
}

// WithServerHandshakeTimeout sets the server TLS/WS and session handshake timeout.
func WithServerHandshakeTimeout(d time.Duration) ServerOption {
	return serverOption(func(c *Options) { c.HandshakeTimeout = d })
}

// WithMaxInboundConns sets server in-use connections per endpoint.
func WithMaxInboundConns(n int) ServerOption {
	return serverOption(func(c *Options) { c.MaxInboundConns = n })
}

// WithMaxInboundConnIdle sets server idle time between calls.
func WithMaxInboundConnIdle(d time.Duration) ServerOption {
	return serverOption(func(c *Options) { c.MaxInboundConnIdle = d })
}

// WithMaxInboundConnAge sets server inbound connection age.
func WithMaxInboundConnAge(d time.Duration) ServerOption {
	return serverOption(func(c *Options) { c.MaxInboundConnAge = d })
}

// WithListenAddress sets the server bind address for Transport.Serve.
func WithListenAddress(addr string) ServerOption {
	return serverOption(func(c *Options) { c.ListenAddress = addr })
}
