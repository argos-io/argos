package transport

import "net"

// ServerOptions holds listen-side settings for ListenAndServe.
type ServerOptions struct {
	ListenAddress string
	Listener      net.Listener
	Network       string
}

// ServerOption configures server listen.
type ServerOption func(*ServerOptions)

// WithListenAddress sets the listen address (host:port).
func WithListenAddress(addr string) ServerOption {
	return func(o *ServerOptions) {
		o.ListenAddress = addr
	}
}

// WithListener injects a pre-bound listener (tests).
func WithListener(ln net.Listener) ServerOption {
	return func(o *ServerOptions) {
		o.Listener = ln
	}
}

// WithListenNetwork sets the listen network (default tcp; udp transports use udp).
func WithListenNetwork(network string) ServerOption {
	return func(o *ServerOptions) {
		o.Network = network
	}
}

// ApplyServerOptions merges server options.
func ApplyServerOptions(opts []ServerOption) ServerOptions {
	var o ServerOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// ClientOptions holds dial settings for Open.
type ClientOptions struct {
	DialAddress string
}

// ClientOption configures client dial/open.
type ClientOption func(*ClientOptions)

// WithDialAddress sets the remote dial address for Open.
func WithDialAddress(addr string) ClientOption {
	return func(o *ClientOptions) {
		o.DialAddress = addr
	}
}

// ApplyClientOptions merges client options.
func ApplyClientOptions(opts []ClientOption) ClientOptions {
	var o ClientOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
