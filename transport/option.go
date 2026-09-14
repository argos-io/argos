package transport

import (
	"net"

	"github.com/argos-io/argos/internal/limits"
)

const (
	// DefaultMaxMessageSize is the default payload limit used by transports.
	DefaultMaxMessageSize = limits.DefaultMessageSize
	// DefaultMaxMetadataSize is the default total metadata limit per call.
	DefaultMaxMetadataSize int64 = 64 << 10
)

// ServerOptions holds listen-side settings for ListenAndServe.
type ServerOptions struct {
	ListenAddress  string
	Listener       net.Listener
	Network        string
	MaxMessageSize int64
	MaxHeaderBytes int
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

// WithMaxMessageSize limits one decoded request or response payload on the
// server side. Non-positive values use DefaultMaxMessageSize.
func WithMaxMessageSize(size int64) ServerOption {
	return func(o *ServerOptions) {
		o.MaxMessageSize = size
	}
}

// WithMaxHeaderBytes limits HTTP request headers where the transport supports
// that setting. Non-positive values use the transport default.
func WithMaxHeaderBytes(size int) ServerOption {
	return func(o *ServerOptions) {
		o.MaxHeaderBytes = size
	}
}

// ApplyServerOptions merges server options.
func ApplyServerOptions(opts []ServerOption) ServerOptions {
	var o ServerOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// ClientOptions holds dial settings for Open.
type ClientOptions struct {
	DialAddress    string
	MaxMessageSize int64
}

// ClientOption configures client dial/open.
type ClientOption func(*ClientOptions)

// WithDialAddress sets the remote dial address for Open.
func WithDialAddress(addr string) ClientOption {
	return func(o *ClientOptions) {
		o.DialAddress = addr
	}
}

// WithClientMaxMessageSize limits one decoded response or request payload on
// the client side. Non-positive values use DefaultMaxMessageSize.
func WithClientMaxMessageSize(size int64) ClientOption {
	return func(o *ClientOptions) {
		o.MaxMessageSize = size
	}
}

// ApplyClientOptions merges client options.
func ApplyClientOptions(opts []ClientOption) ClientOptions {
	var o ClientOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// ResolveMaxMessageSize returns the configured limit or the default.
func ResolveMaxMessageSize(size int64) int64 {
	if size <= 0 {
		return DefaultMaxMessageSize
	}
	return size
}

// ResolveMaxFrameSize returns a frame limit that leaves room for the wire
// envelope around a payload and metadata.
func ResolveMaxFrameSize(messageSize int64) int64 {
	messageSize = ResolveMaxMessageSize(messageSize)
	const envelopeReserve = DefaultMaxMetadataSize + 1<<10
	const maxInt64 = int64(^uint64(0) >> 1)
	if messageSize > maxInt64-envelopeReserve {
		return maxInt64
	}
	return messageSize + envelopeReserve
}
