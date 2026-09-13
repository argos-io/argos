package argos

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/transport"
)

// Config is one transport + codec + filter binding.
type Config struct {
	Transport transport.Transport
	Codec     codec.Codec

	transportName string
	codecName     string
	transportErr  error
	codecErr      error

	Filters []filter.Filter

	ServerTransportOpts []transport.ServerOption
	ClientTarget        string
	ClientTransportOpts []transport.ClientOption
}

// Option configures a Service or Client.
type Option func(*Config)

// NewConfig applies opts to a new Config.
func NewConfig(opts ...Option) Config {
	var cfg Config
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// WithTransport sets Transport from an instance or a registered name ("http2", ...).
// For a name, import the transport subpackage so init registers the factory.
//
// Go disallows interface types with methods in union constraints, so this accepts
// any and type-asserts at runtime. Prefer [WithTransportInstance] or
// [WithTransportNamed] for compile-time checks.
func WithTransport(v any) Option {
	switch x := v.(type) {
	case transport.Transport:
		return func(c *Config) {
			c.Transport = x
			c.transportName = ""
		}
	case string:
		return func(c *Config) {
			c.transportName = x
			c.Transport = nil
		}
	default:
		return func(c *Config) {
			if c.transportErr != nil {
				return
			}
			c.transportErr = fmt.Errorf("argos: WithTransport: want Transport or string, got %T", v)
		}
	}
}

// WithTransportInstance sets Transport from a concrete or interface value.
func WithTransportInstance[T transport.Transport](t T) Option {
	return WithTransport(t)
}

// WithTransportNamed sets Transport from a registered factory name.
func WithTransportNamed(name string) Option {
	return WithTransport(name)
}

// WithCodec sets Codec from an instance or a registered name ("protobuf", ...).
// For a name, import the codec subpackage so init registers the factory.
//
// Prefer [WithCodecInstance] or [WithCodecNamed] for compile-time checks.
func WithCodec(v any) Option {
	switch x := v.(type) {
	case codec.Codec:
		return func(c *Config) {
			c.Codec = x
			c.codecName = ""
		}
	case string:
		return func(c *Config) {
			c.codecName = x
			c.Codec = nil
		}
	default:
		return func(c *Config) {
			if c.codecErr != nil {
				return
			}
			c.codecErr = fmt.Errorf("argos: WithCodec: want Codec or string, got %T", v)
		}
	}
}

// WithCodecInstance sets Codec from a concrete or interface value.
func WithCodecInstance[T codec.Codec](c T) Option {
	return WithCodec(c)
}

// WithCodecNamed sets Codec from a registered factory name.
func WithCodecNamed(name string) Option {
	return WithCodec(name)
}

// CodecForCall returns the Codec for an active call after Run has validated configuration.
func (c *Config) CodecForCall() (codec.Codec, error) {
	if c.codecErr != nil {
		return nil, c.codecErr
	}
	if c.Codec == nil {
		return nil, fmt.Errorf("argos: missing codec")
	}
	return c.Codec, nil
}

// ResolveTransport returns the configured Transport, resolving a registered name on first use.
func (c *Config) ResolveTransport() (transport.Transport, error) {
	if c.transportErr != nil {
		return nil, c.transportErr
	}
	if c.Transport != nil {
		return c.Transport, nil
	}
	if c.transportName == "" {
		return nil, fmt.Errorf("argos: binding needs WithTransport")
	}
	newTR := transport.Get(c.transportName)
	if newTR == nil {
		return nil, fmt.Errorf("transport: unknown %q", c.transportName)
	}
	c.Transport = newTR()
	return c.Transport, nil
}

// ResolveCodec returns the configured Codec, resolving a registered name on first use.
func (c *Config) ResolveCodec() (codec.Codec, error) {
	if c.codecErr != nil {
		return nil, c.codecErr
	}
	if c.Codec != nil {
		return c.Codec, nil
	}
	if c.codecName == "" {
		return nil, fmt.Errorf("argos: missing codec")
	}
	newCodec := codec.Get(c.codecName)
	if newCodec == nil {
		return nil, fmt.Errorf("codec: unknown %q", c.codecName)
	}
	c.Codec = newCodec()
	return c.Codec, nil
}

// WithFilter appends a Filter. Server and client share the same type.
func WithFilter(f filter.Filter) Option {
	return func(c *Config) { c.Filters = append(c.Filters, f) }
}

// WithListenAddress sets the server listen address (server only).
func WithListenAddress(addr string) Option {
	return WithServerTransportOption(transport.WithListenAddress(addr))
}

// WithServerTransportOption appends server listen options.
func WithServerTransportOption(opts ...transport.ServerOption) Option {
	return func(c *Config) {
		c.ServerTransportOpts = append(c.ServerTransportOpts, opts...)
	}
}

// WithTarget sets a client dial target (scheme://service-identifier, e.g. ip://127.0.0.1:9090).
func WithTarget(target string) Option {
	return func(c *Config) { c.ClientTarget = target }
}

// WithClientTransportOption appends client dial/open options.
func WithClientTransportOption(opts ...transport.ClientOption) Option {
	return func(c *Config) {
		c.ClientTransportOpts = append(c.ClientTransportOpts, opts...)
	}
}
