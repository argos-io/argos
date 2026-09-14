package argos

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

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
	optionErr     error

	Filters []filter.Filter

	ServerTransportOpts []transport.ServerOption
	ClientTarget        string
	ClientTransportOpts []transport.ClientOption

	resolveMu *sync.Mutex
}

// Option configures a Service or Client.
type Option func(*Config)

// NewConfig applies opts to a new Config.
func NewConfig(opts ...Option) Config {
	cfg := Config{resolveMu: &sync.Mutex{}}
	for _, opt := range opts {
		if opt == nil {
			if cfg.optionErr == nil {
				cfg.optionErr = errors.New("argos: nil option")
			}
			continue
		}
		opt(&cfg)
	}
	return cfg
}

var zeroConfigResolveMu sync.Mutex

func (c *Config) resolveMutex() *sync.Mutex {
	zeroConfigResolveMu.Lock()
	defer zeroConfigResolveMu.Unlock()
	if c.resolveMu == nil {
		c.resolveMu = &sync.Mutex{}
	}
	return c.resolveMu
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
			if isNilValue(x) {
				if c.transportErr == nil {
					c.transportErr = errors.New("argos: WithTransport: nil Transport")
				}
				return
			}
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
			if isNilValue(x) {
				if c.codecErr == nil {
					c.codecErr = errors.New("argos: WithCodec: nil Codec")
				}
				return
			}
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
	return c.ResolveCodec()
}

// ValidateCompatibility rejects combinations whose built-in wire protocol is
// tied to a particular codec. Unknown custom identities remain valid for
// opaque byte transports; restricted transports require a named codec so an
// accidental binary/text mismatch cannot silently corrupt a call.
func (c *Config) ValidateCompatibility() error {
	cd, err := c.ResolveCodec()
	if err != nil {
		return err
	}
	tr, err := c.ResolveTransport()
	if err != nil {
		return err
	}
	transportID, namedTransport := tr.(transport.Named)
	if !namedTransport {
		return nil
	}
	transportName := transportID.TransportName()
	if transportName == "" {
		return nil
	}
	codecID, namedCodec := cd.(codec.Named)
	codecName := ""
	if namedCodec {
		codecName = codecID.CodecName()
		namedCodec = codecName != ""
	}

	var required string
	switch transportName {
	case "http1", "telnet":
		required = "json"
	case "http2":
		required = "protobuf"
	default:
		return nil
	}
	if !namedCodec {
		return fmt.Errorf("argos: transport %q requires a named %q codec", transportName, required)
	}
	if codecName != required {
		return fmt.Errorf("argos: transport %q is incompatible with codec %q; requires codec %q", transportName, codecName, required)
	}
	return nil
}

// ResolveTransport returns the configured Transport, resolving a registered name on first use.
func (c *Config) ResolveTransport() (transport.Transport, error) {
	mu := c.resolveMutex()
	mu.Lock()
	defer mu.Unlock()
	if c.optionErr != nil {
		return nil, c.optionErr
	}
	if c.transportErr != nil {
		return nil, c.transportErr
	}
	if c.Transport != nil {
		if isNilValue(c.Transport) {
			return nil, errors.New("argos: configured Transport is nil")
		}
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
	if isNilValue(c.Transport) {
		c.Transport = nil
		return nil, fmt.Errorf("transport: factory %q returned nil", c.transportName)
	}
	return c.Transport, nil
}

// ResolveCodec returns the configured Codec, resolving a registered name on first use.
func (c *Config) ResolveCodec() (codec.Codec, error) {
	mu := c.resolveMutex()
	mu.Lock()
	defer mu.Unlock()
	if c.optionErr != nil {
		return nil, c.optionErr
	}
	if c.codecErr != nil {
		return nil, c.codecErr
	}
	if c.Codec != nil {
		if isNilValue(c.Codec) {
			return nil, errors.New("argos: configured Codec is nil")
		}
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
	if isNilValue(c.Codec) {
		c.Codec = nil
		return nil, fmt.Errorf("codec: factory %q returned nil", c.codecName)
	}
	return c.Codec, nil
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	value := reflect.ValueOf(v)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// WithFilter appends a Filter. Server and client share the same type.
func WithFilter(f filter.Filter) Option {
	return func(c *Config) {
		if f == nil {
			if c.optionErr == nil {
				c.optionErr = errors.New("argos: WithFilter: nil filter")
			}
			return
		}
		c.Filters = append(c.Filters, f)
	}
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

// WithMaxMessageSize limits one encoded request or response message on the
// server side. Non-positive values use the transport default.
func WithMaxMessageSize(size int64) Option {
	return WithServerTransportOption(transport.WithMaxMessageSize(size))
}

// WithMaxHeaderBytes limits HTTP request headers on transports that support
// the setting. Non-positive values use the transport default.
func WithMaxHeaderBytes(size int) Option {
	return WithServerTransportOption(transport.WithMaxHeaderBytes(size))
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

// WithClientMaxMessageSize limits one encoded request or response message on
// the client side. Non-positive values use the transport default.
func WithClientMaxMessageSize(size int64) Option {
	return WithClientTransportOption(transport.WithClientMaxMessageSize(size))
}
