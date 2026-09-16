package argos

import "fmt"

// Well-known axis names for text configuration and binding presets.
const (
	TransportTCP   = "tcp"
	TransportWS    = "ws"
	TransportUDP   = "udp"
	TransportHTTP1 = "http1"
	TransportHTTP2 = "http2"

	FramingEnvelope  = "envelope"
	FramingGRPC      = "grpc"
	FramingWholebody = "wholebody"

	CodecProtobuf = "protobuf"
	CodecJSON     = "json"
)

// RegisterTransport records a named transport factory on c. Names are looked up
// when a ServiceConfig names an axis but does not set the factory directly
// (typical for text configuration). Registering the same name with the same
// factory again is a no-op; a different factory returns an error.
func (c *Config) RegisterTransport(name string, fn TransportFunc) error {
	if c == nil {
		return fmt.Errorf("argos: nil Config")
	}
	if name == "" {
		return fmt.Errorf("argos: empty transport name")
	}
	if fn == nil {
		return fmt.Errorf("argos: nil TransportFunc for %q", name)
	}
	if c.transportRegistry == nil {
		c.transportRegistry = make(map[string]TransportFunc)
	}
	if existing, ok := c.transportRegistry[name]; ok {
		if fmt.Sprintf("%p", existing) != fmt.Sprintf("%p", fn) {
			return fmt.Errorf("argos: transport %q already registered", name)
		}
		return nil
	}
	c.transportRegistry[name] = fn
	return nil
}

// RegisterFraming records a named framing factory on c.
func (c *Config) RegisterFraming(name string, fn FramingFunc) error {
	if c == nil {
		return fmt.Errorf("argos: nil Config")
	}
	if name == "" {
		return fmt.Errorf("argos: empty framing name")
	}
	if fn == nil {
		return fmt.Errorf("argos: nil FramingFunc for %q", name)
	}
	if c.framingRegistry == nil {
		c.framingRegistry = make(map[string]FramingFunc)
	}
	if existing, ok := c.framingRegistry[name]; ok {
		if fmt.Sprintf("%p", existing) != fmt.Sprintf("%p", fn) {
			return fmt.Errorf("argos: framing %q already registered", name)
		}
		return nil
	}
	c.framingRegistry[name] = fn
	return nil
}

// RegisterCodec records a named codec factory on c.
func (c *Config) RegisterCodec(name string, fn CodecFunc) error {
	if c == nil {
		return fmt.Errorf("argos: nil Config")
	}
	if name == "" {
		return fmt.Errorf("argos: empty codec name")
	}
	if fn == nil {
		return fmt.Errorf("argos: nil CodecFunc for %q", name)
	}
	if c.codecRegistry == nil {
		c.codecRegistry = make(map[string]CodecFunc)
	}
	if existing, ok := c.codecRegistry[name]; ok {
		if fmt.Sprintf("%p", existing) != fmt.Sprintf("%p", fn) {
			return fmt.Errorf("argos: codec %q already registered", name)
		}
		return nil
	}
	c.codecRegistry[name] = fn
	return nil
}

// LookupTransport returns the factory registered under name on c.
func (c *Config) LookupTransport(name string) (TransportFunc, error) {
	if c == nil || c.transportRegistry == nil {
		return nil, fmt.Errorf("argos: transport %q not registered", name)
	}
	fn, ok := c.transportRegistry[name]
	if !ok {
		return nil, fmt.Errorf("argos: transport %q not registered", name)
	}
	return fn, nil
}

// LookupFraming returns the factory registered under name on c.
func (c *Config) LookupFraming(name string) (FramingFunc, error) {
	if c == nil || c.framingRegistry == nil {
		return nil, fmt.Errorf("argos: framing %q not registered", name)
	}
	fn, ok := c.framingRegistry[name]
	if !ok {
		return nil, fmt.Errorf("argos: framing %q not registered", name)
	}
	return fn, nil
}

// LookupCodec returns the factory registered under name on c.
func (c *Config) LookupCodec(name string) (CodecFunc, error) {
	if c == nil || c.codecRegistry == nil {
		return nil, fmt.Errorf("argos: codec %q not registered", name)
	}
	fn, ok := c.codecRegistry[name]
	if !ok {
		return nil, fmt.Errorf("argos: codec %q not registered", name)
	}
	return fn, nil
}

// ResolveProtocol fills zero factories from registered names. An axis with both
// a factory and a name uses the factory.
func (c *Config) ResolveProtocol(p Protocol) (Protocol, error) {
	out := p
	if out.Transport == nil {
		if p.TransportName == "" {
			return Protocol{}, fmt.Errorf("argos: transport not set and no transport name")
		}
		fn, err := c.LookupTransport(p.TransportName)
		if err != nil {
			return Protocol{}, err
		}
		out.Transport = fn
	}
	if out.Framing == nil {
		if p.FramingName == "" {
			return Protocol{}, fmt.Errorf("argos: framing not set and no framing name")
		}
		fn, err := c.LookupFraming(p.FramingName)
		if err != nil {
			return Protocol{}, err
		}
		out.Framing = fn
	}
	if out.Codec == nil {
		if p.CodecName == "" {
			return Protocol{}, fmt.Errorf("argos: codec not set and no codec name")
		}
		fn, err := c.LookupCodec(p.CodecName)
		if err != nil {
			return Protocol{}, err
		}
		out.Codec = fn
	}
	return out, nil
}

func cloneRegistry[T any](in map[string]T) map[string]T {
	if in == nil {
		return nil
	}
	out := make(map[string]T, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
