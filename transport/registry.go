package transport

import "fmt"

// Well-known transport names for configuration.
const (
	NameTCP   = "tcp"
	NameWS    = "ws"
	NameUDP   = "udp"
	NameHTTP1 = "http1"
	NameHTTP2 = "http2"
)

// Factory builds a Transport. It must not Dial or Serve.
type Factory func() (Transport, error)

// Registry maps names to transport factories. Populate at the program edge
// (main, tests) via concrete package Register functions.
type Registry struct {
	m map[string]Factory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{m: make(map[string]Factory)}
}

// Register records name -> factory. Same name and same factory pointer is a
// no-op; a different factory returns an error.
func (r *Registry) Register(name string, fn Factory) error {
	if r == nil {
		return fmt.Errorf("transport: nil Registry")
	}
	if name == "" {
		return fmt.Errorf("transport: empty name")
	}
	if fn == nil {
		return fmt.Errorf("transport: nil Factory for %q", name)
	}
	if r.m == nil {
		r.m = make(map[string]Factory)
	}
	if existing, ok := r.m[name]; ok {
		if fmt.Sprintf("%p", existing) != fmt.Sprintf("%p", fn) {
			return fmt.Errorf("transport: %q already registered", name)
		}
		return nil
	}
	r.m[name] = fn
	return nil
}

// Lookup returns the factory for name.
func (r *Registry) Lookup(name string) (Factory, error) {
	if r == nil || r.m == nil {
		return nil, fmt.Errorf("transport: %q not registered", name)
	}
	fn, ok := r.m[name]
	if !ok {
		return nil, fmt.Errorf("transport: %q not registered", name)
	}
	return fn, nil
}

// Clone returns a shallow copy of the name map.
func (r *Registry) Clone() *Registry {
	if r == nil || r.m == nil {
		return nil
	}
	out := make(map[string]Factory, len(r.m))
	for k, v := range r.m {
		out[k] = v
	}
	return &Registry{m: out}
}
