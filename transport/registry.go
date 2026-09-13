package transport

import "sync"

// Factory constructs a Transport. Address is supplied via ServerOption / ClientOption.
type Factory func() Transport

var (
	factoryMu sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a Transport factory for name. Built-in transports register from
// subpackages in init; external plugins may call Register.
func Register(name string, new Factory) {
	if name == "" {
		panic("transport: empty name")
	}
	if new == nil {
		panic("transport: nil factory")
	}
	factoryMu.Lock()
	factories[name] = new
	factoryMu.Unlock()
}

// Get returns the factory for name, or nil if none is registered.
func Get(name string) Factory {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	return factories[name]
}
