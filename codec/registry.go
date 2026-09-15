package codec

import "sync"

// Factory constructs a Codec.
type Factory func() Codec

var (
	factoryMu sync.RWMutex
	factories = map[string]Factory{}
)

// Register adds a Codec factory for name. Built-in codecs register from subpackages
// in init; external plugins may call Register. The registry is keyed by name for
// lookup only; it is not a process-level protocol selection table.
func Register(name string, new Factory) {
	if name == "" {
		panic("codec: empty name")
	}
	if new == nil {
		panic("codec: nil factory")
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
