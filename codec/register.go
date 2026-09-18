package codec

import (
	"fmt"
	"sync"
)

// Factory builds a fresh Codec. Registrations use this so each Client or listen
// surface can own its own instance without sharing mutable state.
type Factory func() (Codec, error)

var (
	codecMu        sync.RWMutex
	codecFactories = map[string]Factory{}
)

// Register adds a Codec factory for name. Intended for subpackage init and plugins.
// Register panics on empty name or nil factory.
func Register(name string, fn Factory) {
	if name == "" {
		panic("codec: empty name")
	}
	if fn == nil {
		panic("codec: nil factory")
	}
	codecMu.Lock()
	defer codecMu.Unlock()
	codecFactories[name] = fn
}

// New returns a Codec built from the factory registered for name.
func New(name string) (Codec, error) {
	codecMu.RLock()
	fn := codecFactories[name]
	codecMu.RUnlock()
	if fn == nil {
		return nil, fmt.Errorf("codec: unknown name %q", name)
	}
	cd, err := fn()
	if err != nil {
		return nil, err
	}
	if cd == nil {
		return nil, fmt.Errorf("codec: factory %q returned nil", name)
	}
	return cd, nil
}
