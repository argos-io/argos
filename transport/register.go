package transport

import (
	"fmt"
	"sync"
)

// Factory builds a fresh Transport axis. Registrations use this so each listen
// surface can own its listener and session spec; a factory may return a shared
// instance when a test or harness deliberately registers one.
type Factory func() (Transport, error)

var (
	transportMu        sync.RWMutex
	transportFactories = map[string]Factory{}
)

// Register adds a Transport factory for name. Intended for subpackage init and plugins.
// Register panics on empty name or nil factory.
func Register(name string, fn Factory) {
	if name == "" {
		panic("transport: empty name")
	}
	if fn == nil {
		panic("transport: nil factory")
	}
	transportMu.Lock()
	defer transportMu.Unlock()
	transportFactories[name] = fn
}

// New returns a Transport built from the factory registered for name.
func New(name string) (Transport, error) {
	transportMu.RLock()
	fn := transportFactories[name]
	transportMu.RUnlock()
	if fn == nil {
		return nil, fmt.Errorf("transport: unknown name %q", name)
	}
	tr, err := fn()
	if err != nil {
		return nil, err
	}
	if tr == nil {
		return nil, fmt.Errorf("transport: factory %q returned nil", name)
	}
	return tr, nil
}
