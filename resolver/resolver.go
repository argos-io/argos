// Package resolver resolves client targets by URI scheme into dial addresses.
//
// Target format is scheme://service-identifier, for example ip://127.0.0.1:9090.
// Each scheme registers a Resolver that interprets the body; implementations may
// parse direct addresses (ip) or perform service discovery. Built-in schemes
// register from subpackages (see resolver/ip); external code may call Register.
package resolver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// Resolver resolves a target body (service-identifier) to a dial address.
type Resolver interface {
	Select(ctx context.Context, service string) (string, error)
}

var (
	mu       sync.RWMutex
	registry = map[string]Resolver{}
)

// Register adds a Resolver for scheme name. Intended for subpackage init and plugins.
func Register(name string, r Resolver) {
	if name == "" {
		panic("resolver: empty name")
	}
	if isNilResolver(r) {
		panic("resolver: nil resolver")
	}
	mu.Lock()
	defer mu.Unlock()
	registry[name] = r
}

// Get returns the Resolver for scheme name, or nil if none is registered.
func Get(name string) Resolver {
	mu.RLock()
	defer mu.RUnlock()
	return registry[name]
}

// Parse resolves target to a dial address. Target must be scheme://service-identifier
// (for example ip://127.0.0.1:9090); bare host:port is rejected.
func Parse(ctx context.Context, target string) (string, error) {
	if ctx == nil {
		return "", errors.New("resolver: nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	scheme, body := splitScheme(target)
	if scheme == "" {
		return "", fmt.Errorf("resolver: target must be scheme://service-identifier")
	}
	if body == "" {
		return "", fmt.Errorf("resolver: missing service identifier after %q://", scheme)
	}
	r := Get(scheme)
	if r == nil {
		return "", fmt.Errorf("resolver: unknown scheme %q", scheme)
	}
	return r.Select(ctx, body)
}

func isNilResolver(r Resolver) bool {
	if r == nil {
		return true
	}
	v := reflect.ValueOf(r)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func splitScheme(target string) (scheme, body string) {
	if i := strings.Index(target, "://"); i >= 0 {
		return target[:i], target[i+3:]
	}
	return "", target
}
