// Package selector resolves client targets by URI scheme into dial addresses.
//
// Target format is scheme://service-identifier, for example ip://127.0.0.1:9090.
// Each scheme registers a Selector that interprets the body; implementations may
// parse direct addresses (ip) or perform service discovery. Built-in schemes
// register from subpackages (see selector/ip); external code may call Register.
package selector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// Selector resolves a target body (service-identifier) to a dial address.
type Selector interface {
	Select(ctx context.Context, service string) (string, error)
}

var (
	mu       sync.RWMutex
	registry = map[string]Selector{}
)

// Register adds a Selector for scheme name. Intended for subpackage init and plugins.
func Register(name string, s Selector) {
	if name == "" {
		panic("selector: empty name")
	}
	if isNilSelector(s) {
		panic("selector: nil selector")
	}
	mu.Lock()
	defer mu.Unlock()
	registry[name] = s
}

// Get returns the Selector for scheme name, or nil if none is registered.
func Get(name string) Selector {
	mu.RLock()
	defer mu.RUnlock()
	return registry[name]
}

// Parse resolves target to a dial address. Target must be scheme://service-identifier
// (for example ip://127.0.0.1:9090); bare host:port is rejected.
func Parse(ctx context.Context, target string) (string, error) {
	if ctx == nil {
		return "", errors.New("selector: nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	scheme, body := splitScheme(target)
	if scheme == "" {
		return "", fmt.Errorf("selector: target must be scheme://service-identifier")
	}
	if body == "" {
		return "", fmt.Errorf("selector: missing service identifier after %q://", scheme)
	}
	s := Get(scheme)
	if s == nil {
		return "", fmt.Errorf("selector: unknown scheme %q", scheme)
	}
	return s.Select(ctx, body)
}

func isNilSelector(s Selector) bool {
	if s == nil {
		return true
	}
	v := reflect.ValueOf(s)
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
