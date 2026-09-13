package selector_test

import (
	"testing"

	"github.com/argos-io/argos/selector"
	_ "github.com/argos-io/argos/selector/ip"
)

func TestParseIPBareHostPort(t *testing.T) {
	addr, err := selector.Parse("127.0.0.1:9090")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "127.0.0.1:9090"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestParseIPScheme(t *testing.T) {
	addr, err := selector.Parse("ip://127.0.0.1:9090")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "127.0.0.1:9090"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestParseUnknownScheme(t *testing.T) {
	_, err := selector.Parse("unknown://host:1")
	if err == nil {
		t.Fatal("Parse: want unknown scheme error")
	}
}

func TestParseInvalidAddress(t *testing.T) {
	_, err := selector.Parse("ip://not-a-host-port")
	if err == nil {
		t.Fatal("Parse: want invalid address error")
	}
}

func TestRegisterExternal(t *testing.T) {
	selector.Register("testscheme", selectorFunc(func(service string) (string, error) {
		return "parsed:" + service, nil
	}))
	addr, err := selector.Parse("testscheme://svc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "parsed:svc"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

type selectorFunc func(string) (string, error)

func (f selectorFunc) Select(service string) (string, error) { return f(service) }
