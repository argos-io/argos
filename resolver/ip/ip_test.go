package ip

import (
	"context"
	"strings"
	"testing"

	"github.com/argos-io/argos/resolver"
)

func TestSelectHostname(t *testing.T) {
	addr, err := resolver.Parse(context.Background(), "ip://example.com:443")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "example.com:443"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestSelectList(t *testing.T) {
	addr, err := resolver.Parse(context.Background(), "ip://127.0.0.1:1,127.0.0.2:2")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !strings.HasPrefix(addr, "127.0.0.") {
		t.Fatalf("address = %q, want one of list", addr)
	}
}
