package ip_test

import (
	"strings"
	"testing"

	"github.com/argos-io/argos/selector"
	_ "github.com/argos-io/argos/selector/ip"
)

func TestSelectHostname(t *testing.T) {
	addr, err := selector.Parse("ip://example.com:443")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "example.com:443"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestSelectList(t *testing.T) {
	addr, err := selector.Parse("ip://127.0.0.1:1,127.0.0.2:2")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !strings.HasPrefix(addr, "127.0.0.") {
		t.Fatalf("address = %q, want one of list", addr)
	}
}
