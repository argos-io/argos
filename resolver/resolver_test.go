package resolver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos/resolver"
	_ "github.com/argos-io/argos/resolver/ip"
)

func TestParseRejectsBareHostPort(t *testing.T) {
	_, err := resolver.Parse(context.Background(), "127.0.0.1:7001")
	if err == nil {
		t.Fatal("Parse: want error for target without scheme")
	}
}

func TestParseRejectsNilContext(t *testing.T) {
	// A typed nil, not the literal: passing a nil ctx is exactly what this test
	// exists to reject.
	var nilCtx context.Context
	if _, err := resolver.Parse(nilCtx, "ip://127.0.0.1:7001"); err == nil {
		t.Fatal("Parse accepted nil context")
	}
}

func TestParseRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Parse(ctx, "ip://127.0.0.1:7001"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Parse error = %v, want context.Canceled", err)
	}
}

func TestParseIPScheme(t *testing.T) {
	addr, err := resolver.Parse(context.Background(), "ip://127.0.0.1:7001")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "127.0.0.1:7001"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestParseUnknownScheme(t *testing.T) {
	_, err := resolver.Parse(context.Background(), "unknown://host:1")
	if err == nil {
		t.Fatal("Parse: want unknown scheme error")
	}
}

func TestParseInvalidAddress(t *testing.T) {
	_, err := resolver.Parse(context.Background(), "ip://not-a-host-port")
	if err == nil {
		t.Fatal("Parse: want invalid address error")
	}
}

func TestRegisterExternal(t *testing.T) {
	resolver.Register("testscheme", resolverFunc(func(_ context.Context, service string) (string, error) {
		return "parsed:" + service, nil
	}))
	addr, err := resolver.Parse(context.Background(), "testscheme://svc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "parsed:svc"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestRegisterRejectsInvalidResolver(t *testing.T) {
	tests := []struct {
		name string
		r    resolver.Resolver
	}{
		{name: "empty name", r: resolverFunc(func(context.Context, string) (string, error) { return "", nil })},
		{name: "nil resolver", r: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("Register did not panic")
				}
			}()
			name := "valid"
			if tt.name == "empty name" {
				name = ""
			}
			resolver.Register(name, tt.r)
		})
	}
}

func TestSelectRespectsContext(t *testing.T) {
	resolver.Register("ctxscheme", resolverFunc(func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := resolver.Parse(ctx, "ctxscheme://svc")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Parse: got %v, want context.DeadlineExceeded", err)
	}
}

type resolverFunc func(context.Context, string) (string, error)

func (f resolverFunc) Select(ctx context.Context, service string) (string, error) {
	return f(ctx, service)
}
