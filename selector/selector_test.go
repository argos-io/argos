package selector_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos/selector"
	_ "github.com/argos-io/argos/selector/ip"
)

func TestParseRejectsBareHostPort(t *testing.T) {
	_, err := selector.Parse(context.Background(), "127.0.0.1:9090")
	if err == nil {
		t.Fatal("Parse: want error for target without scheme")
	}
}

func TestParseRejectsNilContext(t *testing.T) {
	if _, err := selector.Parse(nil, "ip://127.0.0.1:9090"); err == nil {
		t.Fatal("Parse accepted nil context")
	}
}

func TestParseRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := selector.Parse(ctx, "ip://127.0.0.1:9090"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Parse error = %v, want context.Canceled", err)
	}
}

func TestParseIPScheme(t *testing.T) {
	addr, err := selector.Parse(context.Background(), "ip://127.0.0.1:9090")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "127.0.0.1:9090"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestParseUnknownScheme(t *testing.T) {
	_, err := selector.Parse(context.Background(), "unknown://host:1")
	if err == nil {
		t.Fatal("Parse: want unknown scheme error")
	}
}

func TestParseInvalidAddress(t *testing.T) {
	_, err := selector.Parse(context.Background(), "ip://not-a-host-port")
	if err == nil {
		t.Fatal("Parse: want invalid address error")
	}
}

func TestRegisterExternal(t *testing.T) {
	selector.Register("testscheme", selectorFunc(func(_ context.Context, service string) (string, error) {
		return "parsed:" + service, nil
	}))
	addr, err := selector.Parse(context.Background(), "testscheme://svc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := addr, "parsed:svc"; got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
}

func TestRegisterRejectsInvalidSelector(t *testing.T) {
	tests := []struct {
		name    string
		selectr selector.Selector
	}{
		{name: "empty name", selectr: selectorFunc(func(context.Context, string) (string, error) { return "", nil })},
		{name: "nil selector", selectr: nil},
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
			selector.Register(name, tt.selectr)
		})
	}
}

func TestSelectRespectsContext(t *testing.T) {
	selector.Register("ctxscheme", selectorFunc(func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := selector.Parse(ctx, "ctxscheme://svc")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Parse: got %v, want context.DeadlineExceeded", err)
	}
}

type selectorFunc func(context.Context, string) (string, error)

func (f selectorFunc) Select(ctx context.Context, service string) (string, error) {
	return f(ctx, service)
}
