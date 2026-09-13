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
