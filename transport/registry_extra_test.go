package transport

import (
	"context"
	"testing"
)

type stubTransport struct{}

func (stubTransport) ListenAndServe(context.Context, func(context.Context, string, Framer) error, ...ServerOption) error {
	return nil
}

func (stubTransport) Open(context.Context, string, ...ClientOption) (Framer, error) {
	return nil, nil
}

func TestGetUnknownTransport(t *testing.T) {
	if Get("no-such-transport-name") != nil {
		t.Fatal("expected nil factory")
	}
}

func TestRegisterPanicsOnInvalidInput(t *testing.T) {
	t.Run("empty name", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		Register("", func() Transport { return stubTransport{} })
	})
	t.Run("nil factory", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		Register("bad-factory", nil)
	})
}
