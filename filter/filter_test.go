package filter

import (
	"context"
	"errors"
	"testing"

	"github.com/argos-io/argos/stream"
)

type stubStream struct{ stream.Stream }

func TestChainOrder(t *testing.T) {
	var order []int
	mk := func(id int) Filter {
		return func(ctx context.Context, method string, st stream.Stream, next Handler) error {
			order = append(order, id)
			return next(ctx, method, st)
		}
	}
	end := func(context.Context, string, stream.Stream) error {
		order = append(order, 99)
		return nil
	}
	_ = Chain([]Filter{mk(1), mk(2), mk(3)}, end)(context.Background(), "m", stubStream{})
	if len(order) != 4 || order[0] != 1 || order[1] != 2 || order[2] != 3 || order[3] != 99 {
		t.Fatalf("order = %v, want [1 2 3 99]", order)
	}
}

func TestChainShortCircuit(t *testing.T) {
	want := errors.New("stop")
	deny := func(context.Context, string, stream.Stream, Handler) error {
		return want
	}
	called := false
	end := func(context.Context, string, stream.Stream) error {
		called = true
		return nil
	}
	err := Chain([]Filter{deny}, end)(context.Background(), "m", stubStream{})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if called {
		t.Fatal("end handler ran after short-circuit")
	}
}

func TestChainRejectsNilHandlers(t *testing.T) {
	if err := Chain(nil, nil)(context.Background(), "m", stubStream{}); err == nil {
		t.Fatal("nil terminal handler did not return an error")
	}
	if err := Chain([]Filter{nil}, func(context.Context, string, stream.Stream) error { return nil })(context.Background(), "m", stubStream{}); err == nil {
		t.Fatal("nil filter did not return an error")
	}
}
