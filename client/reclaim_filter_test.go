package client

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/stream"
)

// TestStreamHeldByFilterKeepsCallOpen is the guard on where reclaim is
// anchored. A filter that keeps the Stream it was handed can still be using
// the call after the caller has dropped its CallStream: the call must stay
// open then. Anchoring the reclaim on the CallStream instead would close a
// live exchange out from under that filter.
func TestStreamHeldByFilterKeepsCallOpen(t *testing.T) {
	ax := echoLoopback(t, nil)
	t.Cleanup(func() { _ = ax.Close() })
	attachFakePool(ax, fakePoolOptions())

	var closes atomic.Int64
	var leaked atomic.Bool
	var mu sync.Mutex
	var held stream.Stream

	capture := func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
		st, err := next(ctx, m)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		held = st
		mu.Unlock()
		return st, nil
	}

	cli, err := New(
		argos.WithTransport(teststack.TransportName(t, &countingTransport{inner: ax, closes: &closes})),
		argos.WithCodec(loopbackCodecName),
		argos.WithServiceName(testService),
		argos.WithTarget(testTarget),
		argos.WithOpenFilter(capture),
		argos.WithClientCallErrorObserver(func(argos.CallInfo, error) { leaked.Store(true) }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	abandonCall(cli, t)

	// Run GC until the abandoned CallStream has actually been reclaimed, so
	// the assertion below is not vacuous.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !leaked.Load() {
		runtime.GC()
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	if !leaked.Load() {
		t.Fatal("abandoned CallStream was never reclaimed; guard not exercised")
	}
	if got := closes.Load(); got != 0 {
		t.Fatalf("call closed while a filter still holds its stream: closes = %d", got)
	}

	// And the held stream still works end to end.
	mu.Lock()
	st := held
	mu.Unlock()
	if st == nil {
		t.Fatal("filter captured no stream")
	}
	if err := st.Send([]byte("ping")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := st.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var got []byte
	if err := st.Recv(&got); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("Recv = %q, want ping", got)
	}
	runtime.KeepAlive(st)
}
