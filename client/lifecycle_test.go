package client

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/teststack"
)

func freshLoopback(t *testing.T, dialed *[]*fake.ByteConn, dialMu *sync.Mutex) *fake.Transport {
	t.Helper()
	return buildLoopbackTransport(t, nil, dialed, dialMu)
}

func TestClientOpenUsesSharedTransport(t *testing.T) {
	t.Parallel()
	cli, err := newClientLoopback(t, freshLoopback(t, nil, nil),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = cs.Close()
}

func TestClientsUseDistinctTransports(t *testing.T) {
	t.Parallel()
	var transports []*fake.Transport
	axFor := func() *fake.Transport {
		ax := buildLoopbackTransport(t, nil, nil, nil)
		transports = append(transports, ax)
		return ax
	}
	cfg := &argos.Options{MaxConcurrentCalls: 4}
	cli1, err := newClientLoopback(t, axFor(), argos.WithClientOptions(cfg), argos.WithServiceName(testService), argos.WithTarget(testTarget))
	if err != nil {
		t.Fatalf("New #1: %v", err)
	}
	cli2, err := newClientLoopback(t, axFor(), argos.WithClientOptions(cfg), argos.WithServiceName(testService), argos.WithTarget(testTarget))
	if err != nil {
		t.Fatalf("New #2: %v", err)
	}
	if cli1 == nil || cli2 == nil {
		t.Fatal("New returned a nil Client")
	}
	if len(transports) != 2 || transports[0] == transports[1] {
		t.Fatal("expected two distinct transport instances")
	}
}

func TestCallStreamLeakReportsPhaseLeak(t *testing.T) {
	if raceEnabled {
		t.Skip("CallStream leak GC observation is flaky under the race detector")
	}

	var leaked atomic.Bool
	var phase atomic.Uint32
	cli, err := newClientLoopback(t, freshLoopback(t, nil, nil),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithClientCallErrorObserver(func(info argos.CallInfo, _ error) {
			if info.Phase == argos.PhaseLeak {
				phase.Store(uint32(info.Phase))
				leaked.Store(true)
			}
		}),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	func() {
		cs, err := cli.Open(context.Background(), testMethod(t))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		_ = cs
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		if leaked.Load() {
			if argos.Phase(phase.Load()) != argos.PhaseLeak {
				t.Fatalf("phase = %v, want PhaseLeak", phase.Load())
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("PhaseLeak not reported after forcing GC (AddCleanup)")
}

// A call that has been Closed must leave nothing running. Every call here
// reuses the one session the pool holds, so the goroutine count reaches its
// steady state after the warmup: a per-call leak would grow it by the call
// count, which the slack cannot absorb.
func TestClosedCallsLeaveNoGoroutines(t *testing.T) {
	t.Parallel()
	const calls = 20
	cli := newTestClient(t)

	echoRoundTrip(t, cli, context.Background())
	echoRoundTrip(t, cli, context.Background())

	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	for i := 0; i < calls; i++ {
		echoRoundTrip(t, cli, context.Background())
	}

	deadline := time.Now().Add(3 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(30 * time.Millisecond)
		after = runtime.NumGoroutine()
		if after <= before+8 {
			return
		}
	}
	t.Fatalf("goroutines before=%d after=%d over %d calls (slack 8)", before, after, calls)
}

// closeCountingAxis records Close so a test can assert the Client never made
// one: closing the axis, and with it the pool, belongs to whoever constructed
// it.
type closeCountingAxis struct {
	*fake.Transport
	closes atomic.Int64
}

func (a *closeCountingAxis) Close() error {
	a.closes.Add(1)
	return a.Transport.Close()
}

// A Client is a plain handle over an axis the caller owns: it holds no pool and
// nothing to release. The same instance may carry other Clients and a listen
// surface, so a Client that closed or drained it would take them down with it.
func TestClientLeavesAxisToItsOwner(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	inner := buildLoopbackTransport(t, &dials, nil, nil)
	opts := []argos.ClientOption{
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithTarget(testTarget),
	}
	attachFakePool(inner, fakePoolOptions())
	ax := &closeCountingAxis{Transport: inner}

	newClient := func() *Client {
		t.Helper()
		cli, err := New(append([]argos.ClientOption{
			argos.WithTransport(teststack.TransportName(t, ax)),
			argos.WithCodec(loopbackCodecName),
		}, opts...)...)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return cli
	}

	ctx, cancel := context.WithCancel(context.Background())
	func() {
		cli1 := newClient()
		echoRoundTrip(t, cli1, ctx)
	}()
	// Cancel the ctx the first Client opened its call with, and drop the Client
	// with it: nothing the first Client did may outlive it.
	cancel()
	runtime.GC()
	runtime.GC()

	cli2 := newClient()
	echoRoundTrip(t, cli2, context.Background())

	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1: the second Client re-dialed instead of reusing the session the first one left in the axis' pool", got)
	}
	if got := ax.closes.Load(); got != 0 {
		t.Fatalf("Client closed the axis %d time(s); the axis belongs to whoever constructed it", got)
	}

	// The axis is still the constructor's to close.
	if err := ax.Close(); err != nil {
		t.Fatalf("axis Close: %v", err)
	}
	if got := ax.closes.Load(); got != 1 {
		t.Fatalf("axis Close count = %d, want 1", got)
	}
}
