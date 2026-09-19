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
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/transport"
)

// countingTransport wraps an axis to observe how many distinct calls were
// closed. Distinct, because Close is idempotent and legitimately called more
// than once per call: CallStream.Close closes the call, and the ctx cancel that
// follows it fires the stopWatch, which closes the same call again on another
// goroutine — at an arbitrary later moment.
type countingTransport struct {
	inner  transport.Transport
	closes *atomic.Int64
}

func (a *countingTransport) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	call, err := a.inner.OpenCall(ctx, endpoint, m, spec)
	if err != nil {
		return nil, err
	}
	return &countingCall{Call: call, closes: a.closes}, nil
}

func (a *countingTransport) Serve(ctx context.Context, onConn func(context.Context, transport.ServerConn), opts ...transport.ServerOption) error {
	return a.inner.Serve(ctx, onConn, opts...)
}

func (a *countingTransport) CallConcurrency() transport.Concurrency { return a.inner.CallConcurrency() }

func (a *countingTransport) CodecName() string { return a.inner.CodecName() }

type countingCall struct {
	transport.Call
	once   sync.Once
	closes *atomic.Int64
}

func (c *countingCall) Close() error {
	c.once.Do(func() { c.closes.Add(1) })
	return c.Call.Close()
}

// abandonCall opens a call and drops the CallStream without closing it.
// noinline so the CallStream is genuinely unreachable when this returns.
//
//go:noinline
func abandonCall(cli *Client, t *testing.T) {
	t.Helper()
	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if cs == nil {
		t.Fatal("nil CallStream")
	}
}

// TestAbandonedCallStreamReleasesItsCall: a Client has no Close and a caller
// that drops a CallStream without closing it is not an error — the call must
// still be released, or its connection stays borrowed from the axis forever.
func TestAbandonedCallStreamReleasesItsCall(t *testing.T) {
	ax := echoLoopback(t, nil)
	t.Cleanup(func() { _ = ax.Close() })
	attachFakePool(ax, fakePoolOptions())

	var closes atomic.Int64
	tr := &countingTransport{inner: ax, closes: &closes}
	cli, err := New(
		argos.WithTransport(teststack.TransportName(t, tr)),
		argos.WithCodec(loopbackCodecName),
		argos.WithServiceName(testService),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	echoRoundTrip(t, cli, context.Background())
	if got := closes.Load(); got != 1 {
		t.Fatalf("after one complete call: closes = %d, want 1", got)
	}

	abandonCall(cli, t)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && closes.Load() < 2 {
		runtime.GC()
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	if got := closes.Load(); got != 2 {
		t.Fatalf("after abandoning a CallStream: closes = %d, want 2: the call was never released", got)
	}
}
