package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
)

func openFilterClient(t *testing.T, dials *atomic.Int64, f filter.OpenFilter) *Client {
	t.Helper()
	return openFilterClientWith(t, dials, 8, f)
}

func openFilterClientWith(t *testing.T, dials *atomic.Int64, maxSessions int, f filter.OpenFilter) *Client {
	t.Helper()
	cli, err := New(
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithMaxSessionsPerEndpoint(maxSessions),
		argos.WithOpenFilter(f),
		sequentialLoopback(t, dials),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// An OpenFilter that opens the call and then fails must hand the borrowed
// session back to the pool. The entry used to stay lent forever: the pool only
// reclaims refcount 0, so each failure permanently consumed one
// MaxSessionsPerEndpoint slot until the client could open nothing at all.
func TestOpenFilterErrorAfterNextReleasesSession(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	want := errors.New("filter failed after opening")
	var opened atomic.Int64
	const failures = 5
	cli := openFilterClientWith(t, &dials, 2, func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
		st, err := next(ctx, m)
		if err != nil {
			return nil, err
		}
		if opened.Add(1) <= failures {
			return nil, want
		}
		return st, nil
	})

	for i := 0; i < failures; i++ {
		if _, err := cli.Open(context.Background(), testMethod(t)); !errors.Is(err, want) {
			t.Fatalf("Open %d: %v, want %v", i, err, want)
		}
	}

	// With MaxSessionsPerEndpoint=2, five leaked entries exhaust the pool.
	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open after %d failed opens: %v; the sessions leaked and exhausted the pool", failures, err)
	}
	_ = cs.Close()
}

// (nil, nil) after next leaves no stream at all. Open must reject it; accepting
// it produced a CallStream whose Send/Recv called a method on a nil Stream
// inside a goroutine, which no caller can recover from.
func TestOpenFilterNilStreamAfterNextIsRejected(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	cli := openFilterClient(t, &dials, func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
		if _, err := next(ctx, m); err != nil {
			return nil, err
		}
		return nil, nil
	})

	cs, err := cli.Open(context.Background(), testMethod(t))
	if err == nil {
		_ = cs.Close()
		t.Fatal("Open accepted a nil stream; every operation on it would panic")
	}
	if !errors.Is(err, filter.ErrOpenFilterMisuse) {
		t.Fatalf("Open err = %v, want ErrOpenFilterMisuse", err)
	}
	// The call the filter opened must still have been released.
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1", got)
	}
}
