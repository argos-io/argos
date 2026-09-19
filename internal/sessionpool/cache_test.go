package sessionpool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/transport"
)

// The cache only ever dials, lends and closes a ClientConn, so this is all the
// fake needs to be.
type cacheTestConn struct{ closed atomic.Bool }

func (c *cacheTestConn) OpenCall(context.Context, descriptor.Method, transport.CallSpec) (transport.Call, error) {
	return &cacheTestCall{}, nil
}

func (c *cacheTestConn) Reusable() bool { return !c.closed.Load() }
func (c *cacheTestConn) Close() error   { c.closed.Store(true); return nil }

type cacheTestCall struct{ closed atomic.Bool }

func (c *cacheTestCall) Method() string                { return "svc.Method" }
func (c *cacheTestCall) Deadline() (time.Time, bool)   { return time.Time{}, false }
func (c *cacheTestCall) SendHeaders() error            { return nil }
func (c *cacheTestCall) Finish(error) error            { return nil }
func (c *cacheTestCall) Recv() ([]byte, func(), error) { return nil, nil, nil }
func (c *cacheTestCall) Send([]byte) error             { return nil }
func (c *cacheTestCall) HalfClose() error              { return nil }
func (c *cacheTestCall) Close() error                  { c.closed.Store(true); return nil }

var (
	_ transport.ClientConn = (*cacheTestConn)(nil)
	_ transport.Call       = (*cacheTestCall)(nil)
)

// dialer counts how many connections were opened, and keeps them, so a test can
// ask whether a connection was reused or a new one dialed.
type dialer struct {
	mu    sync.Mutex
	dials int
	conns []*cacheTestConn
}

func (d *dialer) dial(context.Context, string) (transport.ClientConn, error) {
	c := &cacheTestConn{}
	d.mu.Lock()
	d.dials++
	d.conns = append(d.conns, c)
	d.mu.Unlock()
	return c, nil
}

func (d *dialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials
}

func testMethod() descriptor.Method { return descriptor.MustMethod("svc.Method", descriptor.Unary) }

// reusePolicy keeps an idle connection, so a second call on the same pool
// reuses it. The zero MaxIdleSessions (the built-in baseline) keeps none, which
// would make every call dial and hide reuse from every test here.
func reusePolicy(maxIdle int) Policy {
	return Policy{Pool: transport.PoolLimits{MaxIdleSessions: maxIdle, MaxSessionsPerEndpoint: 4}}
}

func openAndClose(t *testing.T, c *Cache, key Key, d *dialer) {
	t.Helper()
	call, err := c.OpenCall(context.Background(), key, transport.Sequential, d.dial, testMethod(), transport.CallSpec{})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	if err := call.Close(); err != nil {
		t.Fatalf("call.Close: %v", err)
	}
}

func TestCacheSharesOnePoolPerKey(t *testing.T) {
	c := NewCache(CacheOptions{})
	defer c.Close()
	d := &dialer{}
	key := Key{Axis: "grpc", Endpoint: "127.0.0.1:1", Policy: reusePolicy(8)}

	openAndClose(t, c, key, d)
	openAndClose(t, c, key, d)

	if got := d.count(); got != 1 {
		t.Fatalf("dials = %d, want 1: the same key must share one pool", got)
	}
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
}

func TestCacheSeparatesPoolsByEveryKeyDimension(t *testing.T) {
	base := Key{Axis: "grpc", Endpoint: "127.0.0.1:1", Policy: reusePolicy(8)}
	for _, tc := range []struct {
		name string
		key  Key
	}{
		{"axis", Key{Axis: "httpunary", Endpoint: base.Endpoint, Policy: base.Policy}},
		{"endpoint", Key{Axis: base.Axis, Endpoint: "127.0.0.1:2", Policy: base.Policy}},
		{"policy", Key{Axis: base.Axis, Endpoint: base.Endpoint, Policy: reusePolicy(4)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCache(CacheOptions{})
			defer c.Close()
			d := &dialer{}

			openAndClose(t, c, base, d)
			openAndClose(t, c, tc.key, d)

			if got := d.count(); got != 2 {
				t.Fatalf("dials = %d, want 2: %s must select a different pool", got, tc.name)
			}
			if got := c.Len(); got != 2 {
				t.Fatalf("Len = %d, want 2", got)
			}
		})
	}
}

func TestCacheKeepsPoolWhileACallHoldsIt(t *testing.T) {
	c := NewCache(CacheOptions{PoolIdleTimeout: time.Minute})
	defer c.Close()
	d := &dialer{}
	key := Key{Axis: "grpc", Endpoint: "127.0.0.1:1", Policy: reusePolicy(8)}

	call, err := c.OpenCall(context.Background(), key, transport.Sequential, d.dial, testMethod(), transport.CallSpec{})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}

	// Long past the idle timeout, but a call is still holding a connection.
	c.reclaim(time.Now().Add(time.Hour))

	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1: a pool with a call in flight must not be evicted", got)
	}
	if err := call.Close(); err != nil {
		t.Fatalf("call.Close: %v", err)
	}
}

func TestCacheEvictsPoolThatHoldsNothing(t *testing.T) {
	c := NewCache(CacheOptions{PoolIdleTimeout: time.Minute})
	defer c.Close()
	d := &dialer{}
	key := Key{Axis: "grpc", Endpoint: "127.0.0.1:1", Policy: reusePolicy(8)}

	openAndClose(t, c, key, d)
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}

	// Not yet idle long enough.
	c.reclaim(time.Now().Add(30 * time.Second))
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1: evicted before the idle timeout", got)
	}

	c.reclaim(time.Now().Add(2 * time.Minute))
	if got := c.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0: an empty pool past its idle timeout must be dropped", got)
	}
	// The connection it held was closed with it.
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.conns) != 1 || !d.conns[0].closed.Load() {
		t.Fatal("eviction did not close the pool's connections")
	}
}

func TestCacheRefusesNewPoolWhenTableIsFullAndNothingIsIdle(t *testing.T) {
	c := NewCache(CacheOptions{MaxPools: 1})
	defer c.Close()
	d := &dialer{}

	held := Key{Axis: "grpc", Endpoint: "127.0.0.1:1", Policy: reusePolicy(8)}
	call, err := c.OpenCall(context.Background(), held, transport.Sequential, d.dial, testMethod(), transport.CallSpec{})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}

	other := Key{Axis: "grpc", Endpoint: "127.0.0.1:2", Policy: reusePolicy(8)}
	if _, err := c.OpenCall(context.Background(), other, transport.Sequential, d.dial, testMethod(), transport.CallSpec{}); !errors.Is(err, ErrPoolTableFull) {
		t.Fatalf("OpenCall on a full table = %v, want ErrPoolTableFull", err)
	}

	// The in-use pool was not sacrificed to make room.
	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	if err := call.Close(); err != nil {
		t.Fatalf("call.Close: %v", err)
	}
}

func TestCacheReusesRoomFreedByAnIdlePool(t *testing.T) {
	c := NewCache(CacheOptions{MaxPools: 1})
	defer c.Close()
	d := &dialer{}

	first := Key{Axis: "grpc", Endpoint: "127.0.0.1:1", Policy: reusePolicy(8)}
	openAndClose(t, c, first, d)

	second := Key{Axis: "grpc", Endpoint: "127.0.0.1:2", Policy: reusePolicy(8)}
	openAndClose(t, c, second, d)

	if got := c.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1: the idle pool should have been evicted to make room", got)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.conns[0].closed.Load() {
		t.Fatal("the evicted pool's connection was not closed")
	}
}

func TestCacheCloseClosesPoolsAndStopsSweeping(t *testing.T) {
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	c := NewCache(DefaultCacheOptions())
	d := &dialer{}
	key := Key{Axis: "grpc", Endpoint: "127.0.0.1:1", Policy: reusePolicy(8)}
	openAndClose(t, c, key, d)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil (idempotent)", err)
	}
	if got := c.Len(); got != 0 {
		t.Fatalf("Len = %d after Close, want 0", got)
	}
	d.mu.Lock()
	closed := d.conns[0].closed.Load()
	d.mu.Unlock()
	if !closed {
		t.Fatal("Close did not close the pool's connections")
	}
	if _, err := c.OpenCall(context.Background(), key, transport.Sequential, d.dial, testMethod(), transport.CallSpec{}); err == nil {
		t.Fatal("OpenCall after Close returned no error")
	}

	// The sweep goroutine is gone: the count is back to where it started, with
	// slack for the runtime's own goroutines.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines = %d after Close, want <= %d: the sweep did not stop", runtime.NumGoroutine(), before+2)
}
