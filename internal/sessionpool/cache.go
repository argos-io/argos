package sessionpool

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/transport"
)

// ErrPoolTableFull means the cache is at MaxPools and every pool it holds is in
// use, so no new pool can be created. It is a local capacity limit, not a peer
// error.
var ErrPoolTableFull = errors.New("sessionpool: pool table is full")

// Policy is the per-connection policy one pool is built for: the wire limits and
// the pool limits a caller asks for.
//
// It is part of Key, so two callers asking for different limits get different
// pools rather than whichever one happened to dial first. Every field must stay
// comparable — comparability is what makes a policy an identity instead of a
// value two pools could disagree about. Anything uncomparable (an injected Pipe,
// a *tls.Config, a compressor set) belongs to the registered axis, not here.
type Policy struct {
	Limits transport.Limits
	Pool   transport.PoolLimits
}

// Key identifies one pool: the protocol, the peer, and the policy. The axis name
// is part of it because the axis decides the reuse model, and a registered name
// is the only thing the composition layer carries.
type Key struct {
	Axis     string
	Endpoint string
	Policy   Policy
}

// CacheOptions tunes the process-level pool table.
//
// Zero PoolIdleTimeout or MaxPools means "never evict on that trigger", which
// cuts against the zero-means-no-cap convention the connection limits follow:
// a connection limit left off bounds one connection, while an eviction switch
// left off lets this table grow with every endpoint the process is ever asked to
// reach. Use DefaultCacheOptions unless that is what you want.
//
// ReclaimInterval <= 0 takes the default interval; the sweep always runs, so a
// zero CacheOptions still costs one idle goroutine and evicts nothing.
type CacheOptions struct {
	PoolIdleTimeout time.Duration
	MaxPools        int
	ReclaimInterval time.Duration
}

// DefaultCacheOptions is the eviction policy a Cache is expected to run with: a
// pool holding nothing that has gone unused for five minutes is closed and
// dropped, at most 1024 pools are kept, and one goroutine sweeps every ten
// seconds.
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		PoolIdleTimeout: 5 * time.Minute,
		MaxPools:        1024,
		ReclaimInterval: 10 * time.Second,
	}
}

type cacheEntry struct {
	pool     *Pool
	lastUsed time.Time
}

// Cache is the process-level pool table: one pool per Key.
//
// It exists so connectivity has an owner that is not the caller. A Client has no
// Close and dropping one is not an error, so a pool the client owned would be
// leaked by construction; the cache holds the pools instead and reclaims them on
// its own terms. Nothing a caller holds keeps a pool alive, and nothing a caller
// forgets to release keeps it alive either.
//
// Eviction only ever removes a pool nothing outside it is using — every
// connection it holds has been returned and no dial is in flight — so a pool in
// use is never discarded.
type Cache struct {
	cfg CacheOptions

	mu      sync.Mutex
	entries map[Key]*cacheEntry
	closed  bool

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewCache returns a cache that sweeps itself until Close.
func NewCache(cfg CacheOptions) *Cache {
	if cfg.ReclaimInterval <= 0 {
		cfg.ReclaimInterval = DefaultCacheOptions().ReclaimInterval
	}
	c := &Cache{
		cfg:     cfg,
		entries: make(map[Key]*cacheEntry),
		stopCh:  make(chan struct{}),
	}
	c.wg.Add(1)
	go c.reclaimLoop()
	return c
}

// OpenCall runs one call for m through the pool for key, dialing through dial on
// a miss. conc is the axis' reuse model; it chooses the borrow policy and is
// deliberately not part of Key, because it is a property of the axis and Key
// already names the axis.
//
// The returned Call releases its connection when it is closed, as a pooled call
// always has.
func (c *Cache) OpenCall(ctx context.Context, key Key, conc transport.Concurrency, dial DialFunc,
	m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	p, err := c.pool(key, conc, dial)
	if err != nil {
		return nil, err
	}
	return p.OpenCallReleasing(ctx, key.Endpoint, m, spec)
}

// Len reports how many pools the cache holds. Introspection for tests and
// metrics.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Close closes every pool it holds and stops the sweep. Idempotent.
func (c *Cache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	pools := make([]*Pool, 0, len(c.entries))
	for _, e := range c.entries {
		pools = append(pools, e.pool)
	}
	c.entries = make(map[Key]*cacheEntry)
	c.mu.Unlock()

	c.stopOnce.Do(func() { close(c.stopCh) })
	c.wg.Wait()

	var first error
	for _, p := range pools {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *Cache) pool(key Key, conc transport.Concurrency, dial DialFunc) (*Pool, error) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("sessionpool: cache closed")
	}
	if e, ok := c.entries[key]; ok {
		e.lastUsed = now
		return e.pool, nil
	}
	if c.cfg.MaxPools > 0 && len(c.entries) >= c.cfg.MaxPools && !c.evictOneLocked() {
		return nil, ErrPoolTableFull
	}
	p := newPool(dial, poolOptions(key.Policy, conc), false)
	c.entries[key] = &cacheEntry{pool: p, lastUsed: now}
	return p, nil
}

// evictOneLocked drops the longest-unused idle pool and reports whether it
// dropped one. The caller holds c.mu.
//
// Close runs under c.mu: it takes the pool's own lock and closes connections,
// and a pool never reaches back into the cache, so there is no cycle to deadlock
// on. Close is expected fast and idempotent, the same bargain discardLocked
// makes.
func (c *Cache) evictOneLocked() bool {
	var victim Key
	var oldest time.Time
	found := false
	for k, e := range c.entries {
		if !e.pool.Idle() {
			continue
		}
		if !found || e.lastUsed.Before(oldest) {
			victim, oldest, found = k, e.lastUsed, true
		}
	}
	if !found {
		return false
	}
	e := c.entries[victim]
	delete(c.entries, victim)
	_ = e.pool.Close()
	return true
}

func (c *Cache) reclaimLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.ReclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case now := <-ticker.C:
			c.reclaim(now)
		}
	}
}

// reclaim sweeps every pool: connections the pool itself considers idle or
// expired, then whole pools that hold nothing and have gone unused.
func (c *Cache) reclaim(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	for k, e := range c.entries {
		e.pool.Reclaim(now)
		if c.cfg.PoolIdleTimeout <= 0 {
			continue
		}
		if e.pool.Idle() && now.Sub(e.lastUsed) >= c.cfg.PoolIdleTimeout {
			delete(c.entries, k)
			_ = e.pool.Close()
		}
	}
}

// poolOptions maps a policy plus the axis' reuse model onto pool limits.
// MaxCallsPerConn comes from the axis rather than the policy: how many calls one
// connection carries is the protocol's answer, and the axis is already in Key.
func poolOptions(p Policy, conc transport.Concurrency) Options {
	return Options{
		MaxSessionsPerEndpoint: p.Pool.MaxSessionsPerEndpoint,
		MaxIdleSessions:        p.Pool.MaxIdleSessions,
		SessionIdleTimeout:     p.Pool.SessionIdleTimeout,
		MaxSessionLifetime:     p.Pool.MaxSessionLifetime,
		HandshakeTimeout:       p.Pool.HandshakeTimeout,
		MaxCallsPerConn:        maxCallsPerConn(conc),
	}
}

func maxCallsPerConn(c transport.Concurrency) int {
	if c == transport.Concurrent {
		return -1
	}
	return 1
}
