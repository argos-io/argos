// Package sessionpool is connection pooling for a client-side protocol axis.
//
// It is protocol-agnostic bookkeeping: buckets, capacity, idle and lifetime.
// Every judgement that needs to read the wire — the handshake, and whether an
// unused connection is still good — belongs to the axis that supplies the
// connections. That is why DialFunc hands back a connection that has already
// handshaken, and why lend and return only ever consult Reusable().
//
// # Acquire / Release contract
//
// Acquire returns a transport.ClientConn with the pool's in-flight refcount
// already incremented by one (a capacity slot pretaken for the forthcoming
// OpenCall). The caller must OpenCall on that connection, then after
// Call.Close returns call Release exactly once for that Acquire.
//
// OpenCall combines Acquire + sess.OpenCall with Busy/Spent fallback: on
// transport.ErrConnBusy it switches session (retry ≤ MaxSessionsPerEndpoint)
// and never leaks that sentinel; on exhaust it returns status.ErrSessionsExhausted.
//
// Release decrements the refcount and applies the four-state return (§4.6):
//
//	Reusable && refcount==0 && idle room → idle queue
//	Reusable && refcount==0 && idle full → Session.Close
//	!Reusable && refcount==0             → Session.Close
//	refcount > 0                         → keep (Concurrent in-flight; do not Close)
//
// Acquire never waits for another caller's Release when MaxSessionsPerEndpoint
// is reached; it returns status.ErrSessionsExhausted immediately. The only
// wait is multiplexed cold-start singleflight around DialFunc.
//
// Reusable() is read under the pool lock before lend and on return, mutually
// exclusive with idle/lifetime reclaim. The pool keys buckets by endpoint only.
package sessionpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/establish"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// Options holds instance-level pool limits.
type Options struct {
	MaxSessionsPerEndpoint int
	MaxIdleSessions        int // 0 = keep no idle sessions
	SessionIdleTimeout     time.Duration
	MaxSessionLifetime     time.Duration
	HandshakeTimeout       time.Duration // bounds one DialFunc call
	// MaxCallsPerConn is the protocol's reuse model as a number: 1 for
	// sequential and one-shot connections, -1 for multiplexed ones. A
	// multiplexed pool also singleflights cold start, because every waiter
	// would otherwise dial a connection it does not need.
	MaxCallsPerConn int
}

// DialFunc produces one connection for endpoint, handshake included. The pool
// never sees a half-established connection: an implementation that dials but
// fails to handshake releases the socket itself and returns an error.
type DialFunc func(ctx context.Context, endpoint string) (transport.ClientConn, error)

// Pool is a concrete endpoint-keyed connection pool.
type Pool struct {
	dial DialFunc
	// cfg is written once, in New, and never again: the limits an axis will
	// enforce are fixed when the axis is constructed. Readers below therefore
	// take no lock. Installing limits at bind time instead is what made these
	// reads race a writer and let a second Client's Options overwrite the first's.
	cfg    Options
	capPer int // 1 for sequential/one-shot; -1 for multiplexed (unlimited)

	mu      sync.Mutex
	closed  bool
	buckets map[string]*bucket
	bySess  map[transport.ClientConn]*entry
	flights map[string]*dialFlight // multiplexed cold start only

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type bucket struct {
	entries []*entry
	idle    []*entry
	// pendingDial counts in-flight DialFunc reservations toward
	// MaxSessionsPerEndpoint (sequential / one-shot).
	pendingDial int
}

type entry struct {
	sess      transport.ClientConn
	endpoint  string
	refcount  int
	createdAt time.Time
	idleSince time.Time
	inIdle    bool
}

type dialFlight struct {
	done chan struct{}
	sess transport.ClientConn
	err  error
}

// DefaultOptions is the pool-limit baseline every axis seeds its construction-
// time pool from when WithPool is not used. argos.Options does not carry these
// fields — the axis is the only place they live.
//
// Zero means no cap on that dimension where the pool checks > 0 before enforcing
// (MaxSessionsPerEndpoint, SessionIdleTimeout, MaxSessionLifetime,
// HandshakeTimeout). MaxIdleSessions defaults to 0 (keep no idle sessions);
// set < 0 for no cap on idle queue length. MaxCallsPerConn is left zero
// because it is the protocol's reuse model, which only the axis knows.
func DefaultOptions() Options {
	return Options{}
}

// Options returns the pool's effective limits. It exists so an axis can report
// what it will actually enforce: the pool has already filled in its defaults by
// the time this is readable, so the values are the ones in force.
func (p *Pool) Options() Options {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg
}

// New builds a pool. Limits are taken as given: zero means the pool does not
// bound that dimension (see DefaultOptions). MaxCallsPerConn <= 0 other than
// -1 means one call per connection.
func New(dial DialFunc, cfg Options) *Pool {
	capPer := 1
	if cfg.MaxCallsPerConn < 0 {
		capPer = -1
	} else if cfg.MaxCallsPerConn > 1 {
		capPer = cfg.MaxCallsPerConn
	}
	p := &Pool{
		dial:    dial,
		cfg:     cfg,
		capPer:  capPer,
		buckets: make(map[string]*bucket),
		bySess:  make(map[transport.ClientConn]*entry),
		flights: make(map[string]*dialFlight),
		stopCh:  make(chan struct{}),
	}
	p.wg.Add(1)
	go p.reclaimLoop()
	return p
}

// Acquire pretakes one in-flight slot on a reusable session, or dials a new
// one. It does not call OpenCall.
func (p *Pool) Acquire(ctx context.Context, endpoint string) (transport.ClientConn, error) {
	return p.acquire(ctx, endpoint, nil)
}

// OpenCall acquires a session, calls sess.OpenCall, and on transport.ErrConnBusy
// releases the pretaken slot, skips that session for this attempt chain, and
// retries with another. Retry count is at most MaxSessionsPerEndpoint.
//
// On exhaust it returns status.ErrSessionsExhausted (Code ResourceExhausted).
// transport.ErrConnBusy and transport.ErrConnSpent never leak to the caller.
//
// On transport.ErrConnSpent the session is closed/discarded and another is tried.
//
// On success the caller must Release(sess) after Call.Close returns (same
// contract as Acquire).
func (p *Pool) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, transport.ClientConn, error) {
	max := p.cfg.MaxSessionsPerEndpoint
	skip := make(map[transport.ClientConn]struct{})

	for attempt := 0; ; attempt++ {
		if max > 0 && attempt >= max {
			break
		}
		sess, err := p.acquire(ctx, endpoint, skip)
		if err != nil {
			return nil, nil, err
		}
		call, err := sess.OpenCall(ctx, m, spec)
		if err == nil {
			return call, sess, nil
		}
		if errors.Is(err, transport.ErrConnBusy) {
			skip[sess] = struct{}{}
			p.Release(sess)
			continue
		}
		if errors.Is(err, transport.ErrConnSpent) {
			_ = sess.Close()
			p.Release(sess)
			continue
		}
		p.Release(sess)
		return nil, nil, err
	}
	return nil, nil, status.ErrSessionsExhausted
}

// OpenCallReleasing is OpenCall with Release folded into Call.Close, which is
// what transport.Transport promises its callers: they hold a call, never a
// connection. Every axis that pools wants this shape, so it lives here rather
// than being re-wrapped by each one.
func (p *Pool) OpenCallReleasing(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	call, conn, err := p.OpenCall(ctx, endpoint, m, spec)
	if err != nil {
		return nil, err
	}
	return &releasingCall{Call: call, pool: p, conn: conn}, nil
}

// releasingCall returns its connection when the call ends. Release happens
// strictly after Call.Close, so the pool reads the connection's final
// Reusable() state.
type releasingCall struct {
	transport.Call
	pool *Pool
	conn transport.ClientConn
	once sync.Once
}

func (c *releasingCall) Close() error {
	err := c.Call.Close()
	c.once.Do(func() {
		if c.pool != nil && c.conn != nil {
			c.pool.Release(c.conn)
		}
	})
	return err
}

// acquire is Acquire with an optional skip set used by OpenCall Busy fallback
// so the pool switches session instead of re-lending the same busy one.
func (p *Pool) acquire(ctx context.Context, endpoint string, skip map[transport.ClientConn]struct{}) (transport.ClientConn, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, fmt.Errorf("sessionpool: pool closed")
		}
		p.reclaimLocked(time.Now())

		if sess, ok := p.tryLendLocked(endpoint, time.Now(), skip); ok {
			p.mu.Unlock()
			return sess, nil
		}

		b := p.bucketLocked(endpoint)
		atCap := p.cfg.MaxSessionsPerEndpoint > 0 &&
			len(b.entries)+b.pendingDial >= p.cfg.MaxSessionsPerEndpoint

		if p.capPer < 0 {
			if fl, waiting := p.flights[endpoint]; waiting {
				p.mu.Unlock()
				select {
				case <-fl.done:
					if fl.err != nil {
						// Report the dial failure like the flight leader and the
						// Sequential path do. Looping here instead retried with
						// no bound and no backoff, so an endpoint that refuses
						// connections turned one Acquire into a dial storm for
						// as long as the caller's ctx allowed.
						return nil, fl.err
					}
					p.mu.Lock()
					if p.closed {
						p.mu.Unlock()
						return nil, fmt.Errorf("sessionpool: pool closed")
					}
					if sess, ok := p.tryLendLocked(endpoint, time.Now(), skip); ok {
						p.mu.Unlock()
						return sess, nil
					}
					// Session exists but not lendable (e.g. marked bad / skipped) — fall through.
					b2 := p.bucketLocked(endpoint)
					atCap = p.cfg.MaxSessionsPerEndpoint > 0 &&
						len(b2.entries)+b2.pendingDial >= p.cfg.MaxSessionsPerEndpoint
					if atCap {
						p.mu.Unlock()
						return nil, status.ErrSessionsExhausted
					}
					// Need another dial attempt.
					p.mu.Unlock()
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			if atCap {
				p.mu.Unlock()
				return nil, status.ErrSessionsExhausted
			}
			fl := &dialFlight{done: make(chan struct{})}
			p.flights[endpoint] = fl
			b.pendingDial++
			p.mu.Unlock()

			// Deferred: a panic inside dialNew (the axis's own dial and
			// handshake) must not leave the flight registered. Otherwise
			// every later Acquire for this endpoint joins a flight nobody will
			// ever close and blocks until its own ctx expires.
			//
			// The reservation is released by retireFlight below, not here: it
			// has to outlive the dial and cover addEntryLocked too. Dropping
			// pendingDial and the flight first left a window where the session
			// existed but nothing accounted for it, so a concurrent Acquire saw
			// an empty endpoint and dialled a second session past
			// MaxSessionsPerEndpoint.
			var sess transport.ClientConn
			var err error
			retired := false
			retireFlight := func() {
				if retired {
					return
				}
				retired = true
				p.bucketLocked(endpoint).pendingDial--
				delete(p.flights, endpoint)
			}
			func() {
				defer func() {
					if v := recover(); v != nil {
						p.mu.Lock()
						retireFlight()
						fl.err = fmt.Errorf("sessionpool: dial panicked: %v", v)
						close(fl.done)
						p.mu.Unlock()
						panic(v)
					}
				}()
				sess, err = p.dialNew(ctx, endpoint)
			}()
			p.mu.Lock()
			if err != nil {
				poolClosed := p.closed
				retireFlight()
				if poolClosed {
					err = fmt.Errorf("sessionpool: pool closed")
				}
				fl.err = err
				close(fl.done)
				p.mu.Unlock()
				return nil, err
			}
			if p.closed {
				retireFlight()
				fl.err = fmt.Errorf("sessionpool: pool closed")
				close(fl.done)
				p.mu.Unlock()
				_ = sess.Close()
				return nil, fl.err
			}
			e := p.addEntryLocked(endpoint, sess, time.Now())
			e.refcount = 1
			fl.sess = sess
			// Publish the entry before giving up the reservation so the endpoint
			// is never momentarily unaccounted for.
			retireFlight()
			close(fl.done)
			p.mu.Unlock()
			return sess, nil
		}

		// Sequential / one-shot: each waiter dials its own connection.
		if atCap {
			p.mu.Unlock()
			return nil, status.ErrSessionsExhausted
		}
		b.pendingDial++
		p.mu.Unlock()

		var sess transport.ClientConn
		var err error
		func() {
			defer func() {
				if v := recover(); v != nil {
					p.mu.Lock()
					p.bucketLocked(endpoint).pendingDial--
					p.mu.Unlock()
					panic(v)
				}
			}()
			sess, err = p.dialNew(ctx, endpoint)
		}()
		p.mu.Lock()
		b = p.bucketLocked(endpoint)
		b.pendingDial--
		if err != nil {
			poolClosed := p.closed
			p.mu.Unlock()
			if poolClosed {
				err = fmt.Errorf("sessionpool: pool closed")
			}
			return nil, err
		}
		if p.closed {
			p.mu.Unlock()
			_ = sess.Close()
			return nil, fmt.Errorf("sessionpool: pool closed")
		}
		// Capacity may have filled while we dialed; still accept this session
		// because we held a pendingDial reservation.
		e := p.addEntryLocked(endpoint, sess, time.Now())
		e.refcount = 1
		p.mu.Unlock()
		return sess, nil
	}
}

// Release returns a session after Call.Close. It must pair with a successful Acquire.
func (p *Pool) Release(sess transport.ClientConn) {
	if sess == nil {
		return
	}
	p.mu.Lock()
	e := p.bySess[sess]
	if e == nil {
		p.mu.Unlock()
		return
	}
	if e.refcount == 0 {
		// Release must pair with a successful Acquire. A second Release on an
		// already-idle entry used to append it to the idle queue twice, which
		// lets reclaim and lend see the same session under two identities.
		p.mu.Unlock()
		return
	}
	e.refcount--
	now := time.Now()
	p.returnLocked(e, now)
	p.mu.Unlock()
}

// Close closes every tracked session and stops reclaim. Idempotent.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.stopCh)
	var sessions []transport.ClientConn
	for s := range p.bySess {
		sessions = append(sessions, s)
	}
	p.buckets = make(map[string]*bucket)
	p.bySess = make(map[transport.ClientConn]*entry)
	p.mu.Unlock()

	for _, s := range sessions {
		_ = s.Close()
	}
	p.wg.Wait()
	return nil
}

func (p *Pool) dialNew(ctx context.Context, endpoint string) (transport.ClientConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dialCtx := ctx
	var cancel context.CancelFunc
	budget := p.cfg.HandshakeTimeout
	if d := establish.Timeout(ctx); d > 0 {
		budget = d
	}
	if budget > 0 {
		dialCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), budget)
		defer cancel()
		go func() {
			select {
			case <-p.stopCh:
				if cancel != nil {
					cancel()
				}
			case <-dialCtx.Done():
			}
		}()
	}

	return p.dial(dialCtx, endpoint)
}

func (p *Pool) bucketLocked(endpoint string) *bucket {
	b := p.buckets[endpoint]
	if b == nil {
		b = &bucket{}
		p.buckets[endpoint] = b
	}
	return b
}

func (p *Pool) addEntryLocked(endpoint string, sess transport.ClientConn, now time.Time) *entry {
	e := &entry{
		sess:      sess,
		endpoint:  endpoint,
		createdAt: now,
	}
	b := p.bucketLocked(endpoint)
	b.entries = append(b.entries, e)
	p.bySess[sess] = e
	return e
}

func (p *Pool) tryLendLocked(endpoint string, now time.Time, skip map[transport.ClientConn]struct{}) (transport.ClientConn, bool) {
	b := p.buckets[endpoint]
	if b == nil {
		return nil, false
	}
	for _, e := range b.entries {
		if e.sess == nil {
			continue
		}
		if skip != nil {
			if _, ok := skip[e.sess]; ok {
				continue
			}
		}
		if !p.lendableLocked(e, now) {
			continue
		}
		if e.inIdle {
			p.removeIdleLocked(b, e)
		}
		e.refcount++
		return e.sess, true
	}
	return nil, false
}

func (p *Pool) lendableLocked(e *entry, now time.Time) bool {
	if e.sess == nil {
		return false
	}
	if p.lifetimeExpired(e, now) {
		return false
	}
	// Reusable under pool lock, exclusive with reclaim.
	if !e.sess.Reusable() {
		return false
	}
	if p.capPer > 0 && e.refcount >= p.capPer {
		return false
	}
	return true
}

func (p *Pool) lifetimeExpired(e *entry, now time.Time) bool {
	if p.cfg.MaxSessionLifetime <= 0 {
		return false
	}
	return now.Sub(e.createdAt) >= p.cfg.MaxSessionLifetime
}

func (p *Pool) returnLocked(e *entry, now time.Time) {
	reusable := e.sess.Reusable() && !p.lifetimeExpired(e, now)

	if e.refcount > 0 {
		// In-flight remain; Concurrent may keep a non-reusable session until last Release.
		return
	}

	if !reusable {
		p.discardLocked(e)
		return
	}

	b := p.bucketLocked(e.endpoint)
	if p.cfg.MaxIdleSessions >= 0 && len(b.idle) >= p.cfg.MaxIdleSessions {
		p.discardLocked(e)
		return
	}

	e.inIdle = true
	e.idleSince = now
	b.idle = append(b.idle, e)
}

func (p *Pool) discardLocked(e *entry) {
	b := p.buckets[e.endpoint]
	if b != nil {
		p.removeIdleLocked(b, e)
		for i, x := range b.entries {
			if x == e {
				b.entries = append(b.entries[:i], b.entries[i+1:]...)
				break
			}
		}
	}
	delete(p.bySess, e.sess)
	sess := e.sess
	e.sess = nil
	if sess != nil {
		// Close outside would be nicer, but Close is expected fast/idempotent.
		_ = sess.Close()
	}
}

func (p *Pool) removeIdleLocked(b *bucket, e *entry) {
	if !e.inIdle {
		return
	}
	e.inIdle = false
	for i, x := range b.idle {
		if x == e {
			b.idle = append(b.idle[:i], b.idle[i+1:]...)
			break
		}
	}
}

func (p *Pool) reclaimLocked(now time.Time) {
	for _, b := range p.buckets {
		// Copy idle slice: discard mutates it.
		idle := append([]*entry(nil), b.idle...)
		for _, e := range idle {
			if e.refcount > 0 {
				continue
			}
			expired := p.lifetimeExpired(e, now)
			idleTO := false
			if p.cfg.SessionIdleTimeout > 0 && e.inIdle {
				idleTO = now.Sub(e.idleSince) >= p.cfg.SessionIdleTimeout
			}
			if expired || idleTO || !e.sess.Reusable() {
				p.discardLocked(e)
			}
		}
		// Also drop non-idle, zero-refcount entries that are dead (should not happen).
		entries := append([]*entry(nil), b.entries...)
		for _, e := range entries {
			if e.refcount == 0 && !e.inIdle {
				if p.lifetimeExpired(e, now) || (e.sess != nil && !e.sess.Reusable()) {
					p.discardLocked(e)
				}
			}
		}
	}
}

func (p *Pool) reclaimLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case now := <-ticker.C:
			p.mu.Lock()
			if !p.closed {
				p.reclaimLocked(now)
			}
			p.mu.Unlock()
		}
	}
}
