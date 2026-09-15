// Package sessionpool is the client-side session pool.
//
// # Acquire / Release contract
//
// Acquire returns a framing.ClientSession with the pool's in-flight refcount
// already incremented by one (a capacity slot pretaken for the forthcoming
// OpenCall). The caller must OpenCall on that session, then after Call.Close
// returns call Release exactly once for that Acquire.
//
// OpenCall combines Acquire + sess.OpenCall with Busy/Spent fallback: on
// framing.ErrSessionBusy it switches session (retry ≤ MaxSessionsPerEndpoint)
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
// wait is Concurrent cold-start singleflight for Dial+NewClientSession.
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
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

const (
	defaultMaxSessionsPerEndpoint = 64
	defaultMaxIdleSessions        = 8
	defaultHandshakeTimeout       = 10 * time.Second
)

// Config holds instance-level pool limits.
type Config struct {
	MaxSessionsPerEndpoint int
	MaxIdleSessions        int // 0 = keep no idle sessions
	SessionIdleTimeout     time.Duration
	MaxSessionLifetime      time.Duration
	HandshakeTimeout       time.Duration // dial + NewClientSession
	SessionSpec            framing.SessionSpec
}

// DialFunc dials one transport.Conn for endpoint. The pool owns the Conn
// until NewClientSession succeeds (Session then owns it) or fails (pool closes it).
type DialFunc func(ctx context.Context, endpoint string) (transport.Conn, error)

// Pool is a concrete endpoint-keyed session pool.
type Pool struct {
	framing framing.Framing
	dial    DialFunc
	cfg     Config
	reuse   framing.ReuseModel
	capPer  int // 1 for Sequential/OneCall; -1 for Concurrent (unlimited)

	mu      sync.Mutex
	closed  bool
	buckets map[string]*bucket
	bySess  map[framing.ClientSession]*entry
	flights map[string]*dialFlight // Concurrent cold-start only

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type bucket struct {
	entries []*entry
	idle    []*entry
	// pendingDial counts in-flight Dial+NewClientSession reservations
	// toward MaxSessionsPerEndpoint (Sequential / OneCallPerConn).
	pendingDial int
}

type entry struct {
	sess      framing.ClientSession
	endpoint  string
	refcount  int
	createdAt time.Time
	idleSince time.Time
	inIdle    bool
}

type dialFlight struct {
	done chan struct{}
	sess framing.ClientSession
	err  error
}

// New builds a pool. Zero/negative MaxSessionsPerEndpoint and HandshakeTimeout
// take §6.1 defaults; MaxIdleSessions < 0 defaults to 8 (0 means no idle keep).
func New(f framing.Framing, dial DialFunc, cfg Config) *Pool {
	if cfg.MaxSessionsPerEndpoint <= 0 {
		cfg.MaxSessionsPerEndpoint = defaultMaxSessionsPerEndpoint
	}
	if cfg.MaxIdleSessions < 0 {
		cfg.MaxIdleSessions = defaultMaxIdleSessions
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultHandshakeTimeout
	}
	reuse := f.Reuse()
	capPer := 1
	if reuse == framing.Concurrent {
		capPer = -1
	}
	p := &Pool{
		framing: f,
		dial:    dial,
		cfg:     cfg,
		reuse:   reuse,
		capPer:  capPer,
		buckets: make(map[string]*bucket),
		bySess:  make(map[framing.ClientSession]*entry),
		flights: make(map[string]*dialFlight),
		stopCh:  make(chan struct{}),
	}
	p.wg.Add(1)
	go p.reclaimLoop()
	return p
}

// Acquire pretakes one in-flight slot on a reusable session, or dials a new
// one. It does not call OpenCall.
func (p *Pool) Acquire(ctx context.Context, endpoint string) (framing.ClientSession, error) {
	return p.acquire(ctx, endpoint, nil)
}

// OpenCall acquires a session, calls sess.OpenCall, and on framing.ErrSessionBusy
// releases the pretaken slot, skips that session for this attempt chain, and
// retries with another. Retry count is at most MaxSessionsPerEndpoint.
//
// On exhaust it returns status.ErrSessionsExhausted (Code ResourceExhausted).
// framing.ErrSessionBusy and framing.ErrSessionSpent never leak to the caller.
//
// On framing.ErrSessionSpent the session is closed/discarded and another is tried.
//
// On success the caller must Release(sess) after Call.Close returns (same
// contract as Acquire).
func (p *Pool) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec framing.CallSpec) (framing.Call, framing.ClientSession, error) {
	max := p.cfg.MaxSessionsPerEndpoint
	skip := make(map[framing.ClientSession]struct{})

	for attempt := 0; attempt < max; attempt++ {
		sess, err := p.acquire(ctx, endpoint, skip)
		if err != nil {
			return nil, nil, err
		}
		call, err := sess.OpenCall(ctx, m, spec)
		if err == nil {
			return call, sess, nil
		}
		if errors.Is(err, framing.ErrSessionBusy) {
			skip[sess] = struct{}{}
			p.Release(sess)
			continue
		}
		if errors.Is(err, framing.ErrSessionSpent) {
			_ = sess.Close()
			p.Release(sess)
			continue
		}
		p.Release(sess)
		return nil, nil, err
	}
	return nil, nil, status.ErrSessionsExhausted
}

// acquire is Acquire with an optional skip set used by OpenCall Busy fallback
// so the pool switches session instead of re-lending the same busy one.
func (p *Pool) acquire(ctx context.Context, endpoint string, skip map[framing.ClientSession]struct{}) (framing.ClientSession, error) {
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
		atCap := len(b.entries)+b.pendingDial >= p.cfg.MaxSessionsPerEndpoint

		if p.reuse == framing.Concurrent {
			if fl, waiting := p.flights[endpoint]; waiting {
				p.mu.Unlock()
				select {
				case <-fl.done:
					if fl.err != nil {
						// Dial failed; retry loop may dial again or exhaust.
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						default:
							continue
						}
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
					atCap = len(p.bucketLocked(endpoint).entries)+p.bucketLocked(endpoint).pendingDial >= p.cfg.MaxSessionsPerEndpoint
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

			sess, err := p.dialNew(endpoint)
			p.mu.Lock()
			b = p.bucketLocked(endpoint)
			b.pendingDial--
			delete(p.flights, endpoint)
			if err != nil {
				fl.err = err
				close(fl.done)
				p.mu.Unlock()
				return nil, err
			}
			e := p.addEntryLocked(endpoint, sess, time.Now())
			e.refcount = 1
			fl.sess = sess
			close(fl.done)
			p.mu.Unlock()
			return sess, nil
		}

		// Sequential / OneCallPerConn: each waiter dials its own session.
		if atCap {
			p.mu.Unlock()
			return nil, status.ErrSessionsExhausted
		}
		b.pendingDial++
		p.mu.Unlock()

		sess, err := p.dialNew(endpoint)
		p.mu.Lock()
		b = p.bucketLocked(endpoint)
		b.pendingDial--
		if err != nil {
			p.mu.Unlock()
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
func (p *Pool) Release(sess framing.ClientSession) {
	if sess == nil {
		return
	}
	p.mu.Lock()
	e := p.bySess[sess]
	if e == nil {
		p.mu.Unlock()
		return
	}
	if e.refcount > 0 {
		e.refcount--
	}
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
	var sessions []framing.ClientSession
	for s := range p.bySess {
		sessions = append(sessions, s)
	}
	p.buckets = make(map[string]*bucket)
	p.bySess = make(map[framing.ClientSession]*entry)
	p.mu.Unlock()

	for _, s := range sessions {
		_ = s.Close()
	}
	p.wg.Wait()
	return nil
}

func (p *Pool) dialNew(endpoint string) (framing.ClientSession, error) {
	hsCtx, cancel := context.WithTimeout(context.Background(), p.cfg.HandshakeTimeout)
	defer cancel()

	conn, err := p.dial(hsCtx, endpoint)
	if err != nil {
		return nil, err
	}
	sess, err := p.framing.NewClientSession(hsCtx, conn, p.cfg.SessionSpec)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return sess, nil
}

func (p *Pool) bucketLocked(endpoint string) *bucket {
	b := p.buckets[endpoint]
	if b == nil {
		b = &bucket{}
		p.buckets[endpoint] = b
	}
	return b
}

func (p *Pool) addEntryLocked(endpoint string, sess framing.ClientSession, now time.Time) *entry {
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

func (p *Pool) tryLendLocked(endpoint string, now time.Time, skip map[framing.ClientSession]struct{}) (framing.ClientSession, bool) {
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
	if len(b.idle) >= p.cfg.MaxIdleSessions {
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
