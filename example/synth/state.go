package synth

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type ctxKey struct{}

// ConnState is connection-scoped protocol state. On the server, the call ctx
// is derived from the connection ctx, so handlers read this via FromContext.
//
// When the composition layer has placed a *ConnState on the handshake ctx
// (ContextWith), ServerConn.Handshake fills it in place. Otherwise the
// session installs a Sequential same-goroutine handoff so FromContext still
// works through server.Server without a core API change.
type ConnState struct {
	mu sync.Mutex

	greeting  string
	exclusive bool

	onExclusive func()
}

// Greeting returns the greeting string sent by the server during handshake.
func (s *ConnState) Greeting() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.greeting
}

// Exclusive reports whether this connection has entered exclusive mode.
func (s *ConnState) Exclusive() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exclusive
}

// EnterExclusive marks the connection exclusive. On the client session this
// makes Reusable() false so the pool will not return the connection.
func (s *ConnState) EnterExclusive() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.exclusive = true
	cb := s.onExclusive
	s.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func (s *ConnState) setGreeting(g string) {
	s.mu.Lock()
	s.greeting = g
	s.mu.Unlock()
}

func (s *ConnState) setOnExclusive(cb func()) {
	s.mu.Lock()
	s.onExclusive = cb
	s.mu.Unlock()
}

// ContextWith attaches st to ctx. Prefer placing an empty *ConnState on the
// handshake ctx before ServerConn.Handshake so handshake can fill it in place.
func ContextWith(ctx context.Context, st *ConnState) context.Context {
	return context.WithValue(ctx, ctxKey{}, st)
}

// FromContext returns the ConnState for this connection, if any.
func FromContext(ctx context.Context) (*ConnState, bool) {
	if st, ok := ctx.Value(ctxKey{}).(*ConnState); ok && st != nil {
		return st, true
	}
	if v, ok := handoff.Load(goroutineID()); ok {
		return v.(*ConnState), true
	}
	return nil, false
}

// handoff bridges ConnState into server handlers when the composition layer
// did not place a value on the connection ctx. Valid only while the accept
// goroutine (Sequential) is the one that created the session.
var handoff sync.Map // goid uint64 → *ConnState

func installHandoff(goid uint64, st *ConnState) {
	handoff.Store(goid, st)
}

func clearHandoff(goid uint64) {
	handoff.Delete(goid)
}

func goroutineID() uint64 {
	var buf [32]byte
	n := runtime.Stack(buf[:], false)
	// "goroutine 123 ["
	s := string(buf[:n])
	s = strings.TrimPrefix(s, "goroutine ")
	if i := strings.IndexByte(s, ' '); i > 0 {
		s = s[:i]
	}
	id, _ := strconv.ParseUint(s, 10, 64)
	return id
}
