package server

import (
	"sync"

	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/status"
)

// admitGate is the server-wide call admission gate: MaxConcurrentCalls slots
// and a MaxBufferedBytes pool carved all-or-nothing per call (§2.6).
type admitGate struct {
	slots       chan struct{}
	perCall     int64
	maxBuffered int64

	mu       sync.Mutex
	buffered int64
	closed   bool // Server.Shutdown: reject further admits
}

func newAdmitGate(maxCalls int, maxBuffered, perCall int64) *admitGate {
	if maxCalls < 1 {
		maxCalls = 1
	}
	return &admitGate{
		slots:       make(chan struct{}, maxCalls),
		perCall:     perCall,
		maxBuffered: maxBuffered,
	}
}

// tryAdmit reserves one call slot and perCall bytes. On success it returns
// a Budget for the call ctx and a release that returns both.
func (g *admitGate) tryAdmit() (b budget.Budget, release func(), err error) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, nil, status.ErrCallsExhausted
	}
	if g.buffered+g.perCall > g.maxBuffered {
		g.mu.Unlock()
		return nil, nil, status.ErrCallsExhausted
	}
	g.mu.Unlock()

	select {
	case g.slots <- struct{}{}:
	default:
		return nil, nil, status.ErrCallsExhausted
	}

	g.mu.Lock()
	if g.closed || g.buffered+g.perCall > g.maxBuffered {
		g.mu.Unlock()
		<-g.slots
		return nil, nil, status.ErrCallsExhausted
	}
	g.buffered += g.perCall
	g.mu.Unlock()

	b = budget.New(g.perCall)
	var once sync.Once
	release = func() {
		once.Do(func() {
			g.mu.Lock()
			g.buffered -= g.perCall
			if g.buffered < 0 {
				g.buffered = 0
			}
			g.mu.Unlock()
			<-g.slots
		})
	}
	return b, release, nil
}

func (g *admitGate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}
