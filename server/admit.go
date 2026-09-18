package server

import (
	"sync"

	"github.com/argos-io/argos/status"
)

// admitGate is the server-wide in-flight call limit.
type admitGate struct {
	slots     chan struct{}
	unlimited bool
	closed    bool
	mu        sync.Mutex
}

func newAdmitGate(maxCalls int) *admitGate {
	if maxCalls <= 0 {
		return &admitGate{unlimited: true}
	}
	return &admitGate{
		slots: make(chan struct{}, maxCalls),
	}
}

// tryAdmit reserves one call slot. On success release must run exactly once.
func (g *admitGate) tryAdmit() (release func(), err error) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, status.ErrCallsExhausted
	}
	g.mu.Unlock()

	if g.unlimited {
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return nil, status.ErrCallsExhausted
		}
		g.mu.Unlock()
		var once sync.Once
		return func() { once.Do(func() {}) }, nil
	}

	select {
	case g.slots <- struct{}{}:
	default:
		return nil, status.ErrCallsExhausted
	}

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		<-g.slots
		return nil, status.ErrCallsExhausted
	}
	g.mu.Unlock()

	var once sync.Once
	release = func() {
		once.Do(func() { <-g.slots })
	}
	return release, nil
}

func (g *admitGate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}
