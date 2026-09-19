package stream

import (
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

// gcCall records Close from any goroutine: a cleanup runs on its own.
type gcCall struct{ closes atomic.Int64 }

func (c *gcCall) Method() string                { return "svc.Method" }
func (c *gcCall) Deadline() (time.Time, bool)   { return time.Time{}, false }
func (c *gcCall) SendHeaders() error            { return nil }
func (c *gcCall) Finish(error) error            { return nil }
func (c *gcCall) Recv() ([]byte, func(), error) { return nil, nil, io.EOF }
func (c *gcCall) Send([]byte) error             { return nil }
func (c *gcCall) HalfClose() error              { return nil }
func (c *gcCall) Close() error                  { c.closes.Add(1); return nil }

var _ transport.Call = (*gcCall)(nil)

// wrapAndDrop builds a Stream and lets it go out of scope. noinline so the
// stream is genuinely unreachable when this returns.
//
//go:noinline
func wrapAndDrop(c transport.Call) {
	st := Wrap(c, bytesCodec{})
	if st == nil {
		panic("stream: Wrap returned nil")
	}
}

func waitCloses(t *testing.T, call *gcCall, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if call.closes.Load() >= want {
			return
		}
		runtime.GC()
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Close calls = %d, want %d: an unreachable Stream must release its Call", call.closes.Load(), want)
}

// TestWrapClosesCallWhenStreamBecomesUnreachable: a Stream is what holds a
// Call, so a caller that drops its Stream without closing it leaves a call
// nobody can reach and nobody has released — the connection stays borrowed
// from the axis forever. Wrap reclaims it.
func TestWrapClosesCallWhenStreamBecomesUnreachable(t *testing.T) {
	call := &gcCall{}
	wrapAndDrop(call)
	waitCloses(t, call, 1)
}

// TestWrapKeepsCallOpenWhileStreamIsHeld is the other half of the contract:
// while any holder still has the Stream, the Call must stay open. Closing it
// there would cut a live exchange out from under a filter that kept the stream.
func TestWrapKeepsCallOpenWhileStreamIsHeld(t *testing.T) {
	call := &gcCall{}
	st := Wrap(call, bytesCodec{})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	if got := call.closes.Load(); got != 0 {
		t.Fatalf("Close calls = %d while the Stream is held, want 0", got)
	}
	// Keep the Stream reachable across every GC above.
	runtime.KeepAlive(st)
}
