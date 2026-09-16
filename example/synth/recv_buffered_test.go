package synth

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
)

// TestRecvKeepsBufferedResponseWhenLoopAlreadyFinished drives Recv only after
// the whole response is buffered and recvLoop has closed done — the window
// where a select over both the inbox and done drops the response.
func TestRecvKeepsBufferedResponseWhenLoopAlreadyFinished(t *testing.T) {
	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			var in []byte
			if err := st.Recv(&in); err != nil {
				return err
			}
			_ = st.Recv(&in)
			return st.Send([]byte("ping-ok"))
		},
		"Echo": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			var in []byte
			if err := st.Recv(&in); err != nil {
				return err
			}
			_ = st.Recv(&in)
			return st.Send(in)
		},
		"Exclusive": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			return nil
		},
	}
	addr, _, _, _ := startSynthServer(t, handlers)
	cli := newSynthClient(t, addr, ClientTCP())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ping := descriptor.MustMethod(MethodPing, descriptor.Unary)
	const rounds = 20
	for i := 0; i < rounds; i++ {
		cs, err := cli.Open(ctx, ping)
		if err != nil {
			t.Fatalf("round %d: Open: %v", i, err)
		}
		if err := cs.Send([]byte("x")); err != nil {
			t.Fatalf("round %d: Send: %v", i, err)
		}
		if err := cs.HalfClose(); err != nil {
			t.Fatalf("round %d: HalfClose: %v", i, err)
		}
		// Let recvLoop buffer DATA+STATUS and exit before the caller reads.
		time.Sleep(25 * time.Millisecond)
		var out []byte
		err = cs.Recv(&out)
		if errors.Is(err, io.EOF) {
			t.Fatalf("round %d: first Recv returned io.EOF; the buffered response was dropped", i)
		}
		if err != nil {
			t.Fatalf("round %d: Recv: %v", i, err)
		}
		if string(out) != "ping-ok" {
			t.Fatalf("round %d: out = %q, want %q", i, out, "ping-ok")
		}
		_ = cs.Close()
	}
}

// TestRecvKeepsMessageDeliveredWhileEntering covers the window the test above
// cannot reach: Recv finds an empty inbox, and recvLoop pushes its last items
// and closes done before Recv reaches the select over both. Both cases are
// then ready, Go picks at random, and the delivered message is lost half the
// time. It showed up as a responder reporting io.EOF from its first Recv,
// which the initiator then saw as a STATUS carrying "EOF".
func TestRecvKeepsMessageDeliveredWhileEntering(t *testing.T) {
	const rounds = 4000
	lost := 0
	for i := 0; i < rounds; i++ {
		c := &call{
			recvCh: make(chan recvItem, 8),
			done:   make(chan struct{}),
		}
		c.ctx, c.cancel = context.WithCancel(context.Background())

		start := make(chan struct{})
		type result struct {
			payload []byte
			err     error
		}
		got := make(chan result, 1)
		go func() {
			<-start
			p, _, err := c.Recv()
			got <- result{p, err}
		}()
		// What recvLoop does for a one-message response: deliver the payload,
		// deliver the terminal, exit. Racing it against Recv's entry is what
		// puts both select cases in the ready state.
		go func() {
			<-start
			c.recvCh <- recvItem{payload: []byte("ping-ok")}
			c.recvCh <- recvItem{err: io.EOF}
			close(c.done)
		}()
		close(start)

		if r := <-got; r.err != nil || string(r.payload) != "ping-ok" {
			lost++
		}
		c.cancel()
	}
	if lost > 0 {
		t.Fatalf("%d/%d Recv calls dropped a message recvLoop had delivered", lost, rounds)
	}
}

// TestCloseWaitsInsteadOfAbortingAfterPeerEnd pins the other half of the same
// window. Close ran while recvLoop had delivered the peer's end but had not
// closed done yet, read that as a reader parked in a socket read, and aborted
// the carrier. On the responder side that killed a connection the initiator's
// pool was about to reuse, so the next OpenCall failed with EPIPE.
func TestCloseWaitsInsteadOfAbortingAfterPeerEnd(t *testing.T) {
	car := &abortCounter{aborted: make(chan struct{})}
	c := &call{
		session: &session{carrier: car},
		client:  false,
		recvCh:  make(chan recvItem, 8),
		done:    make(chan struct{}),
		// recvLoop read END and is between that and close(done).
		peerHalfClosed: true,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()

	select {
	case <-closed:
		t.Fatal("Close returned before recvLoop exited")
	case <-time.After(50 * time.Millisecond):
	}
	if n := car.calls.Load(); n != 0 {
		t.Fatalf("Close aborted the carrier %d times; the peer had already ended its stream", n)
	}

	close(c.done)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return after recvLoop exited")
	}
	if n := car.calls.Load(); n != 0 {
		t.Fatalf("Close aborted the carrier %d times", n)
	}
}

// A call whose peer still owes bytes must still be unblocked by Abort, or
// Close would wait for a reader parked in a socket read that nothing wakes.
func TestCloseAbortsWhenPeerStillOwesBytes(t *testing.T) {
	car := &abortCounter{aborted: make(chan struct{})}
	c := &call{
		session: &session{carrier: car},
		client:  true,
		recvCh:  make(chan recvItem, 8),
		done:    make(chan struct{}),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	// Stand in for recvLoop: only Abort gets it out of its read.
	go func() {
		<-car.aborted
		close(c.done)
	}()

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := car.calls.Load(); n != 1 {
		t.Fatalf("carrier aborted %d times, want 1", n)
	}
	if c.session.Reusable() {
		t.Error("session still reusable after a call closed without a terminal")
	}
}

// abortCounter is a ByteStreamCarrier that only records Abort: Close is the
// only path under test and it performs no I/O.
type abortCounter struct {
	calls   atomic.Int32
	aborted chan struct{}
	once    sync.Once
}

func (a *abortCounter) Abort() error {
	a.calls.Add(1)
	a.once.Do(func() { close(a.aborted) })
	return nil
}

func (a *abortCounter) Read([]byte) (int, error)    { return 0, io.EOF }
func (a *abortCounter) Write(b []byte) (int, error) { return len(b), nil }
