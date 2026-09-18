package client

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/status"
)

// A leaked CallStream used to hold its admission reservation for the life of
// the Client: reporting the leak was all the cleanup hook did. Later Opens then
// hit ErrCallsExhausted with no call in flight.
func TestCallStreamLeakReturnsAdmission(t *testing.T) {
	if raceEnabled {
		t.Skip("CallStream leak GC observation is flaky under the race detector")
	}

	const slots = 2
	cli, err := newClientLoopback(t, freshLoopback(t, nil, nil),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(slots),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Consume every admission slot and drop the streams without Close.
	func() {
		for i := 0; i < slots; i++ {
			cs, err := cli.Open(context.Background(), testMethod(t))
			if err != nil {
				t.Fatalf("Open #%d: %v", i, err)
			}
			_ = cs // intentionally leaked
		}
	}()

	// Admission is exhausted until the cleanup hooks run.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		cs, err := cli.Open(context.Background(), testMethod(t))
		if err == nil {
			_ = cs.Close()
			return // admission was reclaimed
		}
		if !errors.Is(err, status.ErrCallsExhausted) {
			t.Fatalf("Open: %v, want ErrCallsExhausted while the slots are held", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("admission never came back: a leaked CallStream holds its slot forever")
}

// Recv seeing a cancelled call ctx used to mark headers ready, which let a
// waiter in Header() take the headers branch and return (stale metadata, nil)
// for a call that was already dead. Header() must report the cancellation.
func TestHeaderReportsCancellationAfterCancelledRecv(t *testing.T) {
	t.Parallel()
	cli, err := newClientLoopback(t, freshLoopback(t, nil, nil),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cs, err := cli.Open(ctx, testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = cs.Close() }()

	cancel()
	// Give the call ctx time to propagate, then let Recv observe it first.
	time.Sleep(50 * time.Millisecond)
	var out []byte
	if err := cs.Recv(&out); err == nil {
		t.Fatal("Recv on a cancelled call succeeded")
	}

	_, herr := cs.Header()
	if herr == nil {
		t.Fatal("Header() reported success for a cancelled call")
	}
	if !errors.Is(herr, context.Canceled) {
		t.Fatalf("Header() = %v, want context.Canceled", herr)
	}
}
