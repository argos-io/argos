package http2

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

// A Serve that returns on its own must still close serveDone: Shutdown waits on
// it, and a channel that never closes stalls that wait until the deadline
// (forever under a non-cancelable context).
func TestServeReturningOnContextCancelClosesServeDone(t *testing.T) {
	tr := New().(*Transport)
	defer func() { _ = tr.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(context.Context, transport.Conn) {},
			transport.WithListenAddress("127.0.0.1:0"))
	}()

	// Addr is published under the same lock as serveDone, so a non-nil Addr
	// means Serve has taken ownership and set serveDone.
	deadline := time.Now().Add(2 * time.Second)
	for tr.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("listener Addr not set within timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after its context was canceled")
	}

	done := make(chan error, 1)
	go func() { done <- tr.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown after Serve returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown waited on a serveDone that was never closed")
	}
}
