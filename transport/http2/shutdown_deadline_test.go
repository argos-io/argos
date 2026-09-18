package http2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

// Shutdown promises to interrupt unfinished connections on its deadline. A
// callback that ignores Conn.Close must not hold it open past that deadline.
func TestShutdownBoundedByDeadlineWhenCallbackBlocks(t *testing.T) {
	started := make(chan struct{})
	hold := make(chan struct{})

	raw := New()
	tr := raw.(*Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(context.Context, transport.Conn) {
			close(started)
			<-hold // ignores Conn.Close on purpose
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	defer func() {
		close(hold)
		cancel()
		_ = tr.Close()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not return after Close")
		}
	}()

	addr := waitAddr(t, tr).String()

	clientTr := New()
	defer clientTr.Close()
	conn, err := clientTr.Dial(context.Background(), transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	sc, ok := conn.(transport.StreamConn)
	if !ok {
		t.Fatal("Dial did not return StreamConn")
	}
	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer openCancel()
	car, err := sc.OpenStream(openCtx, transport.RequestPreface{RequestTarget: "/block"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := car.(transport.ByteStreamCarrier).Write([]byte{0}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("onConn did not start")
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer shutCancel()
	shutErr := make(chan error, 1)
	start := time.Now()
	go func() { shutErr <- tr.Shutdown(shutCtx) }()

	select {
	case err := <-shutErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown returned %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("Shutdown returned after %v; must be bounded by its 100ms deadline", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return within 3s despite its 100ms deadline")
	}
}
