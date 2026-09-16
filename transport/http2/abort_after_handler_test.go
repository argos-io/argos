package http2_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

// Closing a server Conn after its handler returned must not touch the retired
// response writer: net/http's HTTP/2 server panics with "WriteHeader called
// after Handler finished", which takes the whole process down.
func TestServerConnCloseAfterHandlerFinishedDoesNotPanic(t *testing.T) {
	connCh := make(chan transport.Conn, 1)

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		// Return without writing a response: net/http sends an implicit 200
		// when the handler returns, after which the h2 writer is retired.
		connCh <- c
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/no-response"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := car.(transport.ByteStreamCarrier).Write([]byte{0}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := car.(transport.SendCloser).CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	status, err := car.(transport.ResponseHeaderReader).ResponseStatus()
	if err != nil {
		t.Fatalf("ResponseStatus: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want the implicit 200", status)
	}
	if _, err := io.ReadAll(car.(transport.ByteStreamCarrier)); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	serverConn := <-connCh

	// The response is on the wire, so the handler has returned; give the h2
	// stack a moment to retire its response writer before the late Close.
	time.Sleep(200 * time.Millisecond)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Close after handler finished panicked: %v", r)
		}
	}()
	if err := serverConn.Close(); err != nil {
		t.Fatalf("Close after handler finished: %v", err)
	}
}

// Aborting a request whose handler is still running still records the deadline
// status: the peer sees the exchange as canceled once the handler returns.
func TestServerConnCloseWhileHandlerRunsWritesDeadlineStatus(t *testing.T) {
	connCh := make(chan transport.Conn, 1)
	release := make(chan struct{})

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		connCh <- c
		<-release
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/blocked"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := car.(transport.ByteStreamCarrier).Write([]byte{0}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	serverConn := <-connCh
	if err := serverConn.Close(); err != nil {
		t.Fatalf("Close during handler: %v", err)
	}
	close(release)

	status, err := car.(transport.ResponseHeaderReader).ResponseStatus()
	if err != nil {
		t.Fatalf("ResponseStatus: %v", err)
	}
	if status != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want %d", status, http.StatusRequestTimeout)
	}
}
