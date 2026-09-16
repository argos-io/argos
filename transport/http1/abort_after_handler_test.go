package http1_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

func TestServerConnCloseAfterHandlerFinishedDoesNotPanic(t *testing.T) {
	connCh := make(chan transport.Conn, 1)

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
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
