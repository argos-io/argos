package ws_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/ws"
)

// startHangupServer upgrades each connection and closes it at once: the peer
// stops accepting messages without this side closing anything.
func startHangupServer(t *testing.T) (addr string) {
	t.Helper()
	raw := ws.New()
	tr := raw.(*ws.Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			_ = c.Close()
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()

	a := waitAddr(t, tr)
	t.Cleanup(func() {
		cancel()
		_ = tr.Close()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not return after Close")
		}
	})
	return a.String()
}

// A failed message send does not prove the peer stopped sending: the response
// may still arrive, so the failure must be a transport.SendError with
// ReceiveOpen() true, letting framing keep the carrier readable.
func TestSendFailureOnHangupKeepsReceiveOpen(t *testing.T) {
	addr := startHangupServer(t)

	clientTr := ws.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	car := conn.(transport.CarrierConn).Carrier().(transport.MessageCarrier)

	// The peer's hangup reaches this side asynchronously; a closed socket may
	// swallow one write before failing.
	var sendErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sendErr = car.SendMessage([]byte("ping")); sendErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sendErr == nil {
		t.Fatal("SendMessage never failed after the peer hung up")
	}
	var se transport.SendError
	if !errors.As(sendErr, &se) {
		t.Fatalf("SendMessage error %v (%T) is not a transport.SendError", sendErr, sendErr)
	}
	if !se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = false; a send failure alone does not prove the receive direction is dead")
	}
}

// Closing this Conn is different: the receive direction is then known to be
// unrecoverable, so the classification must say so.
func TestSendFailureAfterCloseReportsReceiveClosed(t *testing.T) {
	_, addr := startEchoServer(t)

	clientTr := ws.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	car := conn.(transport.CarrierConn).Carrier().(transport.MessageCarrier)
	_ = conn.Close()

	sendErr := car.SendMessage([]byte("ping"))
	if sendErr == nil {
		t.Fatal("SendMessage succeeded on a closed Conn")
	}
	var se transport.SendError
	if !errors.As(sendErr, &se) {
		t.Fatalf("SendMessage error %v (%T) is not a transport.SendError", sendErr, sendErr)
	}
	if se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = true on a closed Conn")
	}
}
