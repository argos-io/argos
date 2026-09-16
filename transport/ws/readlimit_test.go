package ws_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/ws"
)

// coder/websocket caps inbound messages at 32 KiB unless the transport raises
// the limit; the framework's default frame is 4 MiB, so the cap silently kills
// any larger call.
func TestMessageLargerThanReadLimitIsDelivered(t *testing.T) {
	_, addr := startEchoServer(t)

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

	msg := make([]byte, 128<<10)
	for i := range msg {
		msg[i] = byte(i)
	}

	if err := car.SendMessage(msg); err != nil {
		t.Fatalf("SendMessage(%d bytes): %v", len(msg), err)
	}
	got, err := car.RecvMessage()
	if err != nil {
		t.Fatalf("RecvMessage(%d bytes): %v (close status %v)", len(msg), err, websocket.CloseStatus(err))
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo of %d bytes came back as %d bytes", len(msg), len(got))
	}

	// An oversized message must not have poisoned the connection.
	if err := car.SendMessage([]byte("still-alive")); err != nil {
		t.Fatalf("SendMessage after oversized message: %v", err)
	}
	got2, err := car.RecvMessage()
	if err != nil {
		t.Fatalf("RecvMessage after oversized message: %v", err)
	}
	if string(got2) != "still-alive" {
		t.Fatalf("echo = %q, want still-alive", got2)
	}
}

// startLimitedServer reports the error the server-side read of the first
// message produced, then closes the connection.
func startLimitedServer(t *testing.T, limit int64, recvErr chan<- error) string {
	t.Helper()
	raw := ws.New(ws.WithMaxReadBytes(limit))
	tr := raw.(*ws.Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			car := c.(transport.CarrierConn).Carrier().(transport.MessageCarrier)
			_, err := car.RecvMessage()
			recvErr <- err
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

func TestWithMaxReadBytesAppliesToAccept(t *testing.T) {
	const limit = 1024
	recvErr := make(chan error, 1)
	addr := startLimitedServer(t, limit, recvErr)

	clientTr := ws.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	car := conn.(transport.CarrierConn).Carrier().(transport.MessageCarrier)
	if err := car.SendMessage(make([]byte, 4*limit)); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	select {
	case err := <-recvErr:
		if !errors.Is(err, websocket.ErrMessageTooBig) {
			t.Fatalf("server read error = %v, want ErrMessageTooBig", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not reject the oversized message")
	}
}

func TestWithMaxReadBytesAppliesToDial(t *testing.T) {
	const limit = 1024
	_, addr := startEchoServer(t)

	clientTr := ws.New(ws.WithMaxReadBytes(limit))
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	car := conn.(transport.CarrierConn).Carrier().(transport.MessageCarrier)
	if err := car.SendMessage(make([]byte, 4*limit)); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, err := car.RecvMessage(); !errors.Is(err, websocket.ErrMessageTooBig) {
		t.Fatalf("client read error = %v, want ErrMessageTooBig", err)
	}
}
