package ws_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/ws"
)

func waitAddr(t *testing.T, tr *ws.Transport) net.Addr {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a := tr.Addr(); a != nil {
			return a
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener Addr not set within timeout")
	return nil
}

func startEchoServer(t *testing.T) (tr *ws.Transport, addr string) {
	t.Helper()
	raw := ws.New()
	tr = raw.(*ws.Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			cc := c.(transport.CarrierConn)
			car := cc.Carrier().(transport.MessageCarrier)
			for {
				msg, err := car.RecvMessage()
				if err != nil {
					_ = c.Close()
					return
				}
				if err := car.SendMessage(msg); err != nil {
					_ = c.Close()
					return
				}
			}
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
	return tr, a.String()
}

func TestDialServeEcho(t *testing.T) {
	_, addr := startEchoServer(t)

	clientTr := ws.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	cc := conn.(transport.CarrierConn)
	car := cc.Carrier().(transport.MessageCarrier)

	msg := []byte("hello-ws-echo")
	if err := car.SendMessage(msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got, err := car.RecvMessage()
	if err != nil {
		t.Fatalf("RecvMessage: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("echo = %q, want %q", got, msg)
	}

	msg2 := []byte("round-trip-2")
	if err := car.SendMessage(msg2); err != nil {
		t.Fatalf("SendMessage2: %v", err)
	}
	got2, err := car.RecvMessage()
	if err != nil {
		t.Fatalf("RecvMessage2: %v", err)
	}
	if string(got2) != string(msg2) {
		t.Fatalf("echo2 = %q, want %q", got2, msg2)
	}
}

func TestAddrAfterServe(t *testing.T) {
	raw := ws.New()
	tr := raw.(*ws.Transport)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = tr.Serve(ctx, func(context.Context, transport.Conn) {},
			transport.WithListenAddress("127.0.0.1:0"))
	}()
	defer func() {
		cancel()
		_ = tr.Close()
	}()

	a := waitAddr(t, tr)
	host, port, err := net.SplitHostPort(a.String())
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", a.String(), err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("host = %q, want 127.0.0.1", host)
	}
	if port == "0" || port == "" {
		t.Fatalf("port still unbound: %q", port)
	}
}

func TestShutdownStopsAccepts(t *testing.T) {
	raw := ws.New()
	tr := raw.(*ws.Transport)

	var mu sync.Mutex
	accepted := 0

	hold := make(chan struct{})
	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			mu.Lock()
			accepted++
			mu.Unlock()
			<-hold
			_ = c.Close()
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()

	addr := waitAddr(t, tr).String()

	clientTr := ws.New()
	defer clientTr.Close()

	c1, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial1: %v", err)
	}
	defer c1.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := accepted
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first connection not accepted")
		}
		time.Sleep(5 * time.Millisecond)
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shutCancel()
	shutErr := make(chan error, 1)
	go func() { shutErr <- tr.Shutdown(shutCtx) }()

	time.Sleep(20 * time.Millisecond)

	c2, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err == nil {
		_ = c2.Close()
		t.Fatal("Dial after Shutdown should fail")
	}

	close(hold)
	select {
	case err := <-shutErr:
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}

	mu.Lock()
	n := accepted
	mu.Unlock()
	if n != 1 {
		t.Fatalf("accepted = %d, want 1 (no accepts after Shutdown)", n)
	}

	_ = tr.Close()
}
