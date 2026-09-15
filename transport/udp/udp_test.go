package udp_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/udp"
)

func waitAddr(t *testing.T, tr *udp.Transport) net.Addr {
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

func startEchoServer(t *testing.T) (tr *udp.Transport, addr string) {
	t.Helper()
	raw := udp.New()
	tr = raw.(*udp.Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			cc := c.(transport.CarrierConn)
			car := cc.Carrier().(transport.DatagramCarrier)
			for {
				msg, err := car.RecvDatagram()
				if err != nil {
					_ = c.Close()
					return
				}
				if err := car.SendDatagram(msg); err != nil {
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

func TestDialServeRoundTrip(t *testing.T) {
	_, addr := startEchoServer(t)

	clientTr := udp.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	cc := conn.(transport.CarrierConn)
	car := cc.Carrier().(transport.DatagramCarrier)

	msg := []byte("hello-udp-echo")
	if err := car.SendDatagram(msg); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}
	got, err := car.RecvDatagram()
	if err != nil {
		t.Fatalf("RecvDatagram: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
}

func TestSequentialExchanges(t *testing.T) {
	_, addr := startEchoServer(t)

	clientTr := udp.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	car := conn.(transport.CarrierConn).Carrier().(transport.DatagramCarrier)

	for i, msg := range [][]byte{
		[]byte("exchange-1"),
		[]byte("exchange-2"),
		[]byte("exchange-3"),
	} {
		if err := car.SendDatagram(msg); err != nil {
			t.Fatalf("SendDatagram[%d]: %v", i, err)
		}
		got, err := car.RecvDatagram()
		if err != nil {
			t.Fatalf("RecvDatagram[%d]: %v", i, err)
		}
		if string(got) != string(msg) {
			t.Fatalf("echo[%d] = %q, want %q", i, got, msg)
		}
	}
}

func TestOversizedSendFails(t *testing.T) {
	_, addr := startEchoServer(t)

	clientTr := udp.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	car := conn.(transport.CarrierConn).Carrier().(transport.DatagramCarrier)

	oversized := make([]byte, udp.MaxDatagramSize+1)
	err = car.SendDatagram(oversized)
	if err == nil {
		t.Fatal("SendDatagram(oversized) succeeded, want error")
	}
	if !strings.Contains(err.Error(), "maximum is") {
		t.Fatalf("SendDatagram error = %v, want maximum-size message", err)
	}

	// Max-sized datagram must still be accepted by the API gate.
	maxOK := make([]byte, udp.MaxDatagramSize)
	for i := range maxOK {
		maxOK[i] = byte(i)
	}
	if err := car.SendDatagram(maxOK); err != nil {
		t.Fatalf("SendDatagram(MaxDatagramSize): %v", err)
	}
	got, err := car.RecvDatagram()
	if err != nil {
		t.Fatalf("RecvDatagram(max): %v", err)
	}
	if len(got) != len(maxOK) {
		t.Fatalf("echo len = %d, want %d", len(got), len(maxOK))
	}
}

func TestAddrAfterServe(t *testing.T) {
	raw := udp.New()
	tr := raw.(*udp.Transport)
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
	raw := udp.New()
	tr := raw.(*udp.Transport)

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
			car := c.(transport.CarrierConn).Carrier().(transport.DatagramCarrier)
			_, _ = car.RecvDatagram() // consume the creating datagram
			<-hold
			_ = c.Close()
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()

	addr := waitAddr(t, tr).String()

	clientTr := udp.New()
	defer clientTr.Close()

	c1, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial1: %v", err)
	}
	defer c1.Close()
	if err := c1.(transport.CarrierConn).Carrier().(transport.DatagramCarrier).SendDatagram([]byte("ping")); err != nil {
		t.Fatalf("SendDatagram1: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := accepted
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first association not accepted")
		}
		time.Sleep(5 * time.Millisecond)
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shutCancel()
	shutErr := make(chan error, 1)
	go func() { shutErr <- tr.Shutdown(shutCtx) }()

	time.Sleep(20 * time.Millisecond)

	// After Shutdown the PacketConn is closed; new dials may still create a
	// local socket, but server-side accepts must not increase.
	c2, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err == nil {
		_ = c2.(transport.CarrierConn).Carrier().(transport.DatagramCarrier).SendDatagram([]byte("late"))
		_ = c2.Close()
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
