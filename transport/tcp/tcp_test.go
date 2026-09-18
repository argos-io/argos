package tcp

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

func waitAddr(t *testing.T, tr *Transport) net.Addr {
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

func startEchoServer(t *testing.T) (tr *Transport, addr string, cancel context.CancelFunc) {
	t.Helper()
	raw := New()
	tr = raw.(*Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			cc := c.(interface {
				transport.CarrierConn
				io.Reader
				io.Writer
				io.Closer
			})
			car := cc.Carrier().(io.ReadWriter)
			_, _ = io.Copy(car, car)
			_ = cc.Close()
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
	return tr, a.String(), cancel
}

func TestDialServeEcho(t *testing.T) {
	_, addr, _ := startEchoServer(t)

	clientTr := New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	cc := conn.(transport.CarrierConn)
	car := cc.Carrier().(transport.ByteStreamCarrier)

	msg := []byte("hello-tcp-echo")
	if _, err := car.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(car, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("echo = %q, want %q", buf, msg)
	}

	// Both directions: write again after a successful round-trip.
	msg2 := []byte("round-trip-2")
	if _, err := car.Write(msg2); err != nil {
		t.Fatalf("Write2: %v", err)
	}
	buf2 := make([]byte, len(msg2))
	if _, err := io.ReadFull(car, buf2); err != nil {
		t.Fatalf("ReadFull2: %v", err)
	}
	if string(buf2) != string(msg2) {
		t.Fatalf("echo2 = %q, want %q", buf2, msg2)
	}
}

func TestCloseSendEOF(t *testing.T) {
	raw := New()
	tr := raw.(*Transport)

	serverReady := make(chan transport.Conn, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			serverReady <- c
			<-release // hold onConn; test performs the Read
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	defer func() {
		close(release)
		cancel()
		_ = tr.Close()
		<-errCh
	}()

	addr := waitAddr(t, tr).String()

	clientTr := New()
	defer clientTr.Close()

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := clientTr.Dial(dialCtx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	var serverConn transport.Conn
	select {
	case serverConn = <-serverReady:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not accept")
	}

	sc := conn.(transport.SendCloser)
	if err := sc.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Peer should see EOF on read side.
	serverCar := serverConn.(transport.CarrierConn).Carrier().(io.Reader)
	buf := make([]byte, 8)
	n, err := serverCar.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("server Read after CloseSend: n=%d err=%v, want EOF", n, err)
	}
}

func TestAddrAfterServe(t *testing.T) {
	raw := New()
	tr := raw.(*Transport)
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

	// Dial and check Conn.Addr is set.
	clientTr := New()
	defer clientTr.Close()
	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: a.String()})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	tc, ok := conn.(*Conn)
	if !ok {
		t.Fatalf("conn type %T, want *Conn", conn)
	}
	if tc.Addr() == nil {
		t.Fatal("Conn.Addr() is nil")
	}
}

func TestShutdownStopsAccepts(t *testing.T) {
	raw := New()
	tr := raw.(*Transport)

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
			<-hold // keep onConn alive until released
			_ = c.Close()
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()

	addr := waitAddr(t, tr).String()

	clientTr := New()
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

	// Shutdown with a short deadline while first onConn is still held.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shutCancel()
	shutErr := make(chan error, 1)
	go func() { shutErr <- tr.Shutdown(shutCtx) }()

	// Give Shutdown time to close the listener.
	time.Sleep(20 * time.Millisecond)

	c2, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err == nil {
		_ = c2.Close()
		t.Fatal("Dial after Shutdown should fail")
	}

	close(hold) // release in-flight onConn
	select {
	case err := <-shutErr:
		// May be nil (onConn finished before deadline) or context deadline.
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
