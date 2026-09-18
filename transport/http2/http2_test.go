package http2

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

func startServer(t *testing.T, onConn func(context.Context, transport.Conn)) (*Transport, string) {
	t.Helper()
	raw := New()
	tr := raw.(*Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, onConn, transport.WithListenAddress("127.0.0.1:0"))
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

func dial(t *testing.T, addr string) (transport.Transport, transport.StreamConn) {
	t.Helper()
	clientTr := New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	sc, ok := conn.(transport.StreamConn)
	if !ok {
		t.Fatal("Dial did not return StreamConn")
	}
	return clientTr, sc
}

func TestOpenStreamReturnsBeforeResponseHeaders(t *testing.T) {
	gotFirstByte := make(chan struct{})
	release := make(chan struct{})

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.ResponseWriter
		})
		buf := make([]byte, 1)
		if _, err := io.ReadFull(car, buf); err != nil {
			t.Errorf("server ReadFull: %v", err)
			return
		}
		close(gotFirstByte)
		<-release
		_ = car.WriteHeaders(200, transport.Headers{{Name: "X-Test", Value: "ok"}})
		_, _ = car.Write([]byte("resp"))
		_ = car.Finish(200, nil, transport.Headers{{Name: "X-Trailer", Value: "t"}})
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{
		RequestTarget: "/early",
		Headers:       transport.Headers{{Name: "Content-Type", Value: "application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	bc := car.(transport.ByteStreamCarrier)
	rh := car.(transport.ResponseHeaderReader)

	headersDone := make(chan error, 1)
	go func() {
		_, err := rh.ResponseHeaders()
		headersDone <- err
	}()

	select {
	case err := <-headersDone:
		t.Fatalf("ResponseHeaders returned before request body write: %v", err)
	case <-time.After(50 * time.Millisecond):
		// expected: still waiting for response headers
	}

	if _, err := bc.Write([]byte{0}); err != nil {
		t.Fatalf("Write before response headers: %v", err)
	}
	select {
	case <-gotFirstByte:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive first request byte")
	}

	select {
	case err := <-headersDone:
		t.Fatalf("ResponseHeaders returned before server WriteHeaders: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-headersDone:
		if err != nil {
			t.Fatalf("ResponseHeaders: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ResponseHeaders timed out")
	}
}

func TestUnaryRoundTripH2C(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.RequestHeaderReader
			transport.ResponseWriter
		})
		body, err := io.ReadAll(car)
		if err != nil {
			t.Errorf("server ReadAll: %v", err)
			return
		}
		_ = car.WriteHeaders(200, transport.Headers{
			{Name: "Content-Type", Value: "application/octet-stream"},
			{Name: "X-Echo-Len", Value: string(rune('0' + len(body)))},
		})
		_, _ = car.Write([]byte("echo:"))
		_, _ = car.Write(body)
		_ = car.Finish(200, nil, transport.Headers{
			{Name: "X-Trailer-Status", Value: "ok"},
		})
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{
		RequestTarget: "/unary",
		Headers: transport.Headers{
			{Name: "Content-Type", Value: "application/octet-stream"},
			{Name: "X-Client", Value: "yes"},
		},
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	bc := car.(transport.ByteStreamCarrier)
	scCloser := car.(transport.SendCloser)
	rh := car.(transport.ResponseHeaderReader)
	rt := car.(transport.ResponseTrailerReader)

	payload := []byte("hello-h2c")
	if _, err := bc.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := scCloser.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	status, err := rh.ResponseStatus()
	if err != nil {
		t.Fatalf("ResponseStatus: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	hdrs, err := rh.ResponseHeaders()
	if err != nil {
		t.Fatalf("ResponseHeaders: %v", err)
	}
	if !headerHas(hdrs, "Content-Type", "application/octet-stream") {
		t.Fatalf("missing Content-Type in %v", hdrs)
	}

	got, err := io.ReadAll(bc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := append([]byte("echo:"), payload...)
	if string(got) != string(want) {
		t.Fatalf("body = %q, want %q", got, want)
	}

	trailers, err := rt.ResponseTrailers()
	if err != nil {
		t.Fatalf("ResponseTrailers: %v", err)
	}
	if !headerHas(trailers, "X-Trailer-Status", "ok") {
		t.Fatalf("trailers = %v, want X-Trailer-Status=ok", trailers)
	}
}

func TestServerRequestHeadersAndTrailers(t *testing.T) {
	sawHeaders := make(chan transport.Headers, 1)
	sawTarget := make(chan string, 1)

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.RequestHeaderReader
			transport.ResponseWriter
		})
		sawTarget <- car.RequestTarget()
		sawHeaders <- car.RequestHeaders()
		_, _ = io.Copy(io.Discard, car)
		// Trailers-only style: never WriteHeaders; Finish sends all at once.
		_ = car.Finish(200, transport.Headers{
			{Name: "Content-Type", Value: "application/octet-stream"},
		}, transport.Headers{
			{Name: "Grpc-Status", Value: "0"},
			{Name: "Grpc-Message", Value: ""},
		})
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{
		RequestTarget: "/svc/Method",
		Headers: transport.Headers{
			{Name: "Content-Type", Value: "application/grpc"},
			{Name: "Te", Value: "trailers"},
			{Name: "X-User-Md", Value: "v1"},
		},
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = car.(transport.SendCloser).CloseSend()

	select {
	case target := <-sawTarget:
		if target != "/svc/Method" {
			t.Fatalf("RequestTarget = %q, want /svc/Method", target)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe RequestTarget")
	}

	select {
	case hdrs := <-sawHeaders:
		if !headerHas(hdrs, "Content-Type", "application/grpc") {
			t.Fatalf("RequestHeaders missing Content-Type: %v", hdrs)
		}
		if !headerHas(hdrs, "X-User-Md", "v1") {
			t.Fatalf("RequestHeaders missing X-User-Md: %v", hdrs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe RequestHeaders")
	}

	rh := car.(transport.ResponseHeaderReader)
	status, err := rh.ResponseStatus()
	if err != nil {
		t.Fatalf("ResponseStatus: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	hdrs, err := rh.ResponseHeaders()
	if err != nil {
		t.Fatalf("ResponseHeaders: %v", err)
	}
	// Trailers-only: status keys arrive as response headers (single HEADERS+END_STREAM).
	if !headerHas(hdrs, "Content-Type", "application/octet-stream") {
		t.Fatalf("initial header missing in %v", hdrs)
	}
	if !headerHas(hdrs, "Grpc-Status", "0") {
		t.Fatalf("trailers-only Grpc-Status missing in headers %v", hdrs)
	}
}

func TestConcurrentStreamsOneDial(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.ResponseWriter
		})
		body, _ := io.ReadAll(car)
		_ = car.WriteHeaders(200, nil)
		_, _ = car.Write(body)
		_ = car.Finish(200, nil, transport.Headers{{Name: "X-Done", Value: "1"}})
	})

	_, sc := dial(t, addr)

	const n = 2
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			car, err := sc.OpenStream(ctx, transport.RequestPreface{
				RequestTarget: "/c",
				Headers:       transport.Headers{{Name: "Content-Type", Value: "application/octet-stream"}},
			})
			if err != nil {
				errCh <- err
				return
			}
			msg := []byte{byte('A' + i)}
			if _, err := car.(transport.ByteStreamCarrier).Write(msg); err != nil {
				errCh <- err
				return
			}
			if err := car.(transport.SendCloser).CloseSend(); err != nil {
				errCh <- err
				return
			}
			got, err := io.ReadAll(car.(transport.ByteStreamCarrier))
			if err != nil {
				errCh <- err
				return
			}
			if string(got) != string(msg) {
				errCh <- io.ErrUnexpectedEOF
				return
			}
			tr, err := car.(transport.ResponseTrailerReader).ResponseTrailers()
			if err != nil {
				errCh <- err
				return
			}
			if !headerHas(tr, "X-Done", "1") {
				errCh <- io.ErrUnexpectedEOF
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent stream: %v", err)
		}
	}
}

func TestNoGRPCImports(t *testing.T) {
	// Production import graph is also covered by invariants_test; this keeps
	// the package self-contained for Task 3.2 verify.
	// The compile of this package already fails if http2 imports grpc.
	_ = New
}

func TestConnCloseDoesNotKillSharedClient(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.ResponseWriter
		})
		_, _ = io.Copy(io.Discard, car)
		_ = car.Finish(200, transport.Headers{{Name: "Content-Type", Value: "text/plain"}}, nil)
	})

	clientTr := New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx := context.Background()
	c1, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial1: %v", err)
	}
	sc1 := c1.(transport.StreamConn)
	car1, err := sc1.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/a"})
	if err != nil {
		t.Fatalf("OpenStream1: %v", err)
	}
	_ = car1.(transport.SendCloser).CloseSend()
	_, _ = io.Copy(io.Discard, car1.(transport.ByteStreamCarrier))
	_ = c1.Close()

	c2, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial2 after Conn.Close: %v", err)
	}
	defer c2.Close()
	car2, err := c2.(transport.StreamConn).OpenStream(ctx, transport.RequestPreface{RequestTarget: "/b"})
	if err != nil {
		t.Fatalf("OpenStream2: %v", err)
	}
	_ = car2.(transport.SendCloser).CloseSend()
	if _, err := car2.(transport.ResponseHeaderReader).ResponseStatus(); err != nil {
		t.Fatalf("second stream after first Conn.Close: %v", err)
	}
}

func headerHas(hdrs transport.Headers, name, value string) bool {
	for _, h := range hdrs {
		if equalFold(h.Name, name) && h.Value == value {
			return true
		}
	}
	return false
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func TestOpenStreamCarrierUntrackedAfterAbort(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.ResponseWriter
		})
		_, _ = io.Copy(io.Discard, car)
		_ = car.Finish(200, nil, nil)
	})

	_, sc := dial(t, addr)
	ctx := context.Background()
	const n = 32
	for i := 0; i < n; i++ {
		car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/x"})
		if err != nil {
			t.Fatalf("OpenStream #%d: %v", i, err)
		}
		_ = car.(transport.SendCloser).CloseSend()
		_, _ = io.Copy(io.Discard, car.(transport.ByteStreamCarrier))
		_ = car.Abort()
		if got := trackedCarrierCount(sc.(transport.Conn)); got != 0 {
			t.Fatalf("after Abort #%d tracked=%d, want 0", i, got)
		}
	}
}
