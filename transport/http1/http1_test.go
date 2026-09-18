package http1

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
	// HTTP/1.1 is not fully duplex: the peer typically needs CloseSend before
	// response headers are readable. What §4.2 requires is that OpenStream
	// itself returns a writable Carrier without waiting for the response.
	blockHandler := make(chan struct{})

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.UnaryResponseWriter
		})
		<-blockHandler
		_, _ = io.Copy(io.Discard, car)
		_ = car.WriteResponse(200, transport.Headers{{Name: "X-Test", Value: "ok"}}, []byte("resp"))
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	openDone := make(chan struct{})
	var car transport.Carrier
	var openErr error
	go func() {
		car, openErr = sc.OpenStream(ctx, transport.RequestPreface{
			RequestTarget: "/early",
			Headers:       transport.Headers{{Name: "Content-Type", Value: "application/octet-stream"}},
		})
		close(openDone)
	}()

	select {
	case <-openDone:
		if openErr != nil {
			t.Fatalf("OpenStream: %v", openErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OpenStream blocked waiting for response (handler still held)")
	}

	bc := car.(transport.ByteStreamCarrier)
	rh := car.(transport.ResponseHeaderReader)

	// Carrier is writable before any response exists.
	if _, err := bc.Write([]byte("ping")); err != nil {
		t.Fatalf("Write before response: %v", err)
	}

	headersDone := make(chan error, 1)
	go func() {
		_, err := rh.ResponseHeaders()
		headersDone <- err
	}()

	select {
	case err := <-headersDone:
		t.Fatalf("ResponseHeaders returned while server still blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := car.(transport.SendCloser).CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	close(blockHandler)

	select {
	case err := <-headersDone:
		if err != nil {
			t.Fatalf("ResponseHeaders: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ResponseHeaders timed out after releasing server")
	}
}

func TestWriteResponseCommits200Once(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.RequestHeaderReader
			transport.UnaryResponseWriter
		})
		body, err := io.ReadAll(car)
		if err != nil {
			t.Errorf("server ReadAll: %v", err)
			return
		}
		if err := car.WriteResponse(200, transport.Headers{
			{Name: "Content-Type", Value: "application/octet-stream"},
			{Name: "X-Echo-Len", Value: string(rune('0' + len(body)))},
		}, append([]byte("echo:"), body...)); err != nil {
			t.Errorf("WriteResponse: %v", err)
			return
		}
		// Second commit must be rejected and must not change the wire response.
		if err := car.WriteResponse(500, nil, []byte("nope")); err == nil {
			t.Error("second WriteResponse succeeded, want error")
		}
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

	payload := []byte("hello-h1")
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
}

// Probe 0.10 / §4.6: handler "Send"s into the buffer (no WriteHeader), then
// returns error; Finish/WriteResponse commits 500 — client must not see 200.
func TestWriteResponseAfterBufferedSendCanBeError(t *testing.T) {
	const (
		successBody = `{"result":"ok"}`
		errorBody   = `{"error":"handler failed"}`
	)

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.UnaryResponseWriter
		})
		_, _ = io.Copy(io.Discard, car)

		// Handler "Send": buffer success body without committing headers.
		if _, err := car.Write([]byte(successBody)); err != nil {
			t.Errorf("buffered Write: %v", err)
			return
		}

		// Handler returns error; framing Finish writes error status + body.
		// WriteResponse must work without any prior WriteHeader.
		if err := car.WriteResponse(http.StatusInternalServerError, transport.Headers{
			{Name: "Content-Type", Value: "application/json"},
		}, []byte(errorBody)); err != nil {
			t.Errorf("WriteResponse: %v", err)
		}
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/err"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = car.(transport.SendCloser).CloseSend()

	status, err := car.(transport.ResponseHeaderReader).ResponseStatus()
	if err != nil {
		t.Fatalf("ResponseStatus: %v", err)
	}
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (must not be 200)", status)
	}
	got, err := io.ReadAll(car.(transport.ByteStreamCarrier))
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, []byte(errorBody)) {
		t.Fatalf("body = %q, want %q", got, errorBody)
	}
	if bytes.Contains(got, []byte(successBody)) {
		t.Fatalf("client saw buffered success body after handler error")
	}
}

// Probe 0.10 contrast: once net/http WriteHeader(200) runs, status cannot
// change to 500 — why UnaryResponseWriter must commit only at Finish.
func TestCannotChangeStatusAfterWriteHeader200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"too late"}`))
	}))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (second WriteHeader must be ignored)", resp.StatusCode)
	}
	gotBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(gotBody, []byte(`{"error":"too late"}`)) {
		t.Fatalf("body = %q, want error JSON (body still writes, but status stays 200)", gotBody)
	}
}

func TestServerRequestHeaders(t *testing.T) {
	sawHeaders := make(chan transport.Headers, 1)
	sawTarget := make(chan string, 1)

	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.RequestHeaderReader
			transport.UnaryResponseWriter
		})
		sawTarget <- car.RequestTarget()
		sawHeaders <- car.RequestHeaders()
		_, _ = io.Copy(io.Discard, car)
		_ = car.WriteResponse(200, transport.Headers{
			{Name: "Content-Type", Value: "application/octet-stream"},
		}, []byte("ok"))
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{
		RequestTarget: "/svc/Method",
		Headers: transport.Headers{
			{Name: "Content-Type", Value: "application/json"},
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
		if !headerHas(hdrs, "Content-Type", "application/json") {
			t.Fatalf("RequestHeaders missing Content-Type: %v", hdrs)
		}
		if !headerHas(hdrs, "X-User-Md", "v1") {
			t.Fatalf("RequestHeaders missing X-User-Md: %v", hdrs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe RequestHeaders")
	}

	status, err := car.(transport.ResponseHeaderReader).ResponseStatus()
	if err != nil {
		t.Fatalf("ResponseStatus: %v", err)
	}
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
}

func TestConnCloseDoesNotKillSharedClient(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.UnaryResponseWriter
		})
		_, _ = io.Copy(io.Discard, car)
		_ = car.WriteResponse(200, transport.Headers{{Name: "Content-Type", Value: "text/plain"}}, []byte("ok"))
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

func TestServerDoesNotImplementResponseWriter(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier()
		if _, ok := car.(transport.ResponseWriter); ok {
			t.Error("http1 server Carrier must not implement ResponseWriter")
		}
		if _, ok := car.(transport.UnaryResponseWriter); !ok {
			t.Error("http1 server Carrier must implement UnaryResponseWriter")
		}
		_, _ = io.Copy(io.Discard, car.(transport.ByteStreamCarrier))
		_ = car.(transport.UnaryResponseWriter).WriteResponse(200, nil, nil)
	})

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = car.(transport.SendCloser).CloseSend()
	_, _ = io.Copy(io.Discard, car.(transport.ByteStreamCarrier))
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
