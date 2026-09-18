package http2

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

// A peer that connects and never sends a request never reaches onConn, so the
// composition layer's connection admission and idle timers cannot see it. Only
// the HTTP-level timeouts bound it.
func TestHalfOpenConnectionIsClosedByReadHeaderTimeout(t *testing.T) {
	t.Parallel()
	tr := New()
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = tr.Serve(ctx, func(context.Context, transport.Conn) {},
			transport.WithListenAddress("127.0.0.1:0"),
			transport.WithHTTPReadHeaderTimeout(200*time.Millisecond),
			transport.WithHTTPIdleTimeout(200*time.Millisecond))
	}()

	addr := waitListenerAddr(t, tr)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	// Send nothing at all: not even a request line or an HTTP/2 preface.
	_ = nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	if _, err := nc.Read(buf); err == nil {
		t.Fatal("half-open connection was still open")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("half-open connection survived past HTTPReadHeaderTimeout")
	}
}

func waitListenerAddr(t *testing.T, v any) string {
	t.Helper()
	a, ok := v.(interface{ Addr() net.Addr })
	if !ok {
		t.Fatal("transport does not expose Addr()")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := a.Addr(); addr != nil {
			return addr.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener addr not ready")
	return ""
}
