package http1

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

// startEchoServer responds to each request with its body, one exchange at a
// time; httpunary×http1 reuses one endpoint handle for many calls, so the
// carrier map must not grow with them.
func startEchoServer(t *testing.T) string {
	t.Helper()
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.UnaryResponseWriter
		})
		body, err := io.ReadAll(car)
		if err != nil {
			return
		}
		_ = car.WriteResponse(200, transport.Headers{
			{Name: "Content-Type", Value: "application/octet-stream"},
		}, body)
	})
	return addr
}

func openCall(t *testing.T, sc transport.StreamConn, ctx context.Context) transport.Carrier {
	t.Helper()
	car, err := sc.OpenStream(ctx, transport.RequestPreface{
		RequestTarget: "/echo",
		Headers:       transport.Headers{{Name: "Content-Type", Value: "application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	return car
}

func TestCarrierUntrackedAfterCallCompletes(t *testing.T) {
	addr := startEchoServer(t)

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 8
	for i := 0; i < n; i++ {
		car := openCall(t, sc, ctx)
		if _, err := car.(transport.ByteStreamCarrier).Write([]byte("ping")); err != nil {
			t.Fatalf("Write #%d: %v", i, err)
		}
		if err := car.(transport.SendCloser).CloseSend(); err != nil {
			t.Fatalf("CloseSend #%d: %v", i, err)
		}
		got, err := io.ReadAll(car.(transport.ByteStreamCarrier))
		if err != nil {
			t.Fatalf("ReadAll #%d: %v", i, err)
		}
		if string(got) != "ping" {
			t.Fatalf("body #%d = %q, want ping", i, got)
		}
		if got := trackedCarrierCount(sc.(transport.Conn)); got != 0 {
			t.Fatalf("after completed call #%d tracked=%d, want 0", i, got)
		}
	}
}

func TestCarrierUntrackedAfterAbort(t *testing.T) {
	addr := startEchoServer(t)

	_, sc := dial(t, addr)
	ctx := context.Background()

	const n = 8
	for i := 0; i < n; i++ {
		car := openCall(t, sc, ctx)
		if _, err := car.(transport.ByteStreamCarrier).Write([]byte("ping")); err != nil {
			t.Fatalf("Write #%d: %v", i, err)
		}
		if err := car.Abort(); err != nil {
			t.Fatalf("Abort #%d: %v", i, err)
		}
		if got := trackedCarrierCount(sc.(transport.Conn)); got != 0 {
			t.Fatalf("after Abort #%d tracked=%d, want 0", i, got)
		}
	}
}

// The handle must abort exchanges it still tracks, and must stop tracking an
// exchange once it is over: a carrier is tracked only while it is in flight.
func TestCarrierTrackedWhileInFlight(t *testing.T) {
	release := make(chan struct{})
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.UnaryResponseWriter
		})
		_, _ = io.Copy(io.Discard, car)
		<-release
		_ = car.WriteResponse(200, nil, []byte("done"))
	})
	defer close(release)

	_, sc := dial(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car := openCall(t, sc, ctx)
	if _, err := car.(transport.ByteStreamCarrier).Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := car.(transport.SendCloser).CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	if got := trackedCarrierCount(sc.(transport.Conn)); got != 1 {
		t.Fatalf("in-flight tracked=%d, want 1", got)
	}
}
