package http2_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
)

// A completed call must leave the carrier map empty even when the caller reads
// the body to EOF without asking for trailers: net/http needs the body closed
// to release the stream, and the map must not grow with completed calls. http1
// already guaranteed this; http2 untracked only in ResponseTrailers and Abort.
func TestCarrierUntrackedAfterBodyRead(t *testing.T) {
	_, addr := startServer(t, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.ResponseWriter
		})
		body, err := io.ReadAll(car)
		if err != nil {
			return
		}
		_ = car.WriteHeaders(200, transport.Headers{
			{Name: "Content-Type", Value: "application/octet-stream"},
		})
		_, _ = car.Write(body)
		_ = car.Finish(200, nil, nil)
	})

	tr := argoshttp2.New()
	t.Cleanup(func() { _ = tr.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := tr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	sc := conn.(transport.StreamConn)

	for i := 0; i < 8; i++ {
		car, err := sc.OpenStream(ctx, transport.RequestPreface{
			RequestTarget: "/echo",
			Headers:       transport.Headers{{Name: "Content-Type", Value: "application/octet-stream"}},
		})
		if err != nil {
			t.Fatalf("OpenStream #%d: %v", i, err)
		}
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
		// No ResponseTrailers, no Abort: reading to EOF ends the exchange.
		if n := argoshttp2.TrackedCarrierCount(conn); n != 0 {
			t.Fatalf("after completed call #%d tracked=%d, want 0", i, n)
		}
	}
}

// A round trip that fails produces no response, so the exchange is over and
// nothing should stay tracked even if the caller just returns the error.
func TestCarrierUntrackedAfterFailedRoundTrip(t *testing.T) {
	srvTr, addr := startServer(t, func(context.Context, transport.Conn) {})

	tr := argoshttp2.New()
	t.Cleanup(func() { _ = tr.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := tr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sc := conn.(transport.StreamConn)

	car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/gone"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	// Kill the server, not our own StreamConn: closing the client transport
	// would clear the tracking map and make this assertion vacuous.
	_ = srvTr.Close()

	_, _ = car.(transport.ByteStreamCarrier).Write([]byte("x"))
	_ = car.(transport.SendCloser).CloseSend()
	if _, err := io.ReadAll(car.(transport.ByteStreamCarrier)); err == nil {
		t.Skip("round trip unexpectedly succeeded; nothing to assert")
	}
	if n := argoshttp2.TrackedCarrierCount(conn); n != 0 {
		t.Fatalf("after a failed round trip tracked=%d, want 0", n)
	}
}
