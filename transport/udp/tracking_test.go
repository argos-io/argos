package udp_test

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/udp"
)

// A dialed association is tracked only while it is open: session recycling
// closes connections on idle/lifetime timeouts and must not accumulate dead
// entries.
func TestDialedConnUntrackedOnClose(t *testing.T) {
	_, addr := startEchoServer(t)

	clientTr := udp.New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 16
	for i := 0; i < n; i++ {
		conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
		if err != nil {
			t.Fatalf("Dial #%d: %v", i, err)
		}
		if got := udp.TrackedConnCount(clientTr); got != 1 {
			t.Fatalf("open conn #%d tracked=%d, want 1", i, got)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("second Close #%d: %v", i, err)
		}
		if got := udp.TrackedConnCount(clientTr); got != 0 {
			t.Fatalf("after Close #%d tracked=%d, want 0", i, got)
		}
	}
}
