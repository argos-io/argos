package http1

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

// An endpoint handle is tracked only while it is open: session recycling
// closes handles on idle/lifetime timeouts and must not accumulate dead
// entries.
func TestDialedStreamConnUntrackedOnClose(t *testing.T) {
	_, addr := startServer(t, func(context.Context, transport.Conn) {})

	clientTr := New()
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const n = 16
	for i := 0; i < n; i++ {
		conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
		if err != nil {
			t.Fatalf("Dial #%d: %v", i, err)
		}
		if got := trackedStreamConnCount(clientTr); got != 1 {
			t.Fatalf("open handle #%d tracked=%d, want 1", i, got)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("second Close #%d: %v", i, err)
		}
		if got := trackedStreamConnCount(clientTr); got != 0 {
			t.Fatalf("after Close #%d tracked=%d, want 0", i, got)
		}
	}
}
