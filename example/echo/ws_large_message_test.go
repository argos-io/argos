package echov1

import (
	"context"
	"strings"
	"testing"
	"time"

	envelopebinding "github.com/argos-io/argos/binding/envelope"
)

// coder/websocket caps a message read at 32 KiB by default and closes the whole
// connection (status 1009) when exceeded, which is far below the framework's
// 4 MiB frame default. envelope×ws puts one frame per WebSocket message, so
// without an explicit SetReadLimit any call above ~32 KiB tore the session down.
func TestWSLargeMessageExceedsDefaultReadLimit(t *testing.T) {
	// Comfortably above coder/websocket's 32768-byte default and well below the
	// 4 MiB frame default.
	const size = 128 << 10
	cli := startEcho(t, envelopebinding.NewWS())
	ec := NewEchoServiceClient(cli)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := strings.Repeat("x", size)
	resp, err := ec.Echo(ctx, &EchoRequest{Msg: payload})
	if err != nil {
		t.Fatalf("Echo with a %d-byte payload: %v", size, err)
	}
	if got, want := resp.GetMsg(), "hello "+payload; got != want {
		t.Fatalf("response length = %d, want %d", len(got), len(want))
	}
}
