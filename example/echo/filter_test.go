package echov1

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	jsoncodec "github.com/argos-io/argos/codec/json"
	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/transport/http1"
	"github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/telnet"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

type addrTransport interface {
	argos.Transport
	Addr() net.Addr
}

type transportCase struct {
	name  string
	newTR func() addrTransport
	codec argos.Codec
}

func waitListen(tr addrTransport) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tr.Addr() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("transport did not start listening")
}

func startFilteredServer(t *testing.T, tc transportCase) addrTransport {
	t.Helper()
	tr := tc.newTR()
	server := argos.NewServer()
	service := server.NewService(append(withLoopbackTransport(tr),
		argos.WithCodec(tc.codec),
		argos.WithFilter(ServerAuth),
	)...)
	RegisterEchoService(service, NewEchoImpl())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	waitListen(tr)
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("server.Run: %v", err)
		}
	})
	return tr
}

func TestFilterRejectsWithoutToken(t *testing.T) {
	cases := []transportCase{
		{
			name:  "http2",
			newTR: func() addrTransport { return http2.New().(addrTransport) },
			codec: protobufcodec.New(),
		},
		{
			name:  "http1",
			newTR: func() addrTransport { return http1.New().(addrTransport) },
			codec: jsoncodec.New(),
		},
		{
			name:  "tcp",
			newTR: func() addrTransport { return tcp.New().(addrTransport) },
			codec: protobufcodec.New(),
		},
		{
			name:  "udp",
			newTR: func() addrTransport { return udp.New().(addrTransport) },
			codec: protobufcodec.New(),
		},
		{
			name:  "ws",
			newTR: func() addrTransport { return ws.New().(addrTransport) },
			codec: protobufcodec.New(),
		},
		{
			name:  "telnet",
			newTR: func() addrTransport { return telnet.New().(addrTransport) },
			codec: jsoncodec.New(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := startFilteredServer(t, tc)
			client := NewEchoServiceClient(
				argos.WithTransport(tr),
				argos.WithCodec(tc.codec),
			)
			_, err := client.Echo(context.Background(), &EchoRequest{Msg: tc.name})
			if err == nil {
				t.Fatal("Echo succeeded, want Unauthenticated")
			}
			if got := argos.CodeOf(err); got != argos.Unauthenticated {
				t.Fatalf("code = %d (err %v), want Unauthenticated", got, err)
			}
		})
	}
}
