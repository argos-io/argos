package echov1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/server"

	jsoncodec "github.com/argos-io/argos/codec/json"
	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/transport/http1"
	"github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/telnet"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

func startEchoServer(t *testing.T, tr addrTransport, codec codec.Codec) {
	t.Helper()
	server := server.New()
	service := server.NewService(append(withLoopbackTransport(tr),
		argos.WithCodec(codec),
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
}

func TestClientEchoSixTransports(t *testing.T) {
	cases := []struct {
		name  string
		newTR func() addrTransport
		codec codec.Codec
	}{
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
			tr := tc.newTR()
			startEchoServer(t, tr, tc.codec)
			client := NewEchoServiceClient(
				argos.WithTransport(tr),
				argos.WithCodec(tc.codec),
			)
			callCtx, callCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer callCancel()
			resp, err := client.Echo(callCtx, &EchoRequest{Msg: tc.name})
			if err != nil {
				t.Fatalf("Echo: %v", err)
			}
			if got, want := resp.GetMsg(), "hello "+tc.name; got != want {
				t.Fatalf("response = %q, want %q", got, want)
			}
		})
	}
}

func TestClientMetadataPassesAuthFilter(t *testing.T) {
	cases := []struct {
		name  string
		newTR func() addrTransport
		codec codec.Codec
	}{
		{
			name:  "http2",
			newTR: func() addrTransport { return http2.New().(addrTransport) },
			codec: protobufcodec.New(),
		},
		{
			name:  "tcp",
			newTR: func() addrTransport { return tcp.New().(addrTransport) },
			codec: protobufcodec.New(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := tc.newTR()
			server := server.New()
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

			client := NewEchoServiceClient(
				argos.WithTransport(tr),
				argos.WithCodec(tc.codec),
				argos.WithFilter(ClientAuth),
			)
			resp, err := client.Echo(context.Background(), &EchoRequest{Msg: tc.name})
			if err != nil {
				t.Fatalf("Echo: %v", err)
			}
			if got, want := resp.GetMsg(), "hello "+tc.name; got != want {
				t.Fatalf("response = %q, want %q", got, want)
			}
		})
	}
}
