package echov1

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/option"

	jsoncodec "github.com/argos-io/argos/codec/json"
	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/transport/http1"
	"github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/telnet"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

func TestWatchStreaming(t *testing.T) {
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
		{
			name:  "ws",
			newTR: func() addrTransport { return ws.New().(addrTransport) },
			codec: protobufcodec.New(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := tc.newTR()
			startEchoServer(t, tr, tc.codec)
			client := NewEchoServiceClient(
				option.WithTransport(tr),
				option.WithCodec(tc.codec),
			)
			stream := client.Watch(context.Background(), &WatchRequest{Msg: tc.name})
			want := []string{tc.name + " one", tc.name + " two", tc.name + " three"}
			for i, wantMsg := range want {
				event, err := stream.Recv()
				if err != nil {
					t.Fatalf("Recv event %d: %v", i+1, err)
				}
				if got := event.GetMsg(); got != wantMsg {
					t.Fatalf("event %d = %q, want %q", i+1, got, wantMsg)
				}
			}
			if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("Recv after last event = %v, want io.EOF", err)
			}
		})
	}
}

func TestWatchFailsOnUnaryTransports(t *testing.T) {
	cases := []struct {
		name  string
		newTR func() addrTransport
		codec codec.Codec
	}{
		{
			name:  "http1",
			newTR: func() addrTransport { return http1.New().(addrTransport) },
			codec: jsoncodec.New(),
		},
		{
			name:  "udp",
			newTR: func() addrTransport { return udp.New().(addrTransport) },
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
				option.WithTransport(tr),
				option.WithCodec(tc.codec),
			)
			stream := client.Watch(context.Background(), &WatchRequest{Msg: tc.name})
			event, err := stream.Recv()
			if err != nil {
				t.Fatalf("first Recv: %v", err)
			}
			if event.GetMsg() == "" {
				t.Fatal("expected first event")
			}
			deadline := time.After(2 * time.Second)
			for {
				select {
				case <-deadline:
					t.Fatal("second Recv did not fail")
				default:
					_, err := stream.Recv()
					if err != nil {
						if errors.Is(err, io.EOF) {
							t.Fatal("got io.EOF, want streaming error")
						}
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		})
	}
}
