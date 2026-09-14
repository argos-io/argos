package echov1

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"

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
				argos.WithTransport(tr),
				argos.WithCodec(tc.codec),
			)
			callCtx, callCancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer callCancel()
			stream := client.Watch(callCtx, &WatchRequest{Msg: tc.name})
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
				argos.WithTransport(tr),
				argos.WithCodec(tc.codec),
			)
			callCtx, callCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer callCancel()
			stream := client.Watch(callCtx, &WatchRequest{Msg: tc.name})
			defer stream.Close()
			if _, err := stream.Recv(); err == nil {
				t.Fatal("Recv succeeded, want unsupported streaming error")
			} else if errors.Is(err, io.EOF) || !strings.Contains(err.Error(), "does not support streaming calls") {
				t.Fatalf("Recv error = %v, want unsupported streaming error", err)
			}
		})
	}
}
