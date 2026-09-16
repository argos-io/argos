package echov1

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos"
	envelopebinding "github.com/argos-io/argos/binding/envelope"
	grpcbinding "github.com/argos-io/argos/binding/grpc"
	wholebodybinding "github.com/argos-io/argos/binding/wholebody"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport/udp"
)

func TestClientEchoTransports(t *testing.T) {
	cases := []struct {
		name string
		fn   argos.BindingFunc
		tune func(*argos.Config)
	}{
		{name: "grpc_http2", fn: grpcbinding.New()},
		{name: "envelope_tcp", fn: envelopebinding.NewTCP()},
		{name: "envelope_ws", fn: envelopebinding.NewWS()},
		{
			name: "envelope_udp",
			fn:   envelopebinding.NewUDP(),
			// A call has to fit in one datagram, and both ends must agree on
			// that, which is what sharing the Config buys here.
			tune: func(cfg *argos.Config) {
				cfg.MaxFrameSize = udp.MaxDatagramSize
				cfg.MaxMessageSize = 32 << 10
			},
		},
		{name: "wholebody_http1", fn: wholebodybinding.New()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ec := startEcho(t, tc.fn, tc.tune)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := ec.Echo(ctx, &EchoRequest{Msg: tc.name})
			if err != nil {
				t.Fatalf("Echo: %v", err)
			}
			if got, want := resp.GetMsg(), "hello "+tc.name; got != want {
				t.Fatalf("response = %q, want %q", got, want)
			}
		})
	}
}

func TestWatchStreaming(t *testing.T) {
	cases := []struct {
		name string
		fn   argos.BindingFunc
	}{
		{name: "grpc_http2", fn: grpcbinding.New()},
		{name: "envelope_tcp", fn: envelopebinding.NewTCP()},
		{name: "envelope_ws", fn: envelopebinding.NewWS()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ec := startEcho(t, tc.fn)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream, err := ec.Watch(ctx, &WatchRequest{Msg: tc.name})
			if err != nil {
				t.Fatalf("Watch: %v", err)
			}
			defer stream.Close()

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

func TestClientMetadataPassesAuthFilter(t *testing.T) {
	cases := []struct {
		name string
		fn   argos.BindingFunc
	}{
		{name: "grpc_http2", fn: grpcbinding.New()},
		{name: "envelope_tcp", fn: envelopebinding.NewTCP()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ec := startEcho(t, tc.fn, func(cfg *argos.Config) {
				// The two chains are plain fields on the one Config both ends
				// start from, so the server-side and client-side halves of
				// this test's auth cannot drift apart.
				cfg.Filters = append(cfg.Filters, ServerAuth)
				cfg.OpenFilters = append(cfg.OpenFilters, ClientAuth)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := ec.Echo(ctx, &EchoRequest{Msg: tc.name})
			if err != nil {
				t.Fatalf("Echo: %v", err)
			}
			if got, want := resp.GetMsg(), "hello "+tc.name; got != want {
				t.Fatalf("response = %q, want %q", got, want)
			}
		})
	}
}

func TestServerAuthRejectsMissingToken(t *testing.T) {
	ec := startEcho(t, grpcbinding.New(), func(cfg *argos.Config) {
		cfg.Filters = append(cfg.Filters, ServerAuth)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := ec.Echo(ctx, &EchoRequest{Msg: "noauth"})
	if status.CodeOf(err) != status.Unauthenticated {
		t.Fatalf("Echo err = %v (code %v), want Unauthenticated", err, status.CodeOf(err))
	}
}
