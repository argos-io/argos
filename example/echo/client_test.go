package echov1

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/status"
)

func TestClientEchoTransports(t *testing.T) {
	cases := []struct {
		name string
		axes func() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc)
	}{
		{name: "grpc_http2", axes: GRPCAxes},
		{name: "httpunary_rpc_http1", axes: HTTPUnaryRPCAxes},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr, fr, cd := tc.axes()
			ec := startEcho(t, tr, fr, cd)
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
	tr, fr, cd := GRPCAxes()
	ec := startEcho(t, tr, fr, cd)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := ec.Watch(ctx, &WatchRequest{Msg: "grpc_http2"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer stream.Close()

	want := []string{"grpc_http2 one", "grpc_http2 two", "grpc_http2 three"}
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
}

func TestClientMetadataPassesAuthFilter(t *testing.T) {
	tr, fr, cd := GRPCAxes()
	ec := startEcho(t, tr, fr, cd, func(cfg *argos.Config) {
		cfg.Filters = append(cfg.Filters, ServerAuth)
		cfg.OpenFilters = append(cfg.OpenFilters, ClientAuth)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := ec.Echo(ctx, &EchoRequest{Msg: "grpc_http2"})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := resp.GetMsg(), "hello grpc_http2"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestServerAuthRejectsMissingToken(t *testing.T) {
	tr, fr, cd := GRPCAxes()
	ec := startEcho(t, tr, fr, cd, func(cfg *argos.Config) {
		cfg.Filters = append(cfg.Filters, ServerAuth)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := ec.Echo(ctx, &EchoRequest{Msg: "noauth"})
	if status.CodeOf(err) != status.Unauthenticated {
		t.Fatalf("Echo err = %v (code %v), want Unauthenticated", err, status.CodeOf(err))
	}
}
