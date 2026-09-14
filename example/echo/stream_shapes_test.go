package echov1

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/ws"
)

func TestCallStreamClientAndBidiStreaming(t *testing.T) {
	cases := []struct {
		name  string
		newTR func() addrTransport
	}{
		{name: "http2", newTR: func() addrTransport { return http2.New().(addrTransport) }},
		{name: "tcp", newTR: func() addrTransport { return tcp.New().(addrTransport) }},
		{name: "ws", newTR: func() addrTransport { return ws.New().(addrTransport) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := tc.newTR()
			srv := server.New()
			svc := srv.NewService(append(withLoopbackTransport(tr), argos.WithCodec(protobuf.New()))...)
			svc.RegisterWithMethods(streamShapesDispatch,
				server.MethodInfo{Method: "echo.v1.Shape/Collect", Kind: stream.CallClientStreaming},
				server.MethodInfo{Method: "echo.v1.Shape/Chat", Kind: stream.CallBidiStreaming},
			)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- srv.Run(ctx) }()
			waitListen(tr)
			t.Cleanup(func() {
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Errorf("server.Run: %v", err)
				}
			})

			c := client.New(argos.WithTransport(tr), argos.WithCodec(protobuf.New()))
			callCtx, callCancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer callCancel()
			emptyCtx := metadata.With(callCtx, metadata.Metadata{"empty": {"yes"}})
			empty := c.OpenStream(emptyCtx, "echo.v1.Shape/Collect", stream.CallClientStreaming)
			if err := empty.CloseSend(); err != nil {
				t.Fatalf("empty Collect CloseSend: %v", err)
			}
			var emptyResponse EchoResponse
			if err := empty.Recv(&emptyResponse); err != nil {
				t.Fatalf("empty Collect Recv: %v", err)
			}
			if got, want := emptyResponse.GetMsg(), ""; got != want {
				t.Fatalf("empty Collect response = %q, want %q", got, want)
			}
			if err := empty.Close(); err != nil {
				t.Fatalf("empty Collect Close: %v", err)
			}

			collect := c.OpenStream(callCtx, "echo.v1.Shape/Collect", stream.CallClientStreaming)
			for _, msg := range []string{"one", "two", "three"} {
				if err := collect.Send(&EchoRequest{Msg: msg}); err != nil {
					t.Fatalf("Collect Send(%q): %v", msg, err)
				}
			}
			if err := collect.CloseSend(); err != nil {
				t.Fatalf("Collect CloseSend: %v", err)
			}
			var response EchoResponse
			if err := collect.Recv(&response); err != nil {
				t.Fatalf("Collect Recv: %v", err)
			}
			if got, want := response.GetMsg(), "one,two,three"; got != want {
				t.Fatalf("Collect response = %q, want %q", got, want)
			}
			if err := collect.Close(); err != nil {
				t.Fatalf("Collect Close: %v", err)
			}

			chat := c.OpenStream(callCtx, "echo.v1.Shape/Chat", stream.CallBidiStreaming)
			for _, msg := range []string{"first", "second"} {
				if err := chat.Send(&EchoRequest{Msg: msg}); err != nil {
					t.Fatalf("Chat Send(%q): %v", msg, err)
				}
				var event Event
				if err := chat.Recv(&event); err != nil {
					t.Fatalf("Chat Recv(%q): %v", msg, err)
				}
				if got, want := event.GetMsg(), "reply "+msg; got != want {
					t.Fatalf("Chat response = %q, want %q", got, want)
				}
			}
			if err := chat.CloseSend(); err != nil {
				t.Fatalf("Chat CloseSend: %v", err)
			}
			if err := chat.Recv(new(Event)); !errors.Is(err, io.EOF) {
				t.Fatalf("Chat terminal Recv = %v, want io.EOF", err)
			}
			if err := chat.Close(); err != nil {
				t.Fatalf("Chat Close: %v", err)
			}
		})
	}
}

func streamShapesDispatch(ctx context.Context, method string, st stream.Stream) error {
	switch method {
	case "echo.v1.Shape/Collect":
		if values := metadata.FromContext(ctx)["empty"]; len(values) != 0 && (len(values) != 1 || values[0] != "yes") {
			return errors.New("invalid empty-stream metadata")
		}
		var messages []string
		for {
			request := new(EchoRequest)
			err := st.Recv(request)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			messages = append(messages, request.GetMsg())
		}
		return st.Send(&EchoResponse{Msg: strings.Join(messages, ",")})
	case "echo.v1.Shape/Chat":
		for {
			request := new(EchoRequest)
			err := st.Recv(request)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := st.Send(&Event{Msg: "reply " + request.GetMsg()}); err != nil {
				return err
			}
		}
	default:
		return errors.New("unknown shape method")
	}
}
