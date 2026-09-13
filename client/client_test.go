package client_test

import (
	"context"
	"io"
	"testing"

	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/internal/option"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

type captureTransport struct {
	gotAddr *string
}

func (t *captureTransport) ListenAndServe(
	context.Context,
	func(context.Context, string, transport.Framer) error,
	...transport.ServerOption,
) error {
	return nil
}

func (t *captureTransport) Open(_ context.Context, _ string, opts ...transport.ClientOption) (transport.Framer, error) {
	call := transport.ApplyClientOptions(opts)
	*t.gotAddr = call.DialAddress
	return nil, nil
}

func TestClientTargetIPScheme(t *testing.T) {
	var gotAddr string
	c := client.New(
		option.WithTarget("ip://127.0.0.1:9090"),
		option.WithTransport(&captureTransport{gotAddr: &gotAddr}),
		option.WithCodec(nopCodec{}),
	)

	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got, want := gotAddr, "127.0.0.1:9090"; got != want {
		t.Fatalf("dial address = %q, want %q", got, want)
	}
}

func TestClientTargetRejectsBareHostPort(t *testing.T) {
	c := client.New(
		option.WithTarget("127.0.0.1:7000"),
		option.WithTransport(&captureTransport{}),
		option.WithCodec(nopCodec{}),
	)
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for target without scheme")
	}
}

type nopCodec struct{}

func (nopCodec) Marshal(_ io.Writer, _ any) error   { return nil }
func (nopCodec) Unmarshal(_ io.Reader, _ any) error { return nil }

func TestOpenErrorsWithoutTransport(t *testing.T) {
	c := client.New(option.WithCodec(nopCodec{}))
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error without transport")
	}
}

func TestOpenErrorsWithoutCodec(t *testing.T) {
	c := client.New(option.WithTransport(&captureTransport{}))
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error without codec")
	}
}

func TestOpenInvalidTargetReturnsError(t *testing.T) {
	c := client.New(
		option.WithTarget("bad://no-such-scheme"),
		option.WithTransport(&captureTransport{}),
		option.WithCodec(nopCodec{}),
	)
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for unknown selector scheme")
	}
}
