// Package transport is the network layer: message boundaries, routing keys, and metadata on the wire.
package transport

import (
	"context"
	"io"
)

// Framer is the wire side of one call: message boundaries, not routing.
type Framer interface {
	Recv() (io.Reader, error)
	Send() (io.WriteCloser, error)
	CloseSend() error
}

// Transport is a complete channel: listen for calls, or open one.
type Transport interface {
	ListenAndServe(ctx context.Context,
		onCall func(ctx context.Context, method string, f Framer) error,
		opts ...ServerOption) error
	Open(ctx context.Context, method string, opts ...ClientOption) (Framer, error)
}
