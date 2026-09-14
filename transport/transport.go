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
	// Close terminates the whole call and releases the underlying resources.
	// It is idempotent and must unblock a pending Recv or Send where the
	// underlying transport supports blocking I/O.
	Close() error
}

// Named is an optional transport identity used for codec compatibility
// validation. Custom transports may implement it when they have a restricted
// wire representation.
type Named interface {
	TransportName() string
}

// Streaming is an optional capability declaration for transports. A client
// rejects a known streaming call before opening the transport; a server
// rejects it before invoking the business dispatcher. Transports without the
// declaration retain the generic transport contract and are assumed to
// support the call shape.
type Streaming interface {
	SupportsStreaming() bool
}

// Transport is a complete channel: listen for calls, or open one.
type Transport interface {
	ListenAndServe(ctx context.Context,
		onCall func(ctx context.Context, method string, f Framer) error,
		opts ...ServerOption) error
	Open(ctx context.Context, method string, opts ...ClientOption) (Framer, error)
}
