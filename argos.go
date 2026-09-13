// Package argos is the public entry point. Subpackages hold implementations;
// import argos for the API that generated code and applications use.
package argos

import (
	"context"

	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/option"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

type (
	// Transport is a complete channel: listen for calls, or open one.
	Transport = transport.Transport
	// Framer is the wire side of one call.
	Framer = transport.Framer
	// Codec converts messages to and from bytes.
	Codec = codec.Codec
	// Stream is a decoded call stream.
	Stream = stream.Stream
	// Handler is one call after filters.
	Handler = filter.Handler
	// Filter wraps a Handler.
	Filter = filter.Filter
	// Code is argos's own status space.
	Code = errs.Code
	// Metadata is key/value pairs outside the message body.
	Metadata = metadata.Metadata
	// Option configures a Service or Client.
	Option = option.Option
	// Server owns the services served together.
	Server = server.Server
	// Service binds one Transport and Codec to a dispatch Handler.
	Service = server.Service
	// Client opens calls through one Transport and Codec.
	Client = client.Client
	// TransportClientOption configures client dial/open.
	TransportClientOption = transport.ClientOption
	// TransportServerOption configures server listen.
	TransportServerOption = transport.ServerOption
)

const (
	OK              = errs.OK
	InvalidArgument = errs.InvalidArgument
	Unauthenticated = errs.Unauthenticated
	NotFound        = errs.NotFound
	Unimplemented   = errs.Unimplemented
	Internal        = errs.Internal
	Unknown         = errs.Unknown
)

// Error returns an error carrying code and msg.
func Error(code Code, msg string) error {
	return errs.Error(code, msg)
}

// CodeOf returns the Code of an argos error, or Unknown for any other error.
func CodeOf(err error) Code {
	return errs.CodeOf(err)
}

// MetadataFromContext returns the writable Metadata on ctx, or nil if none was laid down.
func MetadataFromContext(ctx context.Context) Metadata {
	return metadata.FromContext(ctx)
}

// WithMetadata merges md into the Metadata already on ctx.
func WithMetadata(ctx context.Context, md Metadata) context.Context {
	return metadata.With(ctx, md)
}

// NewServer creates an empty Server.
func NewServer() *Server {
	return server.New()
}

// NewClient creates a Client configured by opts.
func NewClient(opts ...Option) *Client {
	return client.New(opts...)
}

// WithTransport sets Transport from an instance or a registered name ("http2", ...).
// For a name, import the transport subpackage so init registers the factory.
func WithTransport(v any) Option {
	return option.WithTransport(v)
}

// WithTransportInstance sets Transport from a concrete or interface value.
func WithTransportInstance[T Transport](t T) Option {
	return option.WithTransportInstance(t)
}

// WithTransportNamed sets Transport from a registered factory name.
func WithTransportNamed(name string) Option {
	return option.WithTransportNamed(name)
}

// WithCodec sets Codec from an instance or a registered name ("protobuf", ...).
// For a name, import the codec subpackage so init registers the factory.
func WithCodec(v any) Option {
	return option.WithCodec(v)
}

// WithCodecInstance sets Codec from a concrete or interface value.
func WithCodecInstance[T Codec](c T) Option {
	return option.WithCodecInstance(c)
}

// WithCodecNamed sets Codec from a registered factory name.
func WithCodecNamed(name string) Option {
	return option.WithCodecNamed(name)
}

// WithFilter appends a Filter. Server and client share the same type.
func WithFilter(f Filter) Option {
	return option.WithFilter(f)
}

// WithListenAddress sets the server listen address.
func WithListenAddress(addr string) Option {
	return option.WithListenAddress(addr)
}

// WithServerTransportOption appends server listen options.
func WithServerTransportOption(opts ...TransportServerOption) Option {
	return option.WithServerTransportOption(opts...)
}

// WithTarget sets a client dial target (scheme://service-identifier, e.g. ip://127.0.0.1:9090).
func WithTarget(target string) Option {
	return option.WithTarget(target)
}

// WithClientTransportOption appends client dial/open options.
func WithClientTransportOption(opts ...TransportClientOption) Option {
	return option.WithClientTransportOption(opts...)
}
