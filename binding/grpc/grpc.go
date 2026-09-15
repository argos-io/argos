// Package grpc assembles the grpc × http2 Binding: transport/http2,
// framing/grpc, and a Codec (protobuf by default).
//
// Docs may import this package as grpcbinding. Compression and TLS are
// Options here — not argos.Option — and are forwarded to framing/grpc and
// transport/http2 respectively (§4.2 / §4.8).
package grpc

import (
	"crypto/tls"
	"fmt"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec/protobuf"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
)

// New returns a BindingFunc that builds a fresh Transport × Framing × Codec
// triple on every call. It must not Dial or Serve.
func New(opts ...Option) argos.BindingFunc {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	return func() (argos.Binding, error) {
		return assemble(o)
	}
}

func assemble(o options) (argos.Binding, error) {
	var http2Opts []argoshttp2.Option
	if o.serverTLS != nil {
		http2Opts = append(http2Opts, argoshttp2.WithServerTLS(o.serverTLS))
	}
	if cli := clientTLSConfig(o); cli != nil {
		http2Opts = append(http2Opts, argoshttp2.WithClientTLS(cli))
	}

	var frOpts []grpcframing.Option
	if len(o.compressors) > 0 {
		frOpts = append(frOpts, grpcframing.WithCompressors(o.compressors...))
	}
	if o.sendName != "" {
		frOpts = append(frOpts, grpcframing.WithSendCompressor(o.sendName))
	}

	fr, err := grpcframing.New(frOpts...)
	if err != nil {
		return argos.Binding{}, fmt.Errorf("binding/grpc: framing: %w", err)
	}

	cd := o.codec
	if cd == nil {
		cd = protobuf.New()
	}

	return argos.Binding{
		Transport: argoshttp2.New(http2Opts...),
		Framing:   fr,
		Codec:     cd,
	}, nil
}

func clientTLSConfig(o options) *tls.Config {
	if o.clientTLS == nil && o.authority == "" {
		return nil
	}
	var cfg *tls.Config
	if o.clientTLS != nil {
		cfg = o.clientTLS.Clone()
	} else {
		cfg = &tls.Config{}
	}
	if o.authority != "" && cfg.ServerName == "" {
		cfg.ServerName = o.authority
	}
	return cfg
}
