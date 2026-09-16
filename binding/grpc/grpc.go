// Package grpc assembles grpc × http2 × protobuf (by default).
//
// Docs may import this package as grpcbinding. Compression and TLS are Options
// here — not argos.Option — and are forwarded to framing/grpc and
// transport/http2 respectively (§4.2 / §4.8).
package grpc

import (
	"crypto/tls"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/transport"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
)

// New returns a Protocol preset (http2 × grpc × protobuf unless overridden).
func New(opts ...Option) argos.Protocol {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	return protocol(o)
}

// Service returns a ServiceOption that installs New(opts...) on a service entry.
func Service(opts ...Option) argos.ServiceOption {
	return argos.ServiceProtocol(New(opts...))
}

func protocol(o options) argos.Protocol {
	var http2Opts []argoshttp2.Option
	if o.serverTLS != nil {
		http2Opts = append(http2Opts, argoshttp2.WithServerTLS(o.serverTLS))
	}
	if cli := clientTLSConfig(o); cli != nil {
		http2Opts = append(http2Opts, argoshttp2.WithClientTLS(cli))
	}
	frOpts := frOptsFrom(o)

	return argos.Protocol{
		Transport: func() (transport.Transport, error) {
			return argoshttp2.New(http2Opts...), nil
		},
		Framing: func() (framing.Framing, error) {
			return grpcframing.New(frOpts...)
		},
		Codec: func() (codec.Codec, error) {
			cd := o.codec
			if cd == nil {
				cd = protobuf.New()
			}
			return cd, nil
		},
	}
}

func frOptsFrom(o options) []grpcframing.Option {
	var frOpts []grpcframing.Option
	if len(o.compressors) > 0 {
		frOpts = append(frOpts, grpcframing.WithCompressors(o.compressors...))
	}
	if o.sendName != "" {
		frOpts = append(frOpts, grpcframing.WithSendCompressor(o.sendName))
	}
	return frOpts
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
