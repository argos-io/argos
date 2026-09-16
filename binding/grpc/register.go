package grpc

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/transport"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
)

func init() {
	cfg := argos.DefaultConfig()
	_ = cfg.RegisterTransport(argos.TransportHTTP2, func() (transport.Transport, error) {
		return argoshttp2.New(), nil
	})
	_ = cfg.RegisterFraming(argos.FramingGRPC, func() (framing.Framing, error) {
		return grpcframing.New()
	})
	_ = cfg.RegisterCodec(argos.CodecProtobuf, func() (codec.Codec, error) {
		return protobuf.New(), nil
	})
}
