package echov1

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/framing/wholebody"
	"github.com/argos-io/argos/transport"
	argoshttp1 "github.com/argos-io/argos/transport/http1"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
)

func GRPCAxes() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return argos.TransportFunc(func() (transport.Transport, error) {
			return argoshttp2.New(), nil
		}),
		argos.FramingFunc(func() (framing.Framing, error) { return grpcframing.New() }),
		argos.CodecFunc(func() (codec.Codec, error) { return protobuf.New(), nil })
}

func WholebodyHTTP1Axes() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return argos.TransportFunc(func() (transport.Transport, error) { return argoshttp1.New(), nil }),
		argos.FramingFunc(func() (framing.Framing, error) { return wholebody.New(), nil }),
		argos.CodecFunc(func() (codec.Codec, error) { return jsoncodec.New(), nil })
}

func ServiceAxes(tr argos.TransportFunc, fr argos.FramingFunc, cd argos.CodecFunc) []argos.ServiceOption {
	return []argos.ServiceOption{
		argos.ServiceTransport(tr),
		argos.ServiceFraming(fr),
		argos.ServiceCodec(cd),
	}
}
