package echov1

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	envframing "github.com/argos-io/argos/framing/envelope"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/framing/wholebody"
	"github.com/argos-io/argos/transport"
	argoshttp1 "github.com/argos-io/argos/transport/http1"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

func grpcAxes() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return argos.TransportFunc(func() (transport.Transport, error) {
			return argoshttp2.New(), nil
		}),
		argos.FramingFunc(func() (framing.Framing, error) { return grpcframing.New() }),
		argos.CodecFunc(func() (codec.Codec, error) { return protobuf.New(), nil })
}

func envelopeTCPAxes() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return argos.TransportFunc(func() (transport.Transport, error) { return tcp.New(), nil }),
		argos.FramingFunc(func() (framing.Framing, error) { return envframing.New(), nil }),
		argos.CodecFunc(func() (codec.Codec, error) { return protobuf.New(), nil })
}

func envelopeWSAxes() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	read := ws.DefaultMaxReadBytes
	return argos.TransportFunc(func() (transport.Transport, error) {
			return ws.New(ws.WithMaxReadBytes(0)), nil
		}),
		argos.FramingFunc(func() (framing.Framing, error) {
			return envframing.New(envframing.WithMaxInboundWireBytes(read)), nil
		}),
		argos.CodecFunc(func() (codec.Codec, error) { return protobuf.New(), nil })
}

func envelopeUDPAxes() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return argos.TransportFunc(func() (transport.Transport, error) { return udp.New(), nil }),
		argos.FramingFunc(func() (framing.Framing, error) {
			return envframing.New(envframing.WithOneCallPerConn()), nil
		}),
		argos.CodecFunc(func() (codec.Codec, error) { return protobuf.New(), nil })
}

func wholebodyHTTP1Axes() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return argos.TransportFunc(func() (transport.Transport, error) { return argoshttp1.New(), nil }),
		argos.FramingFunc(func() (framing.Framing, error) { return wholebody.New(), nil }),
		argos.CodecFunc(func() (codec.Codec, error) { return jsoncodec.New(), nil })
}
