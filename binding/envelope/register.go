package envelope

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	envframing "github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

func init() {
	cfg := argos.DefaultConfig()
	_ = cfg.RegisterTransport(argos.TransportTCP, func() (transport.Transport, error) {
		return tcp.New(), nil
	})
	_ = cfg.RegisterTransport(argos.TransportWS, func() (transport.Transport, error) {
		return ws.New(), nil
	})
	_ = cfg.RegisterTransport(argos.TransportUDP, func() (transport.Transport, error) {
		return udp.New(), nil
	})
	_ = cfg.RegisterFraming(argos.FramingEnvelope, func() (framing.Framing, error) {
		return envframing.New(), nil
	})
	_ = cfg.RegisterCodec(argos.CodecProtobuf, func() (codec.Codec, error) {
		return protobuf.New(), nil
	})
}
