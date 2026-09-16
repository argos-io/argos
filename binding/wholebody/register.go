package wholebody

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/framing"
	wbframing "github.com/argos-io/argos/framing/wholebody"
	"github.com/argos-io/argos/transport"
	argoshttp1 "github.com/argos-io/argos/transport/http1"
)

func init() {
	cfg := argos.DefaultConfig()
	_ = cfg.RegisterTransport(argos.TransportHTTP1, func() (transport.Transport, error) {
		return argoshttp1.New(), nil
	})
	_ = cfg.RegisterFraming(argos.FramingWholebody, func() (framing.Framing, error) {
		return wbframing.New(), nil
	})
	_ = cfg.RegisterCodec(argos.CodecJSON, func() (codec.Codec, error) {
		return jsoncodec.New(), nil
	})
}
