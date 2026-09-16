package echov1

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	codecjson "github.com/argos-io/argos/codec/json"
	codecprotobuf "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	framingenvelope "github.com/argos-io/argos/framing/envelope"
	framinggrpc "github.com/argos-io/argos/framing/grpc"
	framingwholebody "github.com/argos-io/argos/framing/wholebody"
	"github.com/argos-io/argos/transport"
	transporthttp1 "github.com/argos-io/argos/transport/http1"
	transporthttp2 "github.com/argos-io/argos/transport/http2"
	transporttcp "github.com/argos-io/argos/transport/tcp"
	transportudp "github.com/argos-io/argos/transport/udp"
	transportws "github.com/argos-io/argos/transport/ws"
)

// DemoRegistries returns transport, framing, and codec registries with every
// axis used by the echo demo. Call from main or tests, then pass
// argos.WithTransportRegistry / WithFramingRegistry / WithCodecRegistry.
func DemoRegistries() (*transport.Registry, *framing.Registry, *codec.Registry, error) {
	tr := transport.NewRegistry()
	for _, reg := range []func(*transport.Registry) error{
		transporttcp.Register,
		transportws.Register,
		transportudp.Register,
		transporthttp1.Register,
		transporthttp2.Register,
	} {
		if err := reg(tr); err != nil {
			return nil, nil, nil, fmt.Errorf("transport: %w", err)
		}
	}
	fr := framing.NewRegistry()
	for _, reg := range []func(*framing.Registry) error{
		framingenvelope.Register,
		framinggrpc.Register,
		framingwholebody.Register,
	} {
		if err := reg(fr); err != nil {
			return nil, nil, nil, fmt.Errorf("framing: %w", err)
		}
	}
	co := codec.NewRegistry()
	for _, reg := range []func(*codec.Registry) error{
		codecprotobuf.Register,
		codecjson.Register,
	} {
		if err := reg(co); err != nil {
			return nil, nil, nil, fmt.Errorf("codec: %w", err)
		}
	}
	return tr, fr, co, nil
}
