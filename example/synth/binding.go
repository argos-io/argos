package synth

import (
	"fmt"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

// rawCodec marshals []byte / string payloads without a schema.
type rawCodec struct{}

func (rawCodec) CodecName() string { return "raw" }

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("synth: raw codec: unsupported type %T", v)
	}
}

func (rawCodec) Unmarshal(b []byte, v any) error {
	switch dst := v.(type) {
	case *[]byte:
		*dst = append([]byte(nil), b...)
		return nil
	case *string:
		*dst = string(b)
		return nil
	default:
		return fmt.Errorf("synth: raw codec: unsupported type %T", v)
	}
}

var (
	_ codec.Codec = rawCodec{}
	_ codec.Named = rawCodec{}
)

func tcpFactories(opts ...Option) (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	captured := append([]Option(nil), opts...)
	return argos.TransportFunc(func() (transport.Transport, error) { return tcp.New(), nil }),
		argos.FramingFunc(func() (framing.Framing, error) { return New(captured...), nil }),
		argos.CodecFunc(func() (codec.Codec, error) { return rawCodec{}, nil })
}

// ServiceTCP installs synth × tcp on a service entry.
func ServiceTCP(opts ...Option) argos.ServiceOption {
	t, f, c := tcpFactories(opts...)
	return argos.JoinService(
		argos.ServiceTransport(t),
		argos.ServiceFraming(f),
		argos.ServiceCodec(c),
	)
}

// ClientTCP is the call-site preset for synth × tcp.
func ClientTCP(opts ...Option) argos.ClientOption {
	t, f, c := tcpFactories(opts...)
	return argos.JoinClient(
		argos.WithTransport(t),
		argos.WithFraming(f),
		argos.WithCodec(c),
	)
}

// TCPAxes returns the three factories (tests that wrap transport).
func TCPAxes(opts ...Option) (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return tcpFactories(opts...)
}
