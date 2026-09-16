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

// NewTCP returns a Protocol preset for synth Framing × tcp (Sequential).
func NewTCP(opts ...Option) argos.Protocol {
	captured := append([]Option(nil), opts...)
	return argos.Protocol{
		Transport: func() (transport.Transport, error) { return tcp.New(), nil },
		Framing:   func() (framing.Framing, error) { return New(captured...), nil },
		Codec:     func() (codec.Codec, error) { return rawCodec{}, nil },
	}
}
