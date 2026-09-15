package synth

import (
	"fmt"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
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

// NewTCP returns a BindingFunc for synth Framing × tcp (Sequential).
// Each invocation returns fresh Transport and Framing instances.
func NewTCP(opts ...Option) argos.BindingFunc {
	return func() (argos.Binding, error) {
		return argos.Binding{
			Transport: tcp.New(),
			Framing:   New(opts...),
			Codec:     rawCodec{},
		}, nil
	}
}
