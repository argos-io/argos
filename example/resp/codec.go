package resp

import (
	"fmt"

	"github.com/argos-io/argos/codec"
)

// BytesCodec is a passthrough Codec: business messages are already RESP bytes.
// It proves the Codec axis can degenerate without a kernel hole.
type BytesCodec struct{}

// NewBytesCodec returns a passthrough []byte Codec named "raw".
func NewBytesCodec() codec.Codec { return BytesCodec{} }

func (BytesCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	case *[]byte:
		if x == nil {
			return nil, fmt.Errorf("resp: Marshal nil *[]byte")
		}
		return append([]byte(nil), (*x)...), nil
	case *string:
		if x == nil {
			return nil, fmt.Errorf("resp: Marshal nil *string")
		}
		return []byte(*x), nil
	default:
		return nil, fmt.Errorf("resp: BytesCodec unsupported type %T", v)
	}
}

func (BytesCodec) Unmarshal(b []byte, v any) error {
	switch x := v.(type) {
	case *[]byte:
		*x = append([]byte(nil), b...)
		return nil
	case *string:
		*x = string(b)
		return nil
	default:
		return fmt.Errorf("resp: BytesCodec unsupported type %T", v)
	}
}

func (BytesCodec) CodecName() string { return "raw" }

func init() {
	codec.Register("raw", func() (codec.Codec, error) { return NewBytesCodec(), nil })
}
