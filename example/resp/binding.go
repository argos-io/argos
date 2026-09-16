package resp

import (
	"fmt"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

// NewBinding returns a Protocol preset for resp × tcp (Sequential) with a
// passthrough BytesCodec. Each Assemble creates fresh instances.
func NewBinding(opts ...Option) argos.Protocol {
	captured := append([]Option(nil), opts...)
	return argos.Protocol{
		Transport: func() (transport.Transport, error) {
			tr := tcp.New()
			if tr == nil {
				return nil, fmt.Errorf("resp: nil Transport")
			}
			return tr, nil
		},
		Framing: func() (framing.Framing, error) {
			fr := New(captured...)
			if fr == nil {
				return nil, fmt.Errorf("resp: nil Framing")
			}
			return fr, nil
		},
		Codec: func() (codec.Codec, error) { return NewBytesCodec(), nil },
	}
}
