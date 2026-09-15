package resp

import (
	"fmt"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/transport/tcp"
)

// NewBinding returns a BindingFunc for resp × tcp (Sequential) with a
// passthrough BytesCodec. Each invocation creates fresh Transport/Framing/Codec
// instances and does not Dial or Serve.
func NewBinding(opts ...Option) argos.BindingFunc {
	// Capture options by value so concurrent BindingFunc calls don't share
	// a mutated Framing; New() is called inside the factory.
	captured := append([]Option(nil), opts...)
	return func() (argos.Binding, error) {
		fr := New(captured...)
		tr := tcp.New()
		if tr == nil || fr == nil {
			return argos.Binding{}, fmt.Errorf("resp: nil Transport or Framing")
		}
		return argos.Binding{
			Transport: tr,
			Framing:   fr,
			Codec:     NewBytesCodec(),
		}, nil
	}
}
