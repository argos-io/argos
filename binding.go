package argos

import (
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// Binding is one assembled Transport × Framing × Codec triple.
type Binding struct {
	Transport transport.Transport
	Framing   framing.Framing
	Codec     codec.Codec
}

// BindingFunc must create fresh, non-shared instances on every call.
// It must not Dial or Serve. The composition layer invokes it once per
// Client or Binding start (§4.2).
type BindingFunc func() (Binding, error)
