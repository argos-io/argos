package envelope

import "github.com/argos-io/argos/framing"

// Register adds the envelope framing factory to r under framing.NameEnvelope.
func Register(r *framing.Registry) error {
	return r.Register(framing.NameEnvelope, func() (framing.Framing, error) {
		return New(), nil
	})
}
