package wholebody

import "github.com/argos-io/argos/framing"

// Register adds the wholebody framing factory to r under framing.NameWholebody.
func Register(r *framing.Registry) error {
	return r.Register(framing.NameWholebody, func() (framing.Framing, error) {
		return New(), nil
	})
}
