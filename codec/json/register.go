package json

import "github.com/argos-io/argos/codec"

// Register adds the json codec factory to r under codec.NameJSON.
func Register(r *codec.Registry) error {
	return r.Register(codec.NameJSON, func() (codec.Codec, error) {
		return New(), nil
	})
}
