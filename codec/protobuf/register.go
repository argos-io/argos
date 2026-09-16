package protobuf

import "github.com/argos-io/argos/codec"

// Register adds the protobuf codec factory to r under codec.NameProtobuf.
func Register(r *codec.Registry) error {
	return r.Register(codec.NameProtobuf, func() (codec.Codec, error) {
		return New(), nil
	})
}
