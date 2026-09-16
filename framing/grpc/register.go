package grpc

import "github.com/argos-io/argos/framing"

// Register adds the grpc framing factory to r under framing.NameGRPC.
func Register(r *framing.Registry) error {
	return r.Register(framing.NameGRPC, func() (framing.Framing, error) {
		return New()
	})
}
