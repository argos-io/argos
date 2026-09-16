package tcp

import "github.com/argos-io/argos/transport"

// Register adds the tcp transport factory to r under transport.NameTCP.
func Register(r *transport.Registry) error {
	return r.Register(transport.NameTCP, func() (transport.Transport, error) {
		return New(), nil
	})
}
