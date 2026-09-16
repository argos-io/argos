package ws

import "github.com/argos-io/argos/transport"

// Register adds the ws transport factory to r under transport.NameWS.
func Register(r *transport.Registry) error {
	return r.Register(transport.NameWS, func() (transport.Transport, error) {
		return New(), nil
	})
}
