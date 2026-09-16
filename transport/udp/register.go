package udp

import "github.com/argos-io/argos/transport"

// Register adds the udp transport factory to r under transport.NameUDP.
func Register(r *transport.Registry) error {
	return r.Register(transport.NameUDP, func() (transport.Transport, error) {
		return New(), nil
	})
}
