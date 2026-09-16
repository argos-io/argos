package http1

import "github.com/argos-io/argos/transport"

// Register adds the http1 transport factory to r under transport.NameHTTP1.
func Register(r *transport.Registry) error {
	return r.Register(transport.NameHTTP1, func() (transport.Transport, error) {
		return New(), nil
	})
}
