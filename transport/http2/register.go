package http2

import "github.com/argos-io/argos/transport"

// Register adds the http2 transport factory to r under transport.NameHTTP2.
func Register(r *transport.Registry) error {
	return r.Register(transport.NameHTTP2, func() (transport.Transport, error) {
		return New(), nil
	})
}
