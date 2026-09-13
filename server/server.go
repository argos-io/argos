// Package server binds transports and codecs to method dispatchers.
package server

import (
	"context"
	"errors"

	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/option"
)

// Server owns the services served together.
type Server struct {
	services []*Service
}

// Service binds one Transport and Codec to a dispatch Handler.
type Service struct {
	binding binding
}

// New creates an empty Server.
func New() *Server {
	return &Server{}
}

// NewService adds a Service to the Server.
func (s *Server) NewService(opts ...option.Option) *Service {
	service := &Service{binding: binding{Config: option.NewConfig(opts...)}}
	s.services = append(s.services, service)
	return service
}

// Register sets the service dispatch Handler. A later call replaces it.
func (svc *Service) Register(dispatch filter.Handler) {
	svc.binding.dispatch = dispatch
}

// Run serves all Services until they stop. A listener error cancels the rest.
func (s *Server) Run(ctx context.Context) error {
	for _, service := range s.services {
		if _, err := service.binding.ResolveCodec(); err != nil {
			return err
		}
		if _, err := service.binding.ResolveTransport(); err != nil {
			return err
		}
	}

	if len(s.services) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan error, len(s.services))
	for _, service := range s.services {
		b := &service.binding
		tr, err := b.ResolveTransport()
		if err != nil {
			return err
		}
		go func() {
			results <- tr.ListenAndServe(ctx, b.invoke, b.ServerTransportOpts...)
		}()
	}

	var joined []error
	cancelled := false
	for range s.services {
		err := <-results
		if err != nil {
			joined = append(joined, err)
			if !cancelled {
				cancel()
				cancelled = true
			}
		}
	}
	return errors.Join(joined...)
}
