// Package server binds transports and codecs to method dispatchers.
package server

import (
	"context"
	"errors"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

// Server owns the services served together.
type Server struct {
	services []*Service
}

// Service binds one Transport and Codec to a dispatch Handler.
type Service struct {
	binding binding
}

// MethodInfo describes the message directions of a registered RPC method.
// Generated stubs use it so the server can reject a streaming method on a
// transport that only implements unary calls.
type MethodInfo struct {
	Method string
	Kind   stream.CallKind
}

// New creates an empty Server.
func New() *Server {
	return &Server{}
}

// NewService adds a Service to the Server.
func (s *Server) NewService(opts ...argos.Option) *Service {
	service := &Service{binding: binding{Config: argos.NewConfig(opts...)}}
	s.services = append(s.services, service)
	return service
}

// Register sets the service dispatch Handler. A later call replaces it.
func (svc *Service) Register(dispatch filter.Handler) {
	svc.binding.dispatch = dispatch
	svc.binding.methods = nil
}

// RegisterWithMethods sets the service dispatch Handler and records the call
// shape of each method. A later call replaces both the handler and method
// metadata.
func (svc *Service) RegisterWithMethods(dispatch filter.Handler, methods ...MethodInfo) {
	svc.binding.dispatch = dispatch
	svc.binding.methods = make(map[string]stream.CallKind, len(methods))
	for _, method := range methods {
		if method.Method != "" {
			svc.binding.methods[method.Method] = method.Kind
		}
	}
}

// Run serves all Services until one of them stops. The first stopped listener
// cancels the rest; a normal stop does not become an error just because it
// caused sibling listeners to return context.Canceled.
func (s *Server) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("argos: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, service := range s.services {
		if err := service.binding.ValidateCompatibility(); err != nil {
			return err
		}
	}

	if len(s.services) == 0 {
		return nil
	}

	parentCtx := ctx
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Keep the instances validated in the preflight pass so listener startup
	// itself cannot introduce a new configuration-resolution error.
	resolved := make([]transport.Transport, len(s.services))
	for i := range s.services {
		tr, err := s.services[i].binding.ResolveTransport()
		if err != nil {
			return err
		}
		resolved[i] = tr
	}

	results := make(chan error, len(s.services))
	for i := range s.services {
		b := &s.services[i].binding
		tr := resolved[i]
		go func(tr transport.Transport, b *binding) {
			results <- tr.ListenAndServe(ctx, b.invoke, b.ServerTransportOpts...)
		}(tr, b)
	}

	var joined []error
	stopped := false
	internalCancel := false
	externalCanceledAtStop := false
	for range s.services {
		err := <-results
		if err != nil && !(internalCancel && !externalCanceledAtStop && errors.Is(err, context.Canceled)) {
			joined = append(joined, err)
		}
		if !stopped {
			cancel()
			stopped = true
			internalCancel = true
			externalCanceledAtStop = parentCtx.Err() != nil
		}
	}
	return errors.Join(joined...)
}
