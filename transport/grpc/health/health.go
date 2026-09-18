// Package health registers the standard grpc.health.v1.Health service on an
// Argos server with semantics aligned to google.golang.org/grpc/health.
package health

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	// ServiceName is the gRPC health service IDL full name.
	ServiceName = "grpc.health.v1.Health"

	maxAllowedServices = 100
)

var (
	methodCheck = ServiceName + ".Check"
	methodList  = ServiceName + ".List"
	methodWatch = ServiceName + ".Watch"
)

// HealthServiceDesc is the descriptor for grpc.health.v1.Health.
var HealthServiceDesc = descriptor.MustService(ServiceName,
	descriptor.MustMethod(methodCheck, descriptor.Unary),
	descriptor.MustMethod(methodList, descriptor.Unary),
	descriptor.MustMethod(methodWatch, descriptor.ServerStreaming),
)

// Server implements grpc.health.v1.Health with grpc-go compatible behaviour.
type Server struct {
	mu sync.RWMutex
	// shutdown is true when all statuses are NOT_SERVING and stay there.
	shutdown  bool
	statusMap map[string]healthpb.HealthCheckResponse_ServingStatus
	updates   map[string]map[uint64]chan healthpb.HealthCheckResponse_ServingStatus
	nextWatch atomic.Uint64
}

// NewServer returns a health server whose overall status starts as SERVING.
func NewServer() *Server {
	return &Server{
		statusMap: map[string]healthpb.HealthCheckResponse_ServingStatus{
			"": healthpb.HealthCheckResponse_SERVING,
		},
		updates: make(map[string]map[uint64]chan healthpb.HealthCheckResponse_ServingStatus),
	}
}

// SetServingStatus sets the serving status of the given service.
func (s *Server) SetServingStatus(service string, serving healthpb.HealthCheckResponse_ServingStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shutdown {
		return
	}
	s.setServingStatusLocked(service, serving)
}

func (s *Server) setServingStatusLocked(service string, serving healthpb.HealthCheckResponse_ServingStatus) {
	s.statusMap[service] = serving
	for _, update := range s.updates[service] {
		select {
		case update <- serving:
		default:
		}
	}
}

// Shutdown sets all serving statuses to NOT_SERVING and rejects further updates.
func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdown = true
	for service := range s.statusMap {
		s.setServingStatusLocked(service, healthpb.HealthCheckResponse_NOT_SERVING)
	}
}

// Register registers hs on srv. The caller must also add a ServiceOptions entry
// for ServiceName on the same listen surface as the application services.
func Register(srv *server.Server, hs *Server) error {
	if hs == nil {
		return status.Error(status.InvalidArgument, "health: nil server")
	}
	return srv.Register(HealthServiceDesc, map[string]filter.Handler{
		"Check": checkHandler(hs),
		"List":  listHandler(hs),
		"Watch": watchHandler(hs),
	})
}

func checkHandler(hs *Server) filter.Handler {
	return func(_ context.Context, _ descriptor.Method, st stream.Stream) error {
		var req healthpb.HealthCheckRequest
		if err := st.Recv(&req); err != nil && err != io.EOF {
			return err
		}
		drainRecv(st)

		hs.mu.RLock()
		defer hs.mu.RUnlock()
		if servingStatus, ok := hs.statusMap[req.GetService()]; ok {
			return st.Send(&healthpb.HealthCheckResponse{Status: servingStatus})
		}
		return status.Error(status.NotFound, "unknown service")
	}
}

func listHandler(hs *Server) filter.Handler {
	return func(_ context.Context, _ descriptor.Method, st stream.Stream) error {
		var req healthpb.HealthListRequest
		if err := st.Recv(&req); err != nil && err != io.EOF {
			return err
		}
		drainRecv(st)

		hs.mu.RLock()
		defer hs.mu.RUnlock()
		if len(hs.statusMap) > maxAllowedServices {
			return status.Error(status.ResourceExhausted, "server health list exceeds maximum capacity: 100")
		}
		statusMap := make(map[string]*healthpb.HealthCheckResponse, len(hs.statusMap))
		for k, v := range hs.statusMap {
			statusMap[k] = &healthpb.HealthCheckResponse{Status: v}
		}
		return st.Send(&healthpb.HealthListResponse{Statuses: statusMap})
	}
}

func watchHandler(hs *Server) filter.Handler {
	return func(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
		var in healthpb.HealthCheckRequest
		if err := st.Recv(&in); err != nil {
			return err
		}
		service := in.GetService()
		update := make(chan healthpb.HealthCheckResponse_ServingStatus, 1)
		id := hs.nextWatch.Add(1)

		hs.mu.Lock()
		if servingStatus, ok := hs.statusMap[service]; ok {
			update <- servingStatus
		} else {
			update <- healthpb.HealthCheckResponse_SERVICE_UNKNOWN
		}
		if _, ok := hs.updates[service]; !ok {
			hs.updates[service] = make(map[uint64]chan healthpb.HealthCheckResponse_ServingStatus)
		}
		hs.updates[service][id] = update
		hs.mu.Unlock()
		defer func() {
			hs.mu.Lock()
			delete(hs.updates[service], id)
			hs.mu.Unlock()
		}()

		var lastSent healthpb.HealthCheckResponse_ServingStatus = -1
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case servingStatus := <-update:
				if lastSent == servingStatus {
					continue
				}
				lastSent = servingStatus
				if err := st.Send(&healthpb.HealthCheckResponse{Status: servingStatus}); err != nil {
					return err
				}
			}
		}
	}
}

func drainRecv(st stream.Stream) {
	for {
		var discard healthpb.HealthCheckRequest
		if err := st.Recv(&discard); err != nil {
			return
		}
	}
}
