package resp

import (
	"context"
	"fmt"
	"sync"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
)

// Store is an in-process fake Redis key/value map (no redis dependency).
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Get returns the value and whether the key exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Set stores key → value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Methods for the default redis.Redis service.
var (
	MethodPING = descriptor.MustMethod(defaultService+".PING", descriptor.Unary)
	MethodGET  = descriptor.MustMethod(defaultService+".GET", descriptor.Unary)
	MethodSET  = descriptor.MustMethod(defaultService+".SET", descriptor.Unary)
)

// ServiceDescriptor returns the redis.Redis service with PING/GET/SET.
func ServiceDescriptor() descriptor.Service {
	return descriptor.MustService(defaultService, MethodPING, MethodGET, MethodSET)
}

// Register attaches PING/GET/SET handlers backed by store to srv.
func Register(srv *server.Server, store *Store) error {
	if store == nil {
		return fmt.Errorf("resp: nil Store")
	}
	return srv.Register(ServiceDescriptor(), map[string]filter.Handler{
		"PING": handlePING,
		"GET":  handleGET(store),
		"SET":  handleSET(store),
	})
}

func drainUnary(st stream.Stream) {
	var extra []byte
	_ = st.Recv(&extra) // expect io.EOF after the single request message
}

func handlePING(_ context.Context, _ descriptor.Method, st stream.Stream) error {
	var args []byte
	if err := st.Recv(&args); err != nil {
		return err
	}
	drainUnary(st)
	return st.Send(EncodeSimple("PONG"))
}

func handleGET(store *Store) filter.Handler {
	return func(_ context.Context, _ descriptor.Method, st stream.Stream) error {
		var raw []byte
		if err := st.Recv(&raw); err != nil {
			return err
		}
		drainUnary(st)
		args, err := DecodeArgs(raw)
		if err != nil {
			return status.Error(status.InvalidArgument, err.Error())
		}
		if len(args) != 1 {
			return status.Error(status.InvalidArgument, "GET requires 1 arg")
		}
		if v, ok := store.Get(args[0]); ok {
			return st.Send(EncodeBulk(v))
		}
		return st.Send(EncodeBulkNull())
	}
}

func handleSET(store *Store) filter.Handler {
	return func(_ context.Context, _ descriptor.Method, st stream.Stream) error {
		var raw []byte
		if err := st.Recv(&raw); err != nil {
			return err
		}
		drainUnary(st)
		args, err := DecodeArgs(raw)
		if err != nil {
			return status.Error(status.InvalidArgument, err.Error())
		}
		if len(args) != 2 {
			return status.Error(status.InvalidArgument, "SET requires 2 args")
		}
		store.Set(args[0], args[1])
		return st.Send(EncodeSimple("OK"))
	}
}
