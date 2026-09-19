package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/transport"
)

// Server is the composition-layer server: Link.Serve → AcceptCall loop →
// admit → route → Accept → Filter → Finish/Close (§4.1 / §5.2).
type Server struct {
	cfg    *argos.Options
	cfgErr error // option set New rejected; returned by Run
	admit  *admitGate

	mu      sync.Mutex
	routes  map[string]map[string]routeEntry // service → method → entry
	running bool
	// ran latches when Serve is launched, not when Run is entered: a start that
	// aborts before any listen surface is live leaves the Server as it was, so
	// Run reports the real reason on the next call instead of claiming it
	// already ran.
	ran bool
	// stopped latches when Shutdown is called, so a Run that starts afterwards
	// is refused instead of quietly serving on a server someone already stopped.
	stopped bool

	runCtx    context.Context
	runCancel context.CancelCauseFunc

	wg       sync.WaitGroup
	serveErr atomic.Pointer[error]
}

type listenReg struct {
	stack argos.ServiceOptions
	cfg   *argos.Options
	name  string
}

type routeEntry struct {
	method  descriptor.Method
	handler filter.Handler
}

type liveBinding struct {
	name        string
	transport   transport.Transport
	codec       codec.Codec
	cfg         *argos.Options
	concurrency transport.Concurrency
	active      atomic.Int64
}

// New constructs a Server from options only: the Options it starts from is the
// one named by argos.WithServerOptions, or the process default.
//
// New does not return an error so that a Server value is always usable as a
// receiver. A rejected option set is remembered and returned by Run, which is
// the first point where it can matter.
func New(opts ...argos.ServerOption) *Server {
	cfg, err := argos.ServerOptions(opts...)
	if err != nil {
		// Keep valid Options so the admission gate and every later method
		// have real numbers to work with; the error is what callers see.
		fallback := argos.Defaults()
		cfg = &fallback
	}
	return &Server{
		cfg:    cfg,
		cfgErr: err,
		admit:  newAdmitGate(cfg.MaxConcurrentCalls),
		routes: make(map[string]map[string]routeEntry),
	}
}

// Register adds a service descriptor and per-method handlers (keyed by short
// method name). Must be called before Run.
func (s *Server) Register(d descriptor.Service, handlers map[string]filter.Handler) error {
	if d.FullName() == "" {
		return fmt.Errorf("server: empty service")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ran || s.running {
		return fmt.Errorf("server: Register after Run")
	}
	if s.stopped {
		return fmt.Errorf("server: closed")
	}
	if _, ok := s.routes[d.FullName()]; ok {
		return fmt.Errorf("server: service %q already registered", d.FullName())
	}
	methods := d.Methods()
	byName := make(map[string]descriptor.Method, len(methods))
	for _, m := range methods {
		byName[m.Name()] = m
	}
	entry := make(map[string]routeEntry, len(handlers))
	for name, h := range handlers {
		if h == nil {
			return fmt.Errorf("server: nil handler for %s.%s", d.FullName(), name)
		}
		m, ok := byName[name]
		if !ok {
			return fmt.Errorf("server: handler for unknown method %s.%s", d.FullName(), name)
		}
		entry[name] = routeEntry{method: m, handler: h}
	}
	for _, m := range methods {
		if _, ok := entry[m.Name()]; !ok {
			return fmt.Errorf("server: missing handler for %s", m.FullName())
		}
	}
	s.routes[d.FullName()] = entry
	return nil
}

// Run starts listen surfaces from Options.Services for each registered service,
// then blocks until they exit or ctx is canceled.
func (s *Server) Run(ctx context.Context) error {
	if s.cfgErr != nil {
		return s.cfgErr
	}
	if ctx == nil {
		return fmt.Errorf("server: nil context")
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return fmt.Errorf("server: closed")
	}
	if s.ran || s.running {
		s.mu.Unlock()
		return fmt.Errorf("server: Run already called")
	}
	regs, err := s.buildListenRegs()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if len(regs) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("server: no registered services")
	}
	routes := cloneRoutes(s.routes)
	s.running = true
	runCtx, runCancel := context.WithCancelCause(ctx)
	s.runCtx = runCtx
	s.runCancel = runCancel
	s.mu.Unlock()

	lives := make([]*liveBinding, 0, len(regs))
	var startErr error
	for _, reg := range regs {
		link, cd, err := newBinding(reg.name, reg.stack)
		if err != nil {
			startErr = fmt.Errorf("server: listen %q: %w", reg.name, err)
			break
		}
		lives = append(lives, &liveBinding{
			name:        reg.name,
			transport:   link,
			codec:       cd,
			cfg:         reg.cfg,
			concurrency: link.CallConcurrency(),
		})
	}
	if startErr != nil {
		// Nothing to release: no surface was served yet, and the axes belong to
		// whoever constructed them — they may be serving someone else already.
		s.mu.Lock()
		s.running = false
		runCancel(startErr)
		s.runCtx = nil
		s.runCancel = nil
		s.mu.Unlock()
		return startErr
	}

	s.mu.Lock()
	s.ran = true
	s.mu.Unlock()

	for _, lb := range lives {
		lb := lb
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			var serveOpts []transport.ServerOption
			if lb.cfg.ListenAddress != "" {
				serveOpts = append(serveOpts, transport.WithListenAddress(lb.cfg.ListenAddress))
			}
			serveOpts = append(serveOpts,
				transport.WithHTTPReadHeaderTimeout(lb.cfg.HTTPReadHeaderTimeout),
				transport.WithHTTPIdleTimeout(lb.cfg.HTTPIdleTimeout),
			)
			err := lb.transport.Serve(runCtx, func(_ context.Context, c transport.ServerConn) {
				s.onConn(lb, routes, c)
			}, serveOpts...)
			if err != nil && runCtx.Err() == nil {
				// CompareAndSwap, not Store: two bindings can fail in the same
				// window, and atomic.Value panics when the two errors have
				// different dynamic types. First failure wins.
				s.serveErr.CompareAndSwap(nil, &err)
				runCancel(err)
			}
		}()
	}

	<-runCtx.Done()
	// Serve ended because runCtx ended, so every listener is already released.
	// The transports are not ours to close: whoever constructed them does.
	s.wg.Wait()

	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
	// Run is over for good (ran is latched), so the admission gate closes with
	// it. The aborted-start path above deliberately leaves the Server as it was
	// and does not come through here.
	s.admit.close()

	if p := s.serveErr.Load(); p != nil {
		return *p
	}
	cause := context.Cause(runCtx)
	if cause != nil && !errors.Is(cause, context.Canceled) && !errors.Is(cause, ErrServerShutdown) {
		return cause
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// Shutdown stops the server by canceling Run's context and waiting for Run to
// return. Idempotent.
//
// It does not touch the transports. They belong to whoever constructed them —
// the same instance may be serving another listen surface or a Client — so the
// only stop signal is the ctx Run was given, which Serve already honors.
//
// # What "stopped" means here
//
// Run returns once the listen surfaces have stopped, and that is all Shutdown
// waits for. It does NOT wait for handlers already in flight: those run on
// goroutines the transport owns, and the server never joined them. So a
// Shutdown that returns means the server accepts nothing new — not that the
// work it accepted is finished.
//
// Draining connections is therefore the axis owner's business, not the
// composition layer's: call the axis' own Shutdown if you need to wait for
// in-flight calls. ctx bounds only the wait for Run, and expiry returns
// ctx.Err() without forcing anything.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	s.stopped = true
	s.admit.close()
	runCancel := s.runCancel
	running := s.running
	s.mu.Unlock()

	if runCancel != nil {
		runCancel(ErrServerShutdown)
	}
	if !running {
		return nil
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneRoutes(in map[string]map[string]routeEntry) map[string]map[string]routeEntry {
	out := make(map[string]map[string]routeEntry, len(in))
	for svc, methods := range in {
		m := make(map[string]routeEntry, len(methods))
		for k, v := range methods {
			m[k] = v
		}
		out[svc] = m
	}
	return out
}

func (s *Server) buildListenRegs() ([]listenReg, error) {
	seen := make(map[string]struct{})
	var regs []listenReg
	for svcName := range s.routes {
		sc, ok := s.cfg.Services[svcName]
		if !ok {
			return nil, fmt.Errorf("server: service %q registered but missing from Options.Services", svcName)
		}
		plans, err := sc.ServerListenPlans(s.cfg.ListenAddress)
		if err != nil {
			return nil, fmt.Errorf("server: service %q: %w", svcName, err)
		}
		for i, plan := range plans {
			key := argos.ServiceListenKey(plan.Address, plan.Stack)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			cfg := s.cfg
			if plan.Address != "" && plan.Address != s.cfg.ListenAddress {
				cfg = cfg.Clone()
				cfg.ListenAddress = plan.Address
			}
			regs = append(regs, listenReg{
				stack: plan.Stack,
				cfg:   cfg,
				name:  fmt.Sprintf("%s-%d", svcName, i),
			})
		}
	}
	return regs, nil
}
