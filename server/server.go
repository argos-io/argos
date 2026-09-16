package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// Server is the composition-layer server: Transport.Serve → AcceptCall loop →
// admit → route → Accept → Filter → Finish/Close (§4.1 / §5.2).
type Server struct {
	cfg    *argos.Config
	cfgErr error // option set New rejected; returned by AddBinding and Run
	admit  *admitGate

	mu             sync.Mutex
	bindings       []bindingReg
	routes         map[string]map[string]routeEntry // service → method → entry
	running        bool
	ran            bool
	closed         bool
	liveBindings   []*liveBinding
	liveTransports []transport.Transport

	// Per-connection cancels registered while onConn is active.
	acceptCancels map[*uint64]context.CancelFunc
	connCancels   map[*uint64]context.CancelCauseFunc

	runCtx    context.Context
	runCancel context.CancelCauseFunc

	wg       sync.WaitGroup
	serveErr atomic.Pointer[error]
}

type bindingReg struct {
	fn   argos.BindingFunc
	cfg  *argos.Config
	name string
}

type routeEntry struct {
	method  descriptor.Method
	handler filter.Handler
}

type liveBinding struct {
	name     string
	tr       transport.Transport
	framing  framing.Framing
	codec    codec.Codec
	cfg      *argos.Config
	reuse    framing.ReuseModel
	active   atomic.Int64
	sessSpec framing.SessionSpec
}

// New constructs a Server from options only: the Config it starts from is the
// one named by argos.WithConfig, or the process default.
//
// New does not return an error so that a Server value is always usable as a
// receiver. A rejected option set is remembered and returned by AddBinding and
// Run, which is the first point where it can matter.
func New(opts ...argos.ServerOption) *Server {
	cfg, err := argos.ServerConfig(opts...)
	if err != nil {
		// Keep a valid Config so the admission gate and every later method
		// have real numbers to work with; the error is what callers see.
		fallback := argos.Defaults()
		cfg = &fallback
	}
	perCall, cerr := cfg.PerCall()
	if cerr != nil {
		perCall = 0
	}
	return &Server{
		cfg:           cfg,
		cfgErr:        err,
		admit:         newAdmitGate(cfg.MaxConcurrentCalls, cfg.MaxBufferedBytes, perCall),
		routes:        make(map[string]map[string]routeEntry),
		acceptCancels: make(map[*uint64]context.CancelFunc),
		connCancels:   make(map[*uint64]context.CancelCauseFunc),
	}
}

// AddBinding registers a BindingFunc. Options overlay the server Config for
// this binding. Must be called before Run.
func (s *Server) AddBinding(fn argos.BindingFunc, opts ...argos.ServerOption) error {
	if s.cfgErr != nil {
		return s.cfgErr
	}
	if fn == nil {
		return fmt.Errorf("server: nil BindingFunc")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ran || s.running {
		return fmt.Errorf("server: AddBinding after Run")
	}
	if s.closed {
		return fmt.Errorf("server: closed")
	}
	cfg := s.cfg
	if len(opts) > 0 {
		// WithConfig names the base wherever it appears, so prepending the
		// server Config keeps a per-binding WithConfig in charge if the caller
		// passes one.
		var err error
		cfg, err = argos.ServerConfig(append([]argos.ServerOption{argos.WithConfig(s.cfg)}, opts...)...)
		if err != nil {
			return err
		}
	}
	name := fmt.Sprintf("binding-%d", len(s.bindings))
	s.bindings = append(s.bindings, bindingReg{fn: fn, cfg: cfg, name: name})
	return nil
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
	if s.closed {
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

// Run starts all bindings (factory once each) and blocks until they exit or
// ctx is canceled. Only one successful Run is allowed.
func (s *Server) Run(ctx context.Context) error {
	if s.cfgErr != nil {
		return s.cfgErr
	}
	if ctx == nil {
		return fmt.Errorf("server: nil context")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("server: closed")
	}
	if s.ran || s.running {
		s.mu.Unlock()
		return fmt.Errorf("server: Run already called")
	}
	if len(s.bindings) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("server: no bindings")
	}
	bindings := append([]bindingReg(nil), s.bindings...)
	routes := cloneRoutes(s.routes)
	s.running = true
	s.ran = true
	runCtx, runCancel := context.WithCancelCause(ctx)
	s.runCtx = runCtx
	s.runCancel = runCancel
	s.mu.Unlock()

	lives := make([]*liveBinding, 0, len(bindings))
	var startErr error
	for _, b := range bindings {
		triple, err := b.fn()
		if err != nil {
			startErr = err
			break
		}
		if triple.Transport == nil || triple.Framing == nil || triple.Codec == nil {
			startErr = fmt.Errorf("server: BindingFunc returned nil component")
			closeTriple(triple)
			break
		}
		// Let the Framing reject size limits its carrier cannot deliver, before
		// Serve. Framings that do not implement CheckConfig opt out.
		if checker, ok := triple.Framing.(interface {
			CheckConfig(framing.Config) error
		}); ok {
			if err := checker.CheckConfig(framing.Config{
				MaxMessageSize:         b.cfg.MaxMessageSize,
				MaxFrameSize:           b.cfg.MaxFrameSize,
				MaxMetadataSize:        b.cfg.MaxMetadataSize,
				MaxInboundMetadataSize: b.cfg.MaxInboundMetadataSize,
				ReadAheadMessages:      b.cfg.ReadAheadMessages,
				OpenTimeout:            b.cfg.OpenTimeout,
				MaxDrainBytes:          b.cfg.MaxDrainBytes,
			}); err != nil {
				startErr = fmt.Errorf("server: binding %q: %w", b.name, err)
				closeTriple(triple)
				break
			}
		}
		codecName := ""
		if n, ok := triple.Codec.(codec.Named); ok {
			codecName = n.CodecName()
		}
		lb := &liveBinding{
			name:    b.name,
			tr:      triple.Transport,
			framing: triple.Framing,
			codec:   triple.Codec,
			cfg:     b.cfg,
			reuse:   triple.Framing.Reuse(),
			sessSpec: framing.SessionSpec{
				CodecName: codecName,
				Config: framing.Config{
					MaxMessageSize:  b.cfg.MaxMessageSize,
					MaxFrameSize:    b.cfg.MaxFrameSize,
					MaxMetadataSize: b.cfg.MaxMetadataSize,

					MaxInboundMetadataSize: b.cfg.MaxInboundMetadataSize,
					ReadAheadMessages:      b.cfg.ReadAheadMessages,
					OpenTimeout:            b.cfg.OpenTimeout,
					MaxDrainBytes:          b.cfg.MaxDrainBytes,
				},
			},
		}
		lives = append(lives, lb)
	}
	if startErr != nil {
		for _, lb := range lives {
			_ = lb.tr.Close()
		}
		s.mu.Lock()
		s.running = false
		runCancel(startErr)
		s.mu.Unlock()
		return startErr
	}

	s.mu.Lock()
	s.liveBindings = lives
	s.liveTransports = make([]transport.Transport, len(lives))
	for i, lb := range lives {
		s.liveTransports[i] = lb.tr
	}
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
			serveOpts = append(serveOpts, transport.WithHTTPTimeouts(
				lb.cfg.HTTPReadHeaderTimeout, lb.cfg.HTTPIdleTimeout))
			err := lb.tr.Serve(runCtx, func(_ context.Context, c transport.Conn) {
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
	_ = s.Close()
	s.wg.Wait()

	s.mu.Lock()
	s.running = false
	s.mu.Unlock()

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

// Shutdown stops accepting new calls (cancels accept ctx only), shuts down
// transports, and waits for onConn to finish. On deadline it cancels connection
// contexts with ErrServerShutdown so in-flight calls surface Unavailable.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	for _, cancel := range s.acceptCancels {
		cancel()
	}
	trs := append([]transport.Transport(nil), s.liveTransports...)
	runCancel := s.runCancel
	s.mu.Unlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(trs))
	for _, tr := range trs {
		wg.Add(1)
		go func(tr transport.Transport) {
			defer wg.Done()
			if err := tr.Shutdown(ctx); err != nil {
				errCh <- err
			}
		}(tr)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		s.mu.Lock()
		for _, cancel := range s.connCancels {
			cancel(ErrServerShutdown)
		}
		s.mu.Unlock()
		<-done
		if runCancel != nil {
			runCancel(ErrServerShutdown)
		}
		s.wg.Wait()
		_ = s.closeLocked()
		return ctx.Err()
	}

	if runCancel != nil {
		runCancel(ErrServerShutdown)
	}
	s.wg.Wait()
	_ = s.closeLocked()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// Close immediately interrupts connections and releases resources. Idempotent.
func (s *Server) Close() error {
	return s.closeLocked()
}

func (s *Server) closeLocked() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.admit.close()
	for _, cancel := range s.acceptCancels {
		cancel()
	}
	for _, cancel := range s.connCancels {
		cancel(ErrServerShutdown)
	}
	trs := append([]transport.Transport(nil), s.liveTransports...)
	runCancel := s.runCancel
	s.mu.Unlock()

	if runCancel != nil {
		runCancel(ErrServerShutdown)
	}
	var first error
	for _, tr := range trs {
		if err := tr.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
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

func closeTriple(b argos.Binding) {
	if c, ok := b.Codec.(io.Closer); ok {
		_ = c.Close()
	}
	if c, ok := b.Framing.(io.Closer); ok {
		_ = c.Close()
	}
	if b.Transport != nil {
		_ = b.Transport.Close()
	}
}
