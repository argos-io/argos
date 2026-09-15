// Package client is the client-side composition layer: admission, OpenFilter
// chain, session pool, and CallStream.
package client

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/resolver"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

// Client owns one Transport×Framing×Codec triple, a session pool, and
// instance-level admission (MaxConcurrentCalls + MaxBufferedBytes).
type Client struct {
	cfg     *argos.Config
	service string
	target  string

	binding argos.Binding
	pool    *sessionpool.Pool
	perCall int64

	lifetime       context.Context
	cancelLifetime context.CancelFunc

	mu     sync.Mutex
	closed bool

	// Admission: concurrent call slots + instance buffer pool.
	admitMu   sync.Mutex
	inFlight  int
	bufRemain int64
}

// New builds a Client for service. It invokes BindingFunc once (no network I/O),
// creates an empty session pool, and validates MaxConcurrentCalls×perCall
// against MaxBufferedBytes when opts are applied.
//
// Target resolution: ServiceTarget for service (via cfg.Services or opts),
// required before the first Open that reaches the pool.
func New(cfg *argos.Config, service string, opts ...argos.Option) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("client: nil Config")
	}
	if service == "" {
		return nil, fmt.Errorf("client: empty service name")
	}
	if len(opts) > 0 {
		var err error
		cfg, err = cfg.With(opts...)
		if err != nil {
			return nil, err
		}
	}
	if err := checkBufferedCapacity(cfg); err != nil {
		return nil, err
	}

	pc, err := cfg.PerCall()
	if err != nil {
		return nil, err
	}

	bindingFn, target := resolveBindingTarget(cfg, service)
	if bindingFn == nil {
		return nil, fmt.Errorf("client: no BindingFunc for service %q", service)
	}
	b, err := bindingFn()
	if err != nil {
		return nil, err
	}
	if b.Transport == nil || b.Framing == nil || b.Codec == nil {
		return nil, fmt.Errorf("client: Binding missing Transport, Framing, or Codec")
	}

	life, cancelLife := context.WithCancel(context.Background())
	c := &Client{
		cfg:            cfg,
		service:        service,
		target:         target,
		binding:        b,
		perCall:        pc,
		lifetime:       life,
		cancelLifetime: cancelLife,
		bufRemain:      cfg.MaxBufferedBytes,
	}

	codecName := ""
	if n, ok := b.Codec.(codec.Named); ok {
		codecName = n.CodecName()
	}
	c.pool = sessionpool.New(b.Framing, c.dial, sessionpool.Config{
		MaxSessionsPerEndpoint: cfg.MaxSessionsPerEndpoint,
		MaxIdleSessions:        cfg.MaxIdleSessions,
		SessionIdleTimeout:     cfg.SessionIdleTimeout,
		MaxSessionLifetime:      cfg.MaxSessionLifetime,
		HandshakeTimeout:       cfg.HandshakeTimeout,
		SessionSpec: framing.SessionSpec{
			CodecName: codecName,
			Config: framing.Config{
				MaxMessageSize:    cfg.MaxMessageSize,
				MaxFrameSize:      cfg.MaxFrameSize,
				MaxMetadataSize:   cfg.MaxMetadataSize,
				ReadAheadMessages: cfg.ReadAheadMessages,
				OpenTimeout:       cfg.OpenTimeout,
				MaxDrainBytes:     cfg.MaxDrainBytes,
			},
		},
	})
	return c, nil
}

func checkBufferedCapacity(cfg *argos.Config) error {
	pc, err := cfg.PerCall()
	if err != nil {
		return err
	}
	product, ok := mulNonNeg(int64(cfg.MaxConcurrentCalls), pc)
	if !ok || product > cfg.MaxBufferedBytes {
		return fmt.Errorf("client: MaxConcurrentCalls × perCall > MaxBufferedBytes (MaxConcurrentCalls, MaxFrameSize, MaxMessageSize, ReadAheadMessages, MaxBufferedBytes)")
	}
	return nil
}

func mulNonNeg(a, b int64) (int64, bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > (1<<63-1)/b {
		return 0, false
	}
	return a * b, true
}

func resolveBindingTarget(cfg *argos.Config, service string) (argos.BindingFunc, string) {
	fn := cfg.Binding
	target := ""
	if sc, ok := cfg.Services[service]; ok {
		if sc.Binding != nil {
			fn = sc.Binding
		}
		target = sc.Target
	}
	return fn, target
}

// dial is the sessionpool DialFunc. HandshakeTimeout is applied by the pool
// on a ctx that is not a child of any call ctx (§2.1).
func (c *Client) dial(ctx context.Context, endpoint string) (transport.Conn, error) {
	select {
	case <-c.lifetime.Done():
		return nil, ErrClosed
	default:
	}
	return c.binding.Transport.Dial(ctx, transport.DialSpec{Endpoint: endpoint})
}

// Open admits one call, runs the OpenFilter chain, and returns a CallStream.
func (c *Client) Open(ctx context.Context, m descriptor.Method) (*CallStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("client: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.IsZero() {
		return nil, fmt.Errorf("client: zero Method")
	}
	if m.Service() != c.service {
		return nil, fmt.Errorf("client: method service %q does not match client service %q", m.Service(), c.service)
	}

	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}

	if err := c.admit(); err != nil {
		return nil, err
	}
	admitted := true
	defer func() {
		if admitted {
			c.releaseAdmit()
		}
	}()

	md := metadata.New(metadata.RoleInitiator, nil)
	callBudget := budget.New(c.perCall)

	callCtx, callCancel := context.WithCancel(ctx)
	// Bridge client lifetime (stand-in for conn cancel on Client.Close) into
	// the call ctx. True per-session conn ctx lives in the pool; see 1.13b gaps.
	stopBridge := context.AfterFunc(c.lifetime, callCancel)

	callCtx = metadata.ContextWith(callCtx, md)
	callCtx = budget.ContextWith(callCtx, callBudget)

	var (
		gotCall framing.Call
		gotSess framing.ClientSession
	)

	terminus := func(ctx context.Context, method descriptor.Method) (stream.Stream, error) {
		if method.FullName() != m.FullName() {
			return nil, fmt.Errorf("client: OpenFilter changed method from %q to %q", m.FullName(), method.FullName())
		}
		target := c.target
		if target == "" {
			return nil, fmt.Errorf("client: missing target for service %q", c.service)
		}
		endpoint, err := resolver.Parse(ctx, target)
		if err != nil {
			return nil, err
		}
		call, sess, err := c.pool.OpenCall(ctx, endpoint, method, framing.CallSpec{Metadata: md})
		if err != nil {
			return nil, mapEstablishErr(err)
		}
		gotCall, gotSess = call, sess
		return stream.Wrap(call, c.binding.Codec), nil
	}

	open := filter.ChainOpen(c.cfg.OpenFilters, terminus)
	st, err := open(callCtx, m)
	if err != nil {
		stopBridge()
		callCancel()
		return nil, err
	}
	if gotCall == nil {
		stopBridge()
		callCancel()
		return nil, fmt.Errorf("client: OpenFilter returned stream without opening a call")
	}

	cs := &CallStream{
		client:     c,
		stream:     st,
		call:       gotCall,
		sess:       gotSess,
		md:         md,
		callCtx:    callCtx,
		callCancel: callCancel,
		stopBridge: stopBridge,
		headersCh:  make(chan struct{}),
	}
	// Caller cancel / lifetime cancel must unblock framing Recv/Send.
	cs.stopWatch = context.AfterFunc(callCtx, func() {
		_ = gotCall.Close()
	})
	cs.leak = &leakState{
		cfg: c.cfg,
		info: argos.CallInfo{
			Service: c.service,
			Method:  m.Name(),
			Phase:   argos.PhaseLeak,
		},
	}
	cs.cleanup = runtime.AddCleanup(cs, func(st *leakState) {
		if !st.closed.Load() {
			argos.NotifyCallError(st.cfg, st.info, errors.New("client: CallStream leaked without Close"))
		}
	}, cs.leak)

	admitted = false // CallStream.Close releases admission
	return cs, nil
}

func (c *Client) admit() error {
	c.admitMu.Lock()
	defer c.admitMu.Unlock()
	if c.inFlight >= c.cfg.MaxConcurrentCalls {
		return status.ErrCallsExhausted
	}
	if c.perCall > c.bufRemain {
		return status.ErrCallsExhausted
	}
	c.inFlight++
	c.bufRemain -= c.perCall
	return nil
}

func (c *Client) releaseAdmit() {
	c.admitMu.Lock()
	defer c.admitMu.Unlock()
	if c.inFlight > 0 {
		c.inFlight--
	}
	c.bufRemain += c.perCall
}

// Close rejects new Opens, cancels in-flight calls via the lifetime ctx,
// and closes the session pool. Idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.cancelLifetime()
	var first error
	if c.pool != nil {
		if err := c.pool.Close(); err != nil {
			first = err
		}
	}
	if c.binding.Transport != nil {
		if err := c.binding.Transport.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
