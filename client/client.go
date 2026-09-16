// Package client is the client-side composition layer: admission, OpenFilter
// chain, session pool, and CallStream.
package client

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

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

	tr      transport.Transport
	framing framing.Framing
	codec   codec.Codec
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

	leak    *clientLeakState
	cleanup runtime.Cleanup
}

// clientLeakState is what the Client's cleanup hook sees. Like the CallStream
// one it must not reference the Client, or the Client would never become
// unreachable and the hook would never run — which is also why the pool's
// DialFunc closes over the Transport instead of over the Client.
type clientLeakState struct {
	closed atomic.Bool
	cfg    *argos.Config
	target string
	pool   *sessionpool.Pool
	tr     transport.Transport
	cancel context.CancelFunc
}

// release is what both Close and the cleanup hook run.
func (st *clientLeakState) release() error {
	st.cancel()
	var first error
	if st.pool != nil {
		if err := st.pool.Close(); err != nil {
			first = err
		}
	}
	if st.tr != nil {
		if err := st.tr.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// New builds a Client from options only: the Config it starts from is the one
// named by argos.WithConfig, or the process default. argos.WithServiceName
// names the service the Client opens calls for and is required — generated
// stubs pass their own.
//
// It assembles the service's protocol once (no network I/O) and creates an
// empty session pool. Protocol and target come from Config.Services for the
// selected service name; a target is required before the first Open.
func New(opts ...argos.ClientOption) (*Client, error) {
	cfg, err := argos.ClientConfig(opts...)
	if err != nil {
		return nil, err
	}
	service, sel := cfg.SelectedService()
	if service == "" {
		return nil, fmt.Errorf("client: missing service name; pass argos.WithServiceName")
	}

	pc, err := cfg.PerCall()
	if err != nil {
		return nil, err
	}

	target := sel.Target
	axes, err := cfg.ResolveService(sel)
	if err != nil {
		return nil, fmt.Errorf("client: service %q: %w", service, err)
	}
	tr, fr, cd, err := axes.Assemble()
	if err != nil {
		return nil, fmt.Errorf("client: service %q: %w", service, err)
	}
	if err := checkFramingConfig(fr, cfg); err != nil {
		_ = tr.Close()
		if c, ok := fr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return nil, fmt.Errorf("client: %w", err)
	}

	life, cancelLife := context.WithCancel(context.Background())
	c := &Client{
		cfg:            cfg,
		service:        service,
		target:         target,
		tr:             tr,
		framing:        fr,
		codec:          cd,
		perCall:        pc,
		lifetime:       life,
		cancelLifetime: cancelLife,
		bufRemain:      cfg.MaxBufferedBytes,
	}

	codecName := ""
	if n, ok := cd.(codec.Named); ok {
		codecName = n.CodecName()
	}
	// dial closes over the Transport and the lifetime ctx, not over the
	// Client: the Client holds the pool, so a DialFunc pointing back at the
	// Client would make the pair reachable from the cleanup hook below and the
	// hook would never run.
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		select {
		case <-life.Done():
			return nil, ErrClosed
		default:
		}
		return tr.Dial(ctx, transport.DialSpec{Endpoint: endpoint})
	}
	c.pool = sessionpool.New(fr, dial, sessionpool.Config{
		MaxSessionsPerEndpoint: cfg.MaxSessionsPerEndpoint,
		MaxIdleSessions:        cfg.MaxIdleSessions,
		SessionIdleTimeout:     cfg.SessionIdleTimeout,
		MaxSessionLifetime:     cfg.MaxSessionLifetime,
		HandshakeTimeout:       cfg.HandshakeTimeout,
		SessionSpec: framing.SessionSpec{
			CodecName: codecName,
			Config: framing.Config{
				MaxMessageSize:  cfg.MaxMessageSize,
				MaxFrameSize:    cfg.MaxFrameSize,
				MaxMetadataSize: cfg.MaxMetadataSize,

				MaxInboundMetadataSize: cfg.MaxInboundMetadataSize,
				ReadAheadMessages:      cfg.ReadAheadMessages,
				OpenTimeout:            cfg.OpenTimeout,
				MaxDrainBytes:          cfg.MaxDrainBytes,
			},
		},
	})

	// Close is still the contract — it is the only way to release the sessions
	// and the pool's reclaim goroutine at a point the program chooses. This
	// hook is the safety net for a Client that is dropped instead: without it
	// a forgotten Close leaked a goroutine and every socket it held for the
	// life of the process.
	c.leak = &clientLeakState{
		cfg:    cfg,
		target: target,
		pool:   c.pool,
		tr:     tr,
		cancel: cancelLife,
	}
	c.cleanup = runtime.AddCleanup(c, func(st *clientLeakState) {
		if st.closed.Load() {
			return
		}
		argos.NotifyConnError(st.cfg, argos.ConnInfo{
			Side:     argos.SideClient,
			Endpoint: st.target,
			Phase:    argos.ConnPhaseClose,
		}, errors.New("client: Client leaked without Close"))
		_ = st.release()
	}, c.leak)
	return c, nil
}

// checkBindingConfig lets a Framing reject size limits its carrier cannot
// deliver, before any dial. Framings that do not implement it opt out.
func checkFramingConfig(fr framing.Framing, cfg *argos.Config) error {
	checker, ok := fr.(interface {
		CheckConfig(framing.Config) error
	})
	if !ok {
		return nil
	}
	return checker.CheckConfig(framing.Config{
		MaxMessageSize:         cfg.MaxMessageSize,
		MaxFrameSize:           cfg.MaxFrameSize,
		MaxMetadataSize:        cfg.MaxMetadataSize,
		MaxInboundMetadataSize: cfg.MaxInboundMetadataSize,
		ReadAheadMessages:      cfg.ReadAheadMessages,
		OpenTimeout:            cfg.OpenTimeout,
		MaxDrainBytes:          cfg.MaxDrainBytes,
	})
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
		return stream.Wrap(call, c.codec), nil
	}

	// abandonOpen unwinds a call the filter chain opened but will not return.
	// Dropping it here used to leak the framing.Call and pin the borrowed
	// session forever: the pool only reclaims entries whose refcount is zero,
	// and the entry kept counting against MaxSessionsPerEndpoint until the
	// client could no longer open anything.
	abandonOpen := func() {
		stopBridge()
		callCancel()
		if gotCall != nil {
			_ = gotCall.Close()
		}
		if gotSess != nil && c.pool != nil {
			c.pool.Release(gotSess)
		}
	}

	open := filter.ChainOpen(c.cfg.OpenFilters, terminus)
	st, err := open(callCtx, m)
	if err != nil {
		abandonOpen()
		return nil, err
	}
	if gotCall == nil {
		abandonOpen()
		return nil, fmt.Errorf("client: OpenFilter returned stream without opening a call")
	}
	if st == nil {
		// (nil, nil) after next: ChainOpen cannot see it, and every later
		// operation would call a method on a nil Stream inside a goroutine —
		// an unrecoverable process panic rather than a call error.
		abandonOpen()
		return nil, fmt.Errorf("%w: OpenFilter returned a nil stream after opening the call", filter.ErrOpenFilterMisuse)
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
		cfg:    c.cfg,
		client: c,
		info: argos.CallInfo{
			Service: c.service,
			Method:  m.Name(),
			Phase:   argos.PhaseLeak,
		},
	}
	cs.cleanup = runtime.AddCleanup(cs, func(st *leakState) {
		if st.closed.Load() {
			return
		}
		argos.NotifyCallError(st.cfg, st.info, errors.New("client: CallStream leaked without Close"))
		// Reclaim the admission reservation. Reporting alone left one slot of
		// MaxConcurrentCalls and perCall bytes of MaxBufferedBytes held for the
		// life of the Client, so a leak eventually produced ErrCallsExhausted
		// with no call in flight. The pooled session is deliberately not
		// released here: nobody called framing Call.Close, so the session's
		// demux state is unknown and handing it to another call would be worse
		// than losing it. stopWatch closes the call when the lifetime ctx ends.
		if st.client != nil {
			st.client.releaseAdmit()
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

	if c.leak == nil { // New always sets it; a zero Client has nothing to release.
		c.cancelLifetime()
		return nil
	}
	c.leak.closed.Store(true)
	c.cleanup.Stop()
	return c.leak.release()
}
