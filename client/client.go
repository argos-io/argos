// Package client is the client-side composition layer: admission, OpenFilter
// chain, and CallStream. Connectivity belongs to the protocol axis.
package client

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/establish"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/resolver"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

// opener is the client-side surface of one protocol transport instance.
type opener interface {
	OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error)
}

// Client owns one protocol (transport + codec) and instance-level admission
// (MaxConcurrentCalls).
//
// It has no Close. Everything a Close used to release belongs to the axis:
// connections and the pool are the axis' business and the axis' constructor
// closes them, while admission is returned by CallStream.Close. Dropping a
// Client therefore leaks nothing that was not already released, and a call in
// flight is governed by the ctx it was opened with — which is the only lifetime
// the caller ever expressed.
type Client struct {
	cfg     *argos.Options
	service string
	target  string

	codec  codec.Codec
	opener opener

	admitMu  sync.Mutex
	inFlight int
}

// New builds a Client from options only: the Options it starts from is the one
// named by argos.WithClientOptions, or the process default. argos.WithServiceName
// names the service the Client opens calls for and is required — generated
// stubs pass their own.
//
// It assembles the service's protocol once (no network I/O). Protocol and
// target come from Options.Services for the selected service name; a target is
// required before the first Open.
func New(opts ...argos.ClientOption) (*Client, error) {
	cfg, err := argos.ClientOptions(opts...)
	if err != nil {
		return nil, err
	}
	service, sel := cfg.SelectedService()
	if service == "" {
		return nil, fmt.Errorf("client: missing service name; pass argos.WithServiceName")
	}

	target := sel.Target
	op, cd, err := newOpener(cfg, service, sel)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:     cfg,
		service: service,
		target:  target,
		codec:   cd,
		opener:  op,
	}, nil
}

func newOpener(cfg *argos.Options, service string, sel argos.ServiceOptions) (opener, codec.Codec, error) {
	if sel.Transport == "" {
		return nil, nil, fmt.Errorf("client: service %q: missing Transport", service)
	}
	tr, err := sel.AssembleTransport()
	if err != nil {
		return nil, nil, fmt.Errorf("client: service %q: %w", service, err)
	}
	cd, err := sel.AssembleCodec()
	if err != nil {
		return nil, nil, fmt.Errorf("client: service %q: %w", service, err)
	}
	what := fmt.Sprintf("client: service %q", service)
	if err := transportbind.CheckCodecName(what, tr.CodecName(), cd); err != nil {
		return nil, nil, err
	}
	return transportOpener{tr: tr}, cd, nil
}

// transportOpener uses a caller-supplied transport instance. It only opens
// calls: the axis owns its connections and its pool, and neither this type nor
// the Client that holds it may release them, because the same instance may be
// in use by other Clients and by a listen surface.
type transportOpener struct{ tr transport.Transport }

func (o transportOpener) OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	return o.tr.OpenCall(ctx, endpoint, m, spec)
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

	// The caller's ctx is the whole lifetime of this call: there is no Client
	// Close to cancel it from behind the caller's back, so a call ends when its
	// own ctx does.
	callCtx, callCancel := context.WithCancel(ctx)

	callCtx = metadata.ContextWith(callCtx, md)

	var gotCall transport.Call

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
		openCtx := establish.WithTimeout(ctx, c.cfg.HandshakeTimeout)
		call, err := c.opener.OpenCall(openCtx, endpoint, method, transport.CallSpec{Metadata: md})
		if err != nil {
			return nil, mapEstablishErr(err)
		}
		gotCall = call
		return stream.Wrap(call, c.codec), nil
	}

	// abandonOpen unwinds a call the filter chain opened but will not return.
	// Dropping it here used to leak the Call and pin the borrowed connection
	// forever: the axis only reclaims a connection whose calls have all ended,
	// and it kept counting against MaxSessionsPerEndpoint until the client
	// could no longer open anything.
	abandonOpen := func() {
		callCancel()
		if gotCall != nil {
			_ = gotCall.Close()
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
		md:         md,
		callCtx:    callCtx,
		callCancel: callCancel,
		headersCh:  make(chan struct{}),
		closedCh:   make(chan struct{}),
	}
	// Caller cancel must unblock the axis Recv/Send.
	gate := &afterCallGate{call: gotCall}
	cs.afterCallGate = gate
	cs.stopWatch = context.AfterFunc(callCtx, func() {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		_ = gate.call.Close()
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
		// MaxConcurrentCalls slot held for the
		// life of the Client, so a leak eventually produced ErrCallsExhausted
		// with no call in flight. The connection is deliberately not touched
		// here: only Call.Close returns it to the axis, and nobody called it,
		// so the call's wire state is unknown and handing the connection to
		// another call would be worse than losing it. stopWatch closes the call
		// when the lifetime ctx ends.
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
	if c.cfg.MaxConcurrentCalls > 0 && c.inFlight >= c.cfg.MaxConcurrentCalls {
		return status.ErrCallsExhausted
	}
	c.inFlight++
	return nil
}

func (c *Client) releaseAdmit() {
	c.admitMu.Lock()
	defer c.admitMu.Unlock()
	if c.inFlight > 0 {
		c.inFlight--
	}
}
