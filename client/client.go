// Package client opens calls through one Transport and Codec.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/selector"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"

	_ "github.com/argos-io/argos/selector/ip"
)

// Client opens calls through one Transport and Codec.
type Client struct {
	cfg argos.Config
}

// New creates a Client configured by opts.
func New(opts ...argos.Option) *Client {
	return &Client{cfg: argos.NewConfig(opts...)}
}

// Open opens method and runs call through the configured filters.
func (c *Client) Open(
	ctx context.Context,
	method string,
	call func(stream.Stream) error,
) (err error) {
	return c.open(ctx, method, stream.CallUnknown, call)
}

// OpenStream starts a long-lived streaming call. The returned CallStream is
// ready for Send and Recv once the configured filter chain has passed and the
// transport has been opened. Close cancels the call and releases its
// resources; it is safe to call more than once.
func (c *Client) OpenStream(ctx context.Context, method string, kind stream.CallKind) *CallStream {
	callStream := newCallStream()
	if ctx == nil {
		callStream.fail(errors.New("argos: nil context"))
		return callStream
	}
	if !kind.IsStreaming() {
		callStream.fail(errors.New("argos: OpenStream requires a streaming call kind"))
		return callStream
	}

	callCtx, cancel := context.WithCancel(ctx)
	callStream.setCancel(cancel)
	go func() {
		err := c.open(callCtx, method, kind, func(st stream.Stream) error {
			if !callStream.attach(st) {
				return context.Canceled
			}
			select {
			case <-callStream.done:
				return context.Canceled
			case <-callCtx.Done():
				return callCtx.Err()
			}
		})
		callStream.finish(err)
	}()
	return callStream
}

func (c *Client) open(
	ctx context.Context,
	method string,
	kind stream.CallKind,
	call func(stream.Stream) error,
) (err error) {
	if ctx == nil {
		return errors.New("argos: nil context")
	}
	if call == nil {
		return errors.New("argos: nil client call")
	}
	cd, err := c.cfg.ResolveCodec()
	if err != nil {
		return err
	}

	tr, opts, err := c.resolveTransport()
	if err != nil {
		return err
	}
	if err := c.cfg.ValidateCompatibility(); err != nil {
		return err
	}
	if kind.IsStreaming() {
		if capability, ok := tr.(transport.Streaming); ok && !capability.SupportsStreaming() {
			name := fmt.Sprintf("%T", tr)
			if named, ok := tr.(transport.Named); ok && named.TransportName() != "" {
				name = named.TransportName()
			}
			return fmt.Errorf("argos: transport %q does not support streaming calls", name)
		}
	}

	// Give this call an owned metadata map. Filters and transports are allowed
	// to add outgoing metadata, but must not mutate a caller's context value or
	// race with another call that reuses that context.
	ctx = metadata.With(ctx, metadata.Metadata{})
	callOpts := transport.ApplyClientOptions(opts)
	lazy := &lazyStream{
		ctx: ctx,
		open: func(openCtx context.Context) (transport.Framer, error) {
			openOpts := append([]transport.ClientOption(nil), opts...)
			if c.cfg.ClientTarget != "" {
				addr, err := selector.Parse(openCtx, c.cfg.ClientTarget)
				if err != nil {
					return nil, err
				}
				openOpts = append(openOpts, transport.WithDialAddress(addr))
			}
			return tr.Open(openCtx, method, openOpts...)
		},
		wrap: func(f transport.Framer) stream.Stream {
			return stream.WrapWithLimit(f, cd, callOpts.MaxMessageSize)
		},
	}
	callDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// A call may be blocked inside Stream.Recv while the transport is
			// waiting for network data; cancellation alone is not guaranteed to
			// interrupt that I/O. Close is the transport-independent abort path.
			_ = lazy.Close()
		case <-callDone:
		}
	}()
	defer close(callDone)
	defer func() {
		if closeErr := lazy.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	end := func(ctx context.Context, _ string, filtered stream.Stream) error {
		opened, openErr := lazy.ensure(ctx)
		if openErr != nil {
			return openErr
		}
		if filtered == lazy {
			filtered = opened
		}
		return call(filtered)
	}
	return filter.Chain(c.cfg.Filters, end)(ctx, method, lazy)
}

// CallStream is the lifecycle-safe, decoded stream returned by OpenStream.
// It is intentionally concrete so generated clients can expose a typed
// wrapper without leaking a transport or codec implementation.
type CallStream struct {
	done chan struct{}

	mu        sync.Mutex
	st        stream.Stream
	openErr   error
	termErr   error
	closed    bool
	cancel    context.CancelFunc
	closeFn   func() error
	closeErr  error
	ready     chan struct{}
	readyOnce sync.Once
	doneOnce  sync.Once
	closeOnce sync.Once
}

func newCallStream() *CallStream {
	return &CallStream{
		done:  make(chan struct{}),
		ready: make(chan struct{}),
	}
}

func (s *CallStream) setCancel(cancel context.CancelFunc) {
	s.mu.Lock()
	closed := s.closed
	s.cancel = cancel
	s.mu.Unlock()
	if closed && cancel != nil {
		cancel()
	}
}

func (s *CallStream) attach(st stream.Stream) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.signalReady()
		return false
	}
	s.st = st
	if closer, ok := st.(interface{ Close() error }); ok {
		s.closeFn = closer.Close
	}
	s.mu.Unlock()
	s.signalReady()
	return true
}

func (s *CallStream) fail(err error) {
	s.mu.Lock()
	if s.openErr == nil {
		s.openErr = err
	}
	s.mu.Unlock()
	s.signalReady()
}

func (s *CallStream) finish(err error) {
	s.mu.Lock()
	if s.st == nil {
		if s.openErr == nil {
			if err != nil {
				s.openErr = err
			} else {
				s.openErr = errors.New("argos: streaming call did not reach the call handler")
			}
		}
	} else if err != nil && !s.closed && s.termErr == nil {
		s.termErr = err
	}
	s.mu.Unlock()
	s.signalReady()
	if err != nil {
		s.closeDone()
	}
}

func (s *CallStream) signalReady() {
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *CallStream) closeDone() {
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *CallStream) get() (stream.Stream, error) {
	select {
	case <-s.ready:
	case <-s.done:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.termErr != nil {
		return nil, s.termErr
	}
	if s.openErr != nil {
		return nil, s.openErr
	}
	if s.closed || s.st == nil {
		return nil, context.Canceled
	}
	return s.st, nil
}

// Send sends one decoded message on the client-to-server direction.
func (s *CallStream) Send(v any) error {
	st, err := s.get()
	if err != nil {
		return err
	}
	if err := st.Send(v); err != nil {
		s.record(err)
		return err
	}
	return nil
}

// Recv receives one decoded message from the server-to-client direction.
func (s *CallStream) Recv(v any) error {
	st, err := s.get()
	if err != nil {
		return err
	}
	if err := st.Recv(v); err != nil {
		s.record(err)
		return err
	}
	return nil
}

// CloseSend half-closes the client-to-server direction.
func (s *CallStream) CloseSend() error {
	st, err := s.get()
	if err != nil {
		return err
	}
	if err := st.CloseSend(); err != nil {
		s.record(err)
		return err
	}
	return nil
}

// Close terminates the whole call and unblocks pending operations.
func (s *CallStream) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		cancel := s.cancel
		closeFn := s.closeFn
		s.mu.Unlock()
		s.closeDone()
		if cancel != nil {
			cancel()
		}
		if closeFn != nil {
			s.closeErr = closeFn()
		}
	})
	return s.closeErr
}

func (s *CallStream) record(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	closed := s.closed
	if !closed && s.termErr == nil {
		s.termErr = err
	}
	s.mu.Unlock()
	if !closed {
		_ = s.Close()
	}
}

func (c *Client) resolveTransport() (transport.Transport, []transport.ClientOption, error) {
	opts := append([]transport.ClientOption(nil), c.cfg.ClientTransportOpts...)
	tr, err := c.cfg.ResolveTransport()
	if err != nil {
		return nil, nil, err
	}
	return tr, opts, nil
}

// lazyStream lets client filters inspect and modify the call context before a
// transport allocates a connection. A filter that uses the stream before it
// calls next opens it with the context it currently has; the normal end path
// opens it with the final context passed through the filter chain.
type lazyStream struct {
	mu   sync.Mutex
	ctx  context.Context
	open func(context.Context) (transport.Framer, error)
	wrap func(transport.Framer) stream.Stream

	framer transport.Framer
	stream stream.Stream
	err    error
	closed bool
}

func (s *lazyStream) ensure(ctx context.Context) (stream.Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream != nil || s.err != nil {
		return s.stream, s.err
	}
	if ctx == nil {
		ctx = s.ctx
	}
	if s.closed {
		if err := ctx.Err(); err != nil {
			s.err = err
		} else {
			s.err = net.ErrClosed
		}
		return nil, s.err
	}
	if err := ctx.Err(); err != nil {
		s.err = err
		return nil, err
	}
	framer, err := s.open(ctx)
	if err != nil {
		if framer != nil {
			_ = framer.Close()
		}
		s.err = err
		return nil, err
	}
	if framer == nil {
		s.err = errors.New("argos: transport returned a nil Framer")
		return nil, s.err
	}
	if err := ctx.Err(); err != nil {
		_ = framer.Close()
		s.err = err
		return nil, err
	}
	s.framer = framer
	s.stream = s.wrap(framer)
	return s.stream, nil
}

func (s *lazyStream) Recv(v any) error {
	st, err := s.ensure(s.ctx)
	if err != nil {
		return err
	}
	return st.Recv(v)
}

func (s *lazyStream) Send(v any) error {
	st, err := s.ensure(s.ctx)
	if err != nil {
		return err
	}
	return st.Send(v)
}

func (s *lazyStream) CloseSend() error {
	st, err := s.ensure(s.ctx)
	if err != nil {
		return err
	}
	return st.CloseSend()
}

func (s *lazyStream) Close() error {
	s.mu.Lock()
	s.closed = true
	framer := s.framer
	s.mu.Unlock()
	if framer == nil {
		return nil
	}
	return framer.Close()
}
