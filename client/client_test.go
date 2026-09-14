package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/http1"
)

type captureTransport struct {
	gotAddr    *string
	openFramer transport.Framer
	opened     chan struct{}
	openOnce   sync.Once
}

func (t *captureTransport) ListenAndServe(
	context.Context,
	func(context.Context, string, transport.Framer) error,
	...transport.ServerOption,
) error {
	return nil
}

func (t *captureTransport) Open(_ context.Context, _ string, opts ...transport.ClientOption) (transport.Framer, error) {
	call := transport.ApplyClientOptions(opts)
	if t.opened != nil {
		t.openOnce.Do(func() { close(t.opened) })
	}
	if t.gotAddr != nil {
		*t.gotAddr = call.DialAddress
	}
	if t.openFramer != nil {
		return t.openFramer, nil
	}
	return captureFramer{}, nil
}

type captureFramer struct{}

func (captureFramer) Recv() (io.Reader, error) { return bytes.NewReader(nil), io.EOF }
func (captureFramer) Send() (io.WriteCloser, error) {
	return captureWriter{}, nil
}
func (captureFramer) CloseSend() error { return nil }
func (captureFramer) Close() error     { return nil }

type captureWriter struct{}

func (captureWriter) Write(p []byte) (int, error) { return len(p), nil }
func (captureWriter) Close() error                { return nil }

func TestClientTargetIPScheme(t *testing.T) {
	var gotAddr string
	c := client.New(
		argos.WithTarget("ip://127.0.0.1:9090"),
		argos.WithTransport(&captureTransport{gotAddr: &gotAddr}),
		argos.WithCodec(nopCodec{}),
	)

	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got, want := gotAddr, "127.0.0.1:9090"; got != want {
		t.Fatalf("dial address = %q, want %q", got, want)
	}
}

func TestClientTargetRejectsBareHostPort(t *testing.T) {
	c := client.New(
		argos.WithTarget("127.0.0.1:7000"),
		argos.WithTransport(&captureTransport{}),
		argos.WithCodec(nopCodec{}),
	)
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for target without scheme")
	}
}

func TestClientRejectsIncompatibleTransportCodec(t *testing.T) {
	c := client.New(
		argos.WithTransport(http1.New()),
		argos.WithCodec(protobuf.New()),
	)
	err := c.Open(context.Background(), "svc/Method", func(stream.Stream) error {
		t.Fatal("call must not run for an incompatible binding")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), `transport "http1" is incompatible`) {
		t.Fatalf("error = %v, want http1/codec incompatibility", err)
	}
}

type nopCodec struct{}

func (nopCodec) Marshal(_ io.Writer, _ any) error   { return nil }
func (nopCodec) Unmarshal(_ io.Reader, _ any) error { return nil }

func TestOpenErrorsWithoutTransport(t *testing.T) {
	c := client.New(argos.WithCodec(nopCodec{}))
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error without transport")
	}
}

func TestOpenErrorsWithoutCodec(t *testing.T) {
	c := client.New(argos.WithTransport(&captureTransport{}))
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error without codec")
	}
}

func TestOpenInvalidTargetReturnsError(t *testing.T) {
	c := client.New(
		argos.WithTarget("bad://no-such-scheme"),
		argos.WithTransport(&captureTransport{}),
		argos.WithCodec(nopCodec{}),
	)
	err := c.Open(context.Background(), "svc/Method", func(_ stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for unknown selector scheme")
	}
}

type blockingFramer struct {
	closed      chan struct{}
	closeCalled chan struct{}
	closeOnce   sync.Once
}

type closeTrackingFramer struct {
	closed atomic.Bool
}

func (f *closeTrackingFramer) Recv() (io.Reader, error) { return nil, io.EOF }
func (f *closeTrackingFramer) Send() (io.WriteCloser, error) {
	return captureWriter{}, nil
}
func (f *closeTrackingFramer) CloseSend() error { return nil }
func (f *closeTrackingFramer) Close() error {
	f.closed.Store(true)
	return nil
}

type openErrorTransport struct {
	framer transport.Framer
}

func (openErrorTransport) ListenAndServe(
	context.Context,
	func(context.Context, string, transport.Framer) error,
	...transport.ServerOption,
) error {
	return nil
}

func (t openErrorTransport) Open(context.Context, string, ...transport.ClientOption) (transport.Framer, error) {
	return t.framer, errors.New("open failed")
}

func TestClientOpenClosesFramerReturnedWithError(t *testing.T) {
	framer := &closeTrackingFramer{}
	c := client.New(
		argos.WithTransport(openErrorTransport{framer: framer}),
		argos.WithCodec(nopCodec{}),
	)
	if err := c.Open(context.Background(), "svc/Method", func(stream.Stream) error { return nil }); err == nil {
		t.Fatal("Open succeeded, want error")
	}
	if !framer.closed.Load() {
		t.Fatal("Open did not close framer returned with error")
	}
}

func TestClientOpenRejectsNilCall(t *testing.T) {
	c := client.New(argos.WithTransport(&captureTransport{}), argos.WithCodec(nopCodec{}))
	if err := c.Open(context.Background(), "svc/Method", nil); err == nil {
		t.Fatal("Open accepted nil call")
	}
}

func TestClientOpenRejectsNilContext(t *testing.T) {
	c := client.New(argos.WithTransport(&captureTransport{}), argos.WithCodec(nopCodec{}))
	var ctx context.Context
	if err := c.Open(ctx, "svc/Method", func(stream.Stream) error { return nil }); err == nil {
		t.Fatal("Open accepted nil context")
	}
}

func TestClientOpenDoesNotMutateCallerMetadata(t *testing.T) {
	original := metadata.Metadata{"token": {"original"}}
	ctx := metadata.With(context.Background(), original)
	tr := &captureTransport{openFramer: captureFramer{}}
	c := client.New(argos.WithTransport(tr), argos.WithCodec(nopCodec{}))
	if err := c.Open(ctx, "svc/Method", func(stream.Stream) error {
		metadata.FromContext(ctx)["token"] = []string{"call"}
		return nil
	}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := original["token"]; len(got) != 1 || got[0] != "original" {
		t.Fatalf("caller metadata = %v, want original value", original)
	}
}

func newBlockingFramer() *blockingFramer {
	return &blockingFramer{
		closed:      make(chan struct{}),
		closeCalled: make(chan struct{}),
	}
}

func (f *blockingFramer) Recv() (io.Reader, error) {
	<-f.closed
	return nil, net.ErrClosed
}

func (f *blockingFramer) Send() (io.WriteCloser, error) {
	return captureWriter{}, nil
}

func (f *blockingFramer) CloseSend() error { return nil }

func (f *blockingFramer) Close() error {
	f.closeOnce.Do(func() {
		close(f.closed)
		close(f.closeCalled)
	})
	return nil
}

func TestClientCancellationClosesBlockedFramer(t *testing.T) {
	framer := newBlockingFramer()
	tr := &captureTransport{opened: make(chan struct{})}
	tr.openFramer = framer
	c := client.New(
		argos.WithTransport(tr),
		argos.WithCodec(nopCodec{}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- c.Open(ctx, "svc/Method", func(st stream.Stream) error {
			return st.Recv(new(struct{}))
		})
	}()
	select {
	case <-tr.opened:
	case <-time.After(time.Second):
		t.Fatal("Transport.Open was not called")
	}

	cancel()
	select {
	case <-framer.closeCalled:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close framer")
	}
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Open error = %v, want net.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Open did not return after framer close")
	}
}

type callStreamFramer struct {
	recv      chan struct{}
	sent      chan struct{}
	closeSend chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newCallStreamFramer() *callStreamFramer {
	return &callStreamFramer{
		recv:      make(chan struct{}, 1),
		sent:      make(chan struct{}, 1),
		closeSend: make(chan struct{}, 1),
		closed:    make(chan struct{}),
	}
}

func (f *callStreamFramer) Recv() (io.Reader, error) {
	select {
	case <-f.recv:
		return bytes.NewReader(nil), nil
	case <-f.closed:
		return nil, net.ErrClosed
	}
}

func (f *callStreamFramer) Send() (io.WriteCloser, error) {
	return &callStreamWriter{framer: f}, nil
}

func (f *callStreamFramer) CloseSend() error {
	select {
	case f.closeSend <- struct{}{}:
	default:
	}
	return nil
}

func (f *callStreamFramer) Close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

type callStreamWriter struct {
	framer *callStreamFramer
	bytes.Buffer
	once sync.Once
}

func (w *callStreamWriter) Close() error {
	w.once.Do(func() {
		select {
		case w.framer.sent <- struct{}{}:
		case <-w.framer.closed:
		}
	})
	return nil
}

func TestOpenStreamSupportsSendRecvCloseSendAndClose(t *testing.T) {
	framer := newCallStreamFramer()
	tr := &captureTransport{openFramer: framer}
	c := client.New(argos.WithTransport(tr), argos.WithCodec(nopCodec{}))
	call := c.OpenStream(context.Background(), "svc/Chat", stream.CallBidiStreaming)

	if err := call.Send(struct{}{}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case <-framer.sent:
	case <-time.After(time.Second):
		t.Fatal("Send did not reach the framer")
	}
	framer.recv <- struct{}{}
	if err := call.Recv(new(struct{})); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if err := call.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	select {
	case <-framer.closeSend:
	case <-time.After(time.Second):
		t.Fatal("CloseSend did not reach the framer")
	}
	if err := call.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-framer.closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not release the framer")
	}
}

type closeErrorFramer struct {
	err error
}

func (f *closeErrorFramer) Recv() (io.Reader, error) { return bytes.NewReader(nil), io.EOF }
func (f *closeErrorFramer) Send() (io.WriteCloser, error) {
	return captureWriter{}, nil
}
func (*closeErrorFramer) CloseSend() error { return nil }
func (f *closeErrorFramer) Close() error   { return f.err }

func TestOpenStreamCloseReturnsFramerError(t *testing.T) {
	want := errors.New("close failed")
	c := client.New(
		argos.WithTransport(&captureTransport{openFramer: &closeErrorFramer{err: want}}),
		argos.WithCodec(nopCodec{}),
	)
	call := c.OpenStream(context.Background(), "svc/Chat", stream.CallBidiStreaming)
	if err := call.Send(struct{}{}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := call.Close(); !errors.Is(err, want) {
		t.Fatalf("Close error = %v, want %v", err, want)
	}
}

func TestOpenStreamFilterShortCircuitDoesNotOpenTransport(t *testing.T) {
	var opens atomic.Int32
	tr := &captureTransport{}
	tr.openFramer = &callStreamFramer{}
	tr.gotAddr = nil
	filterErr := errors.New("stream denied")
	c := client.New(
		argos.WithTransport(openCountingTransport{base: tr, opens: &opens}),
		argos.WithCodec(nopCodec{}),
		argos.WithFilter(func(context.Context, string, stream.Stream, filter.Handler) error {
			return filterErr
		}),
	)

	call := c.OpenStream(context.Background(), "svc/Chat", stream.CallClientStreaming)
	if err := call.Send(struct{}{}); !errors.Is(err, filterErr) {
		t.Fatalf("Send error = %v, want %v", err, filterErr)
	}
	if got := opens.Load(); got != 0 {
		t.Fatalf("Transport.Open calls = %d, want 0", got)
	}
}

func TestOpenStreamCloseBeforeWorkerStartsCancelsCall(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	tr := &captureTransport{openFramer: &callStreamFramer{}}
	c := client.New(
		argos.WithTransport(tr),
		argos.WithCodec(nopCodec{}),
		argos.WithFilter(func(ctx context.Context, _ string, _ stream.Stream, _ filter.Handler) error {
			close(started)
			<-ctx.Done()
			close(finished)
			return ctx.Err()
		}),
	)

	call := c.OpenStream(context.Background(), "svc/Chat", stream.CallClientStreaming)
	if err := call.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stream worker did not start")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel a worker that started after Close")
	}
	if err := call.Send(struct{}{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Send after early Close = %v, want context.Canceled", err)
	}
}

func TestClientCancellationBeforeFilterNextDoesNotOpenTransport(t *testing.T) {
	var opens atomic.Int32
	entered := make(chan struct{})
	tr := &captureTransport{}
	c := client.New(
		argos.WithTransport(openCountingTransport{base: tr, opens: &opens}),
		argos.WithCodec(nopCodec{}),
		argos.WithFilter(func(ctx context.Context, method string, st stream.Stream, next filter.Handler) error {
			close(entered)
			<-ctx.Done()
			return next(ctx, method, st)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- c.Open(ctx, "svc/Method", func(stream.Stream) error { return nil })
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("filter did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Open error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Open did not return after cancellation")
	}
	if got := opens.Load(); got != 0 {
		t.Fatalf("Transport.Open calls = %d, want 0", got)
	}
}

type openCountingTransport struct {
	base  *captureTransport
	opens *atomic.Int32
}

func (t openCountingTransport) ListenAndServe(
	context.Context,
	func(context.Context, string, transport.Framer) error,
	...transport.ServerOption,
) error {
	return nil
}

func (t openCountingTransport) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	t.opens.Add(1)
	return t.base.Open(ctx, method, opts...)
}
