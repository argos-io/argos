package echov1

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec/protobuf"
)

type memoryWriteCloser struct {
	bytes.Buffer
	once sync.Once
	send func([]byte)
}

func (w *memoryWriteCloser) Close() error {
	w.once.Do(func() {
		w.send(bytes.Clone(w.Buffer.Bytes()))
	})
	return nil
}

type memoryFramer struct {
	recv      <-chan []byte
	send      chan<- []byte
	closeOnce sync.Once
}

func (f *memoryFramer) Recv() (io.Reader, error) {
	msg, ok := <-f.recv
	if !ok {
		return nil, io.EOF
	}
	return bytes.NewReader(msg), nil
}

func (f *memoryFramer) Send() (io.WriteCloser, error) {
	return &memoryWriteCloser{send: func(msg []byte) { f.send <- msg }}, nil
}

func (f *memoryFramer) CloseSend() error {
	f.closeOnce.Do(func() { close(f.send) })
	return nil
}

type memoryCall struct {
	ctx    context.Context
	method string
	framer argos.Framer
}

type memoryTransport struct {
	ready chan struct{}
	calls chan memoryCall
	once  sync.Once
}

func newMemoryTransport() *memoryTransport {
	return &memoryTransport{
		ready: make(chan struct{}),
		calls: make(chan memoryCall),
	}
}

func (t *memoryTransport) ListenAndServe(
	ctx context.Context,
	onCall func(context.Context, string, argos.Framer) error,
	_ ...argos.TransportServerOption,
) error {
	t.once.Do(func() { close(t.ready) })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case call := <-t.calls:
			go func() { _ = onCall(call.ctx, call.method, call.framer) }()
		}
	}
}

func (t *memoryTransport) Open(
	ctx context.Context,
	method string,
	_ ...argos.TransportClientOption,
) (argos.Framer, error) {
	requests := make(chan []byte)
	responses := make(chan []byte)
	server := &memoryFramer{recv: requests, send: responses}
	client := &memoryFramer{recv: responses, send: requests}
	select {
	case t.calls <- memoryCall{ctx: ctx, method: method, framer: server}:
		return client, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestEchoRoundTrip(t *testing.T) {
	transport := newMemoryTransport()
	server := argos.NewServer()
	service := server.NewService(
		argos.WithTransport(transport),
		argos.WithCodec(protobuf.New()),
	)
	RegisterEchoService(service, &echoImpl{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	<-transport.ready
	t.Cleanup(func() {
		cancel()
		<-done
	})

	client := NewEchoServiceClient(
		argos.WithTransport(transport),
		argos.WithCodec(protobuf.New()),
	)
	response, err := client.Echo(
		context.Background(),
		&EchoRequest{Msg: "world"},
	)
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "hello world"; got != want {
		t.Fatalf("response msg = %q, want %q", got, want)
	}
}
