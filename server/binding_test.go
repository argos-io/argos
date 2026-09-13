package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/option"
	"github.com/argos-io/argos/stream"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

func (jsonCodec) Unmarshal(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}

type testFramer struct {
	recv            []byte
	sent            bytes.Buffer
	closeSendCount  atomic.Int32
	closeSendErr    error
	dispatchDone    *atomic.Bool
	closedAfterCall atomic.Bool
}

func (f *testFramer) Recv() (io.Reader, error) {
	return bytes.NewReader(f.recv), nil
}

func (f *testFramer) Send() (io.WriteCloser, error) {
	return nopWriteCloser{&f.sent}, nil
}

func (f *testFramer) CloseSend() error {
	f.closeSendCount.Add(1)
	if f.dispatchDone != nil && f.dispatchDone.Load() {
		f.closedAfterCall.Store(true)
	}
	return f.closeSendErr
}

type nopWriteCloser struct {
	*bytes.Buffer
}

func (nopWriteCloser) Close() error { return nil }

func TestBindingInvokeUnaryAndClosesSendAfterDispatch(t *testing.T) {
	svc := &Service{binding: binding{
		Config: option.NewConfig(option.WithCodec(jsonCodec{})),
	}}
	var dispatchDone atomic.Bool
	svc.Register(func(_ context.Context, method string, st stream.Stream) error {
		defer dispatchDone.Store(true)
		if method != "/echo.Echo/Say" {
			t.Fatalf("method = %q", method)
		}
		var request string
		if err := st.Recv(&request); err != nil {
			return err
		}
		if request != "ping" {
			t.Fatalf("request = %q", request)
		}
		return st.Send("pong")
	})

	input, _ := json.Marshal("ping")
	framer := &testFramer{recv: input, dispatchDone: &dispatchDone}
	if err := svc.binding.invoke(context.Background(), "/echo.Echo/Say", framer); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if framer.closeSendCount.Load() < 1 {
		t.Fatal("CloseSend was not called")
	}
	if !framer.closedAfterCall.Load() {
		t.Fatal("CloseSend ran before dispatch returned")
	}
	var response string
	if err := json.Unmarshal(framer.sent.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response != "pong" {
		t.Fatalf("response = %q", response)
	}
}

func TestBindingFilterShortCircuitSkipsDispatchAndCloseSend(t *testing.T) {
	svc := &Service{binding: binding{
		Config: option.NewConfig(
			option.WithCodec(jsonCodec{}),
			option.WithFilter(func(context.Context, string, stream.Stream, filter.Handler) error {
				return errs.Error(errs.Unauthenticated, "missing token")
			}),
		),
	}}
	dispatchCalled := false
	svc.Register(func(context.Context, string, stream.Stream) error {
		dispatchCalled = true
		return nil
	})

	framer := &testFramer{}
	err := svc.binding.invoke(context.Background(), "/echo.Echo/Say", framer)
	if errs.CodeOf(err) != errs.Unauthenticated {
		t.Fatalf("error = %v", err)
	}
	if dispatchCalled {
		t.Fatal("dispatch must not run after a filter short-circuits")
	}
	if framer.closeSendCount.Load() != 0 {
		t.Fatal("filter short-circuit must skip CloseSend")
	}
}

func TestBindingUnregisteredMethodReturnsUnimplemented(t *testing.T) {
	svc := &Service{binding: binding{
		Config: option.NewConfig(option.WithCodec(jsonCodec{})),
	}}

	framer := &testFramer{}
	err := svc.binding.invoke(context.Background(), "/missing.Service/Method", framer)
	if errs.CodeOf(err) != errs.Unimplemented {
		t.Fatalf("error = %v", err)
	}
	if framer.closeSendCount.Load() < 1 {
		t.Fatal("CloseSend was not called")
	}
}

func TestBindingCloseSendErrorAfterSuccessfulDispatch(t *testing.T) {
	closeErr := errors.New("close send failed")
	svc := &Service{binding: binding{
		Config: option.NewConfig(option.WithCodec(jsonCodec{})),
	}}
	svc.Register(func(context.Context, string, stream.Stream) error {
		return nil
	})

	framer := &testFramer{closeSendErr: closeErr}
	if err := svc.binding.invoke(context.Background(), "/echo.Echo/Say", framer); !errors.Is(err, closeErr) {
		t.Fatalf("error = %v, want %v", err, closeErr)
	}
}

func TestBindingDispatchErrorWinsOverCloseSendError(t *testing.T) {
	dispatchErr := errs.Error(errs.Internal, "dispatch failed")
	closeErr := errors.New("close send failed")
	svc := &Service{binding: binding{
		Config: option.NewConfig(option.WithCodec(jsonCodec{})),
	}}
	svc.Register(func(context.Context, string, stream.Stream) error {
		return dispatchErr
	})

	framer := &testFramer{closeSendErr: closeErr}
	err := svc.binding.invoke(context.Background(), "/echo.Echo/Say", framer)
	if !errors.Is(err, dispatchErr) {
		t.Fatalf("error = %v, want dispatch error", err)
	}
	if errors.Is(err, closeErr) {
		t.Fatal("CloseSend error must not replace dispatch error")
	}
}
