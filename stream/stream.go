// Package stream is a decoded call stream. Unary is a stream of exactly one element.
package stream

import (
	"errors"
	"io"
	"reflect"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/transport"
)

// Stream is a decoded call stream.
type Stream interface {
	Recv(v any) error
	Send(v any) error
	// HalfClose ends the send direction, matching Call.HalfClose one layer
	// down: the peer sees an end-of-stream, and Recv keeps working.
	HalfClose() error
}

// Closer is implemented by a stream that owns the underlying call and can
// release it. It is deliberately not part of Stream: a filter receives a
// Stream and must not release the call out from under the caller that opened
// it. Callers that own the call type-assert to it.
type Closer interface {
	Close() error
}

// ErrSendClosed means this call's send direction is finished: the peer will
// not accept further request messages, but the receive direction may still
// deliver a complete response. It is not the call's terminal state. Only
// Send and HalfClose return it — never Recv. Wrap produces it when Call.Send
// or Call.HalfClose returns a transport.SendError with ReceiveOpen() true.
//
// errors.Is(ErrSendClosed, io.EOF) is true for grpc-go habit compatibility.
// Normal receive end remains bare io.EOF; the two never share a call site.
var ErrSendClosed error = sendClosedError{}

type sendClosedError struct{}

func (sendClosedError) Error() string { return "stream: send closed" }

func (sendClosedError) Is(target error) bool { return target == io.EOF }

type wrappedStream struct {
	call  transport.Call
	codec codec.Codec
}

func (s *wrappedStream) Recv(v any) error {
	if isNil(s.call) {
		return errors.New("stream: nil call")
	}
	if isNil(s.codec) {
		return errors.New("stream: nil codec")
	}
	payload, release, err := s.call.Recv()
	if err != nil {
		// Recv never maps to ErrSendClosed; pass axis errors through.
		return err
	}
	defer release()
	return s.codec.Unmarshal(payload, v)
}

func (s *wrappedStream) Send(v any) error {
	if isNil(s.call) {
		return errors.New("stream: nil call")
	}
	if isNil(s.codec) {
		return errors.New("stream: nil codec")
	}
	b, err := s.codec.Marshal(v)
	if err != nil {
		return err
	}
	return mapSendErr(s.call.Send(b))
}

func (s *wrappedStream) HalfClose() error {
	if isNil(s.call) {
		return errors.New("stream: nil call")
	}
	return mapSendErr(s.call.HalfClose())
}

// Close terminates the underlying call. It satisfies Closer.
func (s *wrappedStream) Close() error {
	if isNil(s.call) {
		return errors.New("stream: nil call")
	}
	return s.call.Close()
}

func mapSendErr(err error) error {
	if err == nil {
		return nil
	}
	var se transport.SendError
	if errors.As(err, &se) && se.ReceiveOpen() {
		return ErrSendClosed
	}
	return err
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Wrap turns a Call and Codec into a Stream.
func Wrap(c transport.Call, cd codec.Codec) Stream {
	return &wrappedStream{call: c, codec: cd}
}
