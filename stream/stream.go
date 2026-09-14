// Package stream is a decoded call stream. Unary is a stream of exactly one element.
package stream

import (
	"bytes"
	"errors"
	"io"
	"reflect"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/internal/limits"
	"github.com/argos-io/argos/transport"
)

// CallKind describes the message directions of one RPC.
type CallKind uint8

const (
	// CallUnknown keeps the generic Client.Open API compatible with callers
	// that do not declare an RPC shape.
	CallUnknown CallKind = iota
	CallUnary
	CallServerStreaming
	CallClientStreaming
	CallBidiStreaming
)

// IsStreaming reports whether the call has a streaming direction.
func (k CallKind) IsStreaming() bool {
	return k == CallServerStreaming || k == CallClientStreaming || k == CallBidiStreaming
}

// Stream is a decoded call stream.
type Stream interface {
	Recv(v any) error
	Send(v any) error
	CloseSend() error
}

type framedStream struct {
	framer         transport.Framer
	codec          codec.Codec
	maxMessageSize int64
}

func (s *framedStream) Recv(v any) error {
	if isNil(s.framer) {
		return errors.New("stream: nil framer")
	}
	if isNil(s.codec) {
		return errors.New("stream: nil codec")
	}
	r, err := s.framer.Recv()
	if err != nil {
		return err
	}
	if r == nil {
		return errors.New("stream: framer returned nil reader")
	}
	limited := limits.NewReader(r, s.maxMessageSize)
	if err := s.codec.Unmarshal(limited, v); err != nil {
		return err
	}
	// A custom codec may successfully decode without consuming the entire
	// message. Drain the remainder so the strict reader still probes for bytes
	// beyond the configured limit instead of allowing the codec to bypass it.
	if _, err := io.Copy(io.Discard, limited); err != nil {
		return err
	}
	return nil
}

func (s *framedStream) Send(v any) error {
	if isNil(s.framer) {
		return errors.New("stream: nil framer")
	}
	if isNil(s.codec) {
		return errors.New("stream: nil codec")
	}
	// Marshal before acquiring the transport writer. A number of transports
	// commit headers or a frame when the writer is closed, so creating a writer
	// before codec.Marshal fails could otherwise send an empty/partial message.
	var payload bytes.Buffer
	limited := limits.NewWriter(&payload, s.maxMessageSize)
	if err := s.codec.Marshal(limited, v); err != nil {
		return err
	}
	if limited.Exceeded() {
		return limits.ErrTooLarge
	}
	w, err := s.framer.Send()
	if err != nil {
		return err
	}
	if w == nil {
		return errors.New("stream: framer returned nil writer")
	}
	data := payload.Bytes()
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil {
			_ = w.Close()
			return err
		}
		if n <= 0 || n > len(data) {
			_ = w.Close()
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return w.Close()
}

func (s *framedStream) CloseSend() error {
	if isNil(s.framer) {
		return errors.New("stream: nil framer")
	}
	return s.framer.CloseSend()
}

// Close terminates the underlying framed call. It is intentionally an extra
// method rather than part of Stream so existing filters and custom streams do
// not need a breaking interface change.
func (s *framedStream) Close() error {
	if isNil(s.framer) {
		return errors.New("stream: nil framer")
	}
	return s.framer.Close()
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

// Wrap turns a Framer and Codec into a Stream.
func Wrap(f transport.Framer, c codec.Codec) Stream {
	return WrapWithLimit(f, c, limits.DefaultMessageSize)
}

// WrapWithLimit turns a Framer and Codec into a Stream with a maximum decoded
// message size. It keeps the limit at the message layer, where it applies to
// both built-in and user-provided codecs.
func WrapWithLimit(f transport.Framer, c codec.Codec, maxMessageSize int64) Stream {
	return &framedStream{
		framer:         f,
		codec:          c,
		maxMessageSize: maxMessageSize,
	}
}
