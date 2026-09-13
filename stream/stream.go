// Package stream is a decoded call stream. Unary is a stream of exactly one element.
package stream

import (
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/transport"
)

// Stream is a decoded call stream.
type Stream interface {
	Recv(v any) error
	Send(v any) error
	CloseSend() error
}

type framedStream struct {
	framer transport.Framer
	codec  codec.Codec
}

func (s *framedStream) Recv(v any) error {
	r, err := s.framer.Recv()
	if err != nil {
		return err
	}
	return s.codec.Unmarshal(r, v)
}

func (s *framedStream) Send(v any) error {
	w, err := s.framer.Send()
	if err != nil {
		return err
	}
	if err := s.codec.Marshal(w, v); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (s *framedStream) CloseSend() error {
	return s.framer.CloseSend()
}

// Wrap turns a Framer and Codec into a Stream.
func Wrap(f transport.Framer, c codec.Codec) Stream {
	return &framedStream{framer: f, codec: c}
}
