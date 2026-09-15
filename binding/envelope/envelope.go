// Package envelope assembles envelope Framing with tcp, ws, or udp transports.
//
// Docs may import this package as envelopebinding. Codec defaults to protobuf;
// override with WithCodec. UDP uses OneCallPerConn — callers must set
// MaxFrameSize/MaxMessageSize within the datagram budget on the argos.Config
// (New*Session enforces CheckDatagramLimits).
package envelope

import (
	"fmt"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
	envframing "github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

// Option configures NewTCP / NewWS / NewUDP.
type Option interface {
	apply(*options)
}

type options struct {
	codec codec.Codec
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithCodec selects the Codec for the binding. Default is codec/protobuf.
func WithCodec(c codec.Codec) Option {
	return optionFunc(func(o *options) { o.codec = c })
}

func assemble(tr transport.Transport, frOpts []envframing.Option, o options) (argos.Binding, error) {
	cd := o.codec
	if cd == nil {
		cd = protobuf.New()
	}
	if tr == nil {
		return argos.Binding{}, fmt.Errorf("binding/envelope: nil Transport")
	}
	return argos.Binding{
		Transport: tr,
		Framing:   envframing.New(frOpts...),
		Codec:     cd,
	}, nil
}

func applyOpts(opts []Option) options {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	return o
}

// NewTCP returns a BindingFunc for envelope × tcp (Sequential).
func NewTCP(opts ...Option) argos.BindingFunc {
	o := applyOpts(opts)
	return func() (argos.Binding, error) {
		return assemble(tcp.New(), nil, o)
	}
}

// NewWS returns a BindingFunc for envelope × ws (Sequential).
func NewWS(opts ...Option) argos.BindingFunc {
	o := applyOpts(opts)
	return func() (argos.Binding, error) {
		return assemble(ws.New(), nil, o)
	}
}

// NewUDP returns a BindingFunc for envelope × udp (OneCallPerConn).
// Unary only in practice: one request datagram and one response datagram.
func NewUDP(opts ...Option) argos.BindingFunc {
	o := applyOpts(opts)
	return func() (argos.Binding, error) {
		return assemble(udp.New(), []envframing.Option{envframing.WithOneCallPerConn()}, o)
	}
}
