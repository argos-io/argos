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

	// maxReadBytes caps one WebSocket message read. Overflow closes the whole
	// connection (status 1009), so it must fit one envelope frame. Zero keeps
	// ws.DefaultMaxReadBytes; see ws.WithMaxReadBytes.
	maxReadBytes int64
	// badRead carries a rejected maxReadBytes to the BindingFunc, which is the
	// first place allowed to return an error.
	badRead int64
}

// WithMaxReadBytes sets the WebSocket per-message read limit. It must be large
// enough for one envelope frame, i.e. at least MaxMessageSize plus the frame
// header. Zero keeps ws.DefaultMaxReadBytes.
//
// Negative values are rejected when the BindingFunc runs. The ws transport
// reads a negative limit as "no limit", which would hand a peer control over
// how much one inbound message may allocate.
func WithMaxReadBytes(n int64) Option {
	return optionFunc(func(o *options) {
		if n < 0 {
			o.badRead = n
			return
		}
		o.maxReadBytes = n
	})
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithCodec selects the Codec for the binding. Default is codec/protobuf.
func WithCodec(c codec.Codec) Option {
	return optionFunc(func(o *options) { o.codec = c })
}

func assemble(tr transport.Transport, frOpts []envframing.Option, o options) (argos.Binding, error) {
	if o.badRead < 0 {
		return argos.Binding{}, fmt.Errorf(
			"binding/envelope: WithMaxReadBytes(%d) must not be negative; the ws transport reads a negative limit as unlimited",
			o.badRead)
	}
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
		// Both sides get the same number so CheckConfig can reject a
		// MaxFrameSize that ws would refuse to deliver.
		read := o.maxReadBytes
		if read == 0 {
			read = ws.DefaultMaxReadBytes
		}
		return assemble(
			ws.New(ws.WithMaxReadBytes(o.maxReadBytes)),
			[]envframing.Option{envframing.WithMaxInboundWireBytes(read)},
			o,
		)
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
