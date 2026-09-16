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
	"github.com/argos-io/argos/framing"
	envframing "github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

// Option configures the envelope presets.
type Option interface {
	apply(*options)
}

type options struct {
	codec codec.Codec

	maxReadBytes int64
	badRead      int64
}

// WithMaxReadBytes sets the WebSocket per-message read limit.
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

// WithCodec selects the Codec. Default is codec/protobuf.
func WithCodec(c codec.Codec) Option {
	return optionFunc(func(o *options) { o.codec = c })
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

func codecFactory(o options) argos.CodecFunc {
	return func() (codec.Codec, error) {
		if o.badRead < 0 {
			return nil, fmt.Errorf(
				"binding/envelope: WithMaxReadBytes(%d) must not be negative; the ws transport reads a negative limit as unlimited",
				o.badRead)
		}
		cd := o.codec
		if cd == nil {
			cd = protobuf.New()
		}
		return cd, nil
	}
}

// NewTCP is envelope × tcp (Sequential).
func NewTCP(opts ...Option) argos.Protocol {
	o := applyOpts(opts)
	return argos.Protocol{
		Transport: func() (transport.Transport, error) { return tcp.New(), nil },
		Framing: func() (framing.Framing, error) {
			return envframing.New(), nil
		},
		Codec: codecFactory(o),
	}
}

// NewWS is envelope × ws (Sequential).
func NewWS(opts ...Option) argos.Protocol {
	o := applyOpts(opts)
	read := o.maxReadBytes
	if read == 0 {
		read = ws.DefaultMaxReadBytes
	}
	return argos.Protocol{
		Transport: func() (transport.Transport, error) {
			if o.badRead < 0 {
				return nil, fmt.Errorf(
					"binding/envelope: WithMaxReadBytes(%d) must not be negative; the ws transport reads a negative limit as unlimited",
					o.badRead)
			}
			return ws.New(ws.WithMaxReadBytes(o.maxReadBytes)), nil
		},
		Framing: func() (framing.Framing, error) {
			return envframing.New(envframing.WithMaxInboundWireBytes(read)), nil
		},
		Codec: codecFactory(o),
	}
}

// NewUDP is envelope × udp (OneCallPerConn).
func NewUDP(opts ...Option) argos.Protocol {
	o := applyOpts(opts)
	return argos.Protocol{
		Transport: func() (transport.Transport, error) { return udp.New(), nil },
		Framing: func() (framing.Framing, error) {
			return envframing.New(envframing.WithOneCallPerConn()), nil
		},
		Codec: codecFactory(o),
	}
}

// ServiceTCP returns a ServiceOption for NewTCP(opts...).
func ServiceTCP(opts ...Option) argos.ServiceOption {
	return argos.ServiceProtocol(NewTCP(opts...))
}
