// Package wholebody assembles wholebody Framing with transport/http1.
//
// Docs may import this package as wholebodybinding. Only unary RPCs are
// supported. Codec defaults to JSON; override with WithCodec.
package wholebody

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/wholebody"
	"github.com/argos-io/argos/transport"
	argoshttp1 "github.com/argos-io/argos/transport/http1"
)

// Option configures New.
type Option interface {
	apply(*options)
}

type options struct {
	codec codec.Codec
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithCodec selects the Codec. Default is codec/json.
func WithCodec(c codec.Codec) Option {
	return optionFunc(func(o *options) { o.codec = c })
}

// New returns wholebody × http1.
func New(opts ...Option) argos.Protocol {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	return argos.Protocol{
		Transport: func() (transport.Transport, error) {
			return argoshttp1.New(), nil
		},
		Framing: func() (framing.Framing, error) {
			return wholebody.New(), nil
		},
		Codec: func() (codec.Codec, error) {
			cd := o.codec
			if cd == nil {
				cd = jsoncodec.New()
			}
			return cd, nil
		},
	}
}

// Service returns a ServiceOption that installs New(opts...) on a service entry.
func Service(opts ...Option) argos.ServiceOption {
	return argos.ServiceProtocol(New(opts...))
}
