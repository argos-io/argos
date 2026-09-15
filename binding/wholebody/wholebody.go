// Package wholebody assembles wholebody Framing with transport/http1.
//
// Docs may import this package as wholebodybinding. Only unary RPCs are
// supported. Codec defaults to JSON (application/json); override with WithCodec.
package wholebody

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/framing/wholebody"
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

// WithCodec selects the Codec for the binding. Default is codec/json.
func WithCodec(c codec.Codec) Option {
	return optionFunc(func(o *options) { o.codec = c })
}

// New returns a BindingFunc that builds wholebody × http1 on every call.
func New(opts ...Option) argos.BindingFunc {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	return func() (argos.Binding, error) {
		cd := o.codec
		if cd == nil {
			cd = jsoncodec.New()
		}
		return argos.Binding{
			Transport: argoshttp1.New(),
			Framing:   wholebody.New(),
			Codec:     cd,
		}, nil
	}
}
