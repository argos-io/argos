package resp

import (
	"fmt"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

func bindingFactories(opts ...Option) (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	captured := append([]Option(nil), opts...)
	return argos.TransportFunc(func() (transport.Transport, error) {
			tr := tcp.New()
			if tr == nil {
				return nil, fmt.Errorf("resp: nil Transport")
			}
			return tr, nil
		}),
		argos.FramingFunc(func() (framing.Framing, error) {
			fr := New(captured...)
			if fr == nil {
				return nil, fmt.Errorf("resp: nil Framing")
			}
			return fr, nil
		}),
		argos.CodecFunc(func() (codec.Codec, error) { return NewBytesCodec(), nil })
}

// ServiceBinding installs resp × tcp on a service entry.
func ServiceBinding(opts ...Option) argos.ServiceOption {
	t, f, c := bindingFactories(opts...)
	return argos.JoinService(
		argos.ServiceTransport(t),
		argos.ServiceFraming(f),
		argos.ServiceCodec(c),
	)
}

// BindingAxes returns the three factories (tests that wrap transport).
func BindingAxes(opts ...Option) (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
	return bindingFactories(opts...)
}

// ClientBinding is the call-site preset for resp × tcp.
func ClientBinding(opts ...Option) argos.ClientOption {
	t, f, c := bindingFactories(opts...)
	return argos.JoinClient(
		argos.WithTransport(t),
		argos.WithFraming(f),
		argos.WithCodec(c),
	)
}
