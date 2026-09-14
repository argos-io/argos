package server

import (
	"context"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

type binding struct {
	argos.Config
	dispatch filter.Handler
	methods  map[string]stream.CallKind
}

// invoke is the whole server call: the filter chain wrapping dispatch. A filter
// that short-circuits never reaches dispatch, so it never closes the send side.
func (b *binding) invoke(ctx context.Context, method string, f transport.Framer) error {
	cd, err := b.CodecForCall()
	if err != nil {
		return err
	}
	serveOpts := transport.ApplyServerOptions(b.ServerTransportOpts)
	st := stream.WrapWithLimit(f, cd, serveOpts.MaxMessageSize)
	return filter.Chain(b.Filters, b.dispatchEnd(f))(ctx, method, st)
}

func (b *binding) dispatchEnd(f transport.Framer) filter.Handler {
	return func(ctx context.Context, method string, st stream.Stream) error {
		var err error
		if kind, ok := b.methods[method]; ok && kind.IsStreaming() {
			if capability, ok := b.Transport.(transport.Streaming); ok && !capability.SupportsStreaming() {
				err = errs.Error(errs.Unimplemented, "transport does not support streaming calls")
			} else if b.dispatch == nil {
				err = errs.Error(errs.Unimplemented, "method is not registered")
			} else {
				err = b.dispatch(ctx, method, st)
			}
		} else if b.dispatch == nil {
			err = errs.Error(errs.Unimplemented, "method is not registered")
		} else {
			err = b.dispatch(ctx, method, st)
		}
		if closeErr := f.CloseSend(); err == nil && closeErr != nil {
			err = closeErr
		}
		return err
	}
}
