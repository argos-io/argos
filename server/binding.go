package server

import (
	"context"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/option"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

type binding struct {
	option.Config
	dispatch filter.Handler
}

// invoke is the whole server call: the filter chain wrapping dispatch. A filter
// that short-circuits never reaches dispatch, so it never closes the send side.
func (b *binding) invoke(ctx context.Context, method string, f transport.Framer) error {
	cd, err := b.CodecForCall()
	if err != nil {
		return err
	}
	st := stream.Wrap(f, cd)
	return filter.Chain(b.Filters, b.dispatchEnd(f))(ctx, method, st)
}

func (b *binding) dispatchEnd(f transport.Framer) filter.Handler {
	return func(ctx context.Context, method string, st stream.Stream) error {
		var err error
		if b.dispatch == nil {
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
