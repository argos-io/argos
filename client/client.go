// Package client opens calls through one Transport and Codec.
package client

import (
	"context"

	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/selector"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"

	_ "github.com/argos-io/argos/selector/ip"
)

// Client opens calls through one Transport and Codec.
type Client struct {
	cfg argos.Config
}

// New creates a Client configured by opts.
func New(opts ...argos.Option) *Client {
	return &Client{cfg: argos.NewConfig(opts...)}
}

// Open opens method and runs call through the configured filters.
func (c *Client) Open(
	ctx context.Context,
	method string,
	call func(stream.Stream) error,
) error {
	cd, err := c.cfg.ResolveCodec()
	if err != nil {
		return err
	}

	tr, opts, err := c.resolveTransport(ctx)
	if err != nil {
		return err
	}

	if metadata.FromContext(ctx) == nil {
		ctx = metadata.With(ctx, metadata.Metadata{})
	}
	f, err := tr.Open(ctx, method, opts...)
	if err != nil {
		return err
	}
	st := stream.Wrap(f, cd)
	end := func(_ context.Context, _ string, filtered stream.Stream) error {
		return call(filtered)
	}
	return filter.Chain(c.cfg.Filters, end)(ctx, method, st)
}

func (c *Client) resolveTransport(ctx context.Context) (transport.Transport, []transport.ClientOption, error) {
	opts := append([]transport.ClientOption(nil), c.cfg.ClientTransportOpts...)
	if c.cfg.ClientTarget != "" {
		addr, err := selector.Parse(ctx, c.cfg.ClientTarget)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, transport.WithDialAddress(addr))
	}
	tr, err := c.cfg.ResolveTransport()
	if err != nil {
		return nil, nil, err
	}
	return tr, opts, nil
}
