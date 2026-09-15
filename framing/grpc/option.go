package grpc

import (
	"fmt"

	"github.com/argos-io/argos/compressor"
)

// Option configures New.
type Option func(*options)

type options struct {
	extra []compressor.Compressor
	send  string
}

// WithCompressors adds compressors to the Framing's algorithm list.
// Identity is always present by default; gzip and custom algorithms must be
// passed here to enable them. Duplicate names (including a second identity)
// are rejected by New.
func WithCompressors(cs ...compressor.Compressor) Option {
	return func(o *options) {
		o.extra = append(o.extra, cs...)
	}
}

// WithSendCompressor selects the outbound message encoding. The name must be
// present in the configured compressor list (identity is always available).
// An empty name means identity.
func WithSendCompressor(name string) Option {
	return func(o *options) {
		o.send = name
	}
}

func applyOptions(opts []Option) (*Framing, error) {
	o := &options{send: compressor.Identity.Name()}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	list := []compressor.Compressor{compressor.Identity}
	seen := map[string]struct{}{compressor.Identity.Name(): {}}
	for _, c := range o.extra {
		if c == nil {
			return nil, fmt.Errorf("framing/grpc: nil compressor")
		}
		name := c.Name()
		if name == "" {
			return nil, fmt.Errorf("framing/grpc: compressor with empty name")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("framing/grpc: duplicate compressor %q", name)
		}
		seen[name] = struct{}{}
		list = append(list, c)
	}

	sendName := o.send
	if sendName == "" {
		sendName = compressor.Identity.Name()
	}
	if _, ok := compressor.Find(sendName, list); !ok {
		return nil, fmt.Errorf("framing/grpc: send compressor %q not configured", sendName)
	}

	return &Framing{
		compressors: list,
		sendName:    sendName,
	}, nil
}
