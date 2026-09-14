// Package metadata carries key/value pairs outside the message body on context.
package metadata

import "context"

type metadataKey struct{}

// Metadata is key/value pairs carried outside the message body.
type Metadata map[string][]string

// Clone returns an independent copy of md, including its value slices.
func Clone(md Metadata) Metadata {
	if md == nil {
		return nil
	}
	out := make(Metadata, len(md))
	for key, values := range md {
		out[key] = append([]string(nil), values...)
	}
	return out
}

// FromContext returns the writable Metadata on ctx, or nil if none was laid down.
func FromContext(ctx context.Context) Metadata {
	md, _ := ctx.Value(metadataKey{}).(Metadata)
	return md
}

// With merges md into the Metadata already on ctx. It does not replace the
// metadata visible through the returned context, and it does not mutate the
// parent context or any caller-owned value slices.
func With(ctx context.Context, md Metadata) context.Context {
	existing := Clone(FromContext(ctx))
	if existing == nil {
		existing = make(Metadata)
	}
	for k, v := range md {
		existing[k] = append([]string(nil), v...)
	}
	return context.WithValue(ctx, metadataKey{}, existing)
}
