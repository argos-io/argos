// Package metadata carries key/value pairs outside the message body on context.
package metadata

import "context"

type metadataKey struct{}

// Metadata is key/value pairs carried outside the message body.
type Metadata map[string][]string

// FromContext returns the writable Metadata on ctx, or nil if none was laid down.
func FromContext(ctx context.Context) Metadata {
	md, _ := ctx.Value(metadataKey{}).(Metadata)
	return md
}

// With merges md into the Metadata already on ctx. It does not replace the map.
func With(ctx context.Context, md Metadata) context.Context {
	existing := FromContext(ctx)
	if existing == nil {
		existing = make(Metadata)
		ctx = context.WithValue(ctx, metadataKey{}, existing)
	}
	for k, v := range md {
		existing[k] = v
	}
	return ctx
}
