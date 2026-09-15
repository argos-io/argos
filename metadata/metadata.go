// Package metadata holds call-scoped headers and trailers.
//
// Metadata values returned by getters are snapshots only. CallMetadata is the
// single authoritative, concurrent-safe handle for one call; place it on the
// call context with ContextWith / FromContext.
package metadata

import (
	"context"
	"errors"

	"github.com/argos-io/argos/status"
)

// Metadata is a snapshot of key/value pairs. Getters always return independent
// copies; callers must never assume the map or its slices alias internal state.
type Metadata map[string][]string

// Clone returns an independent deep copy of md, including value slices.
// Clone(nil) returns nil.
func Clone(md Metadata) Metadata {
	if md == nil {
		return nil
	}
	out := make(Metadata, len(md))
	for k, vs := range md {
		if vs == nil {
			out[k] = nil
			continue
		}
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// CallMetadata is the call-scoped authority for directional headers/trailers
// and for forwarding an explicit initial-headers submit (SendHeaders).
type CallMetadata interface {
	IncomingHeaders() Metadata
	IncomingTrailers() Metadata
	OutgoingHeaders() Metadata
	OutgoingTrailers() Metadata
	AddOutgoingHeader(key string, values ...string) error
	AddOutgoingTrailer(key string, values ...string) error
	// SendHeaders submits the current outgoing headers snapshot.
	// Only RoleResponder may succeed; unsupported carriers return
	// status.Unimplemented without freezing outgoing headers.
	SendHeaders() error
}

type ctxKey struct{}

// FromContext returns the CallMetadata placed by ContextWith, if any.
func FromContext(ctx context.Context) (CallMetadata, bool) {
	md, ok := ctx.Value(ctxKey{}).(CallMetadata)
	return md, ok
}

// ContextWith attaches md as the sole CallMetadata handle on ctx.
func ContextWith(ctx context.Context, md CallMetadata) context.Context {
	return context.WithValue(ctx, ctxKey{}, md)
}

// Role distinguishes initiator (client) from responder (server) privileges.
type Role uint8

const (
	// RoleInitiator is the client side: may not AddOutgoingTrailer;
	// SendHeaders always returns Unimplemented.
	RoleInitiator Role = iota
	// RoleResponder is the server side: may SendHeaders and set trailers
	// until they are frozen.
	RoleResponder
)

// Stable local usage / lifecycle errors (errors.Is).
var (
	// ErrHeadersAlreadySent is returned by a second successful-path SendHeaders.
	ErrHeadersAlreadySent = errors.New("metadata: headers already sent")

	// ErrOutgoingHeadersFrozen is returned by AddOutgoingHeader after outgoing
	// headers have been frozen (successful SendHeaders or FreezeOutgoingHeaders).
	ErrOutgoingHeadersFrozen = errors.New("metadata: outgoing headers frozen")

	// ErrOutgoingTrailersFrozen is returned by AddOutgoingTrailer after trailers
	// have been frozen (e.g. FreezeOutgoingTrailers on Finish).
	ErrOutgoingTrailersFrozen = errors.New("metadata: outgoing trailers frozen")

	errNotConcrete = errors.New("metadata: CallMetadata was not created by metadata.New")
)

// Stable Unimplemented errors for role / carrier limits.
var (
	errSendHeadersUnsupported = status.Error(status.Unimplemented, "metadata: SendHeaders unsupported")
	errTrailersInitiator      = status.Error(status.Unimplemented, "metadata: AddOutgoingTrailer unsupported for initiator")
)
