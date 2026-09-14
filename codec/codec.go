// Package codec converts messages to and from bytes. It does not know transport or metadata.
package codec

import "io"

// Codec converts messages to and from bytes.
type Codec interface {
	Marshal(w io.Writer, v any) error
	Unmarshal(r io.Reader, v any) error
}

// Named is an optional codec identity used for transport compatibility
// validation. Custom codecs may implement it when their wire representation
// has a known compatibility class.
type Named interface {
	CodecName() string
}
