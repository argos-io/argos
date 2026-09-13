// Package codec converts messages to and from bytes. It does not know transport or metadata.
package codec

import "io"

// Codec converts messages to and from bytes.
type Codec interface {
	Marshal(w io.Writer, v any) error
	Unmarshal(r io.Reader, v any) error
}
