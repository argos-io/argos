// Package codec converts messages to and from bytes. It does not know transport or metadata.
package codec

// Codec converts messages to and from bytes. It is a pure function over one
// whole message: the message layer encodes to a byte slice and hands that slice
// to a transport, so a Codec never performs I/O and never sees a Framer.
type Codec interface {
	// Marshal encodes v. The returned slice is owned by the caller.
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes b into v. b is borrowed only until return: the
	// implementation MUST NOT retain aliases of b or its sub-slices after
	// return. Copy any bytes that must outlive the call (§2.1 — contract
	// reversed from v1). framing.Call.Recv payloads are released after
	// Unmarshal returns and may be reused.
	Unmarshal(b []byte, v any) error
}

// Named is an optional codec identity used for transport compatibility
// validation. Custom codecs may implement it when their wire representation
// has a known compatibility class.
type Named interface {
	CodecName() string
}
