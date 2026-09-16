package envelope

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Header is one name/value pair in an OPEN, HEADERS, or STATUS metadata blob.
type Header struct {
	Name  string
	Value string
}

// appendMetadata encodes headers as:
//
//	u16 count || repeated (u16len name || u16len value)
func appendMetadata(dst []byte, hdrs []Header) ([]byte, error) {
	if len(hdrs) > 0xffff {
		return nil, fmt.Errorf("envelope: metadata count %d exceeds uint16", len(hdrs))
	}
	dst = appendU16(dst, uint16(len(hdrs)))
	for _, h := range hdrs {
		var err error
		dst, err = appendString(dst, h.Name)
		if err != nil {
			return nil, err
		}
		dst, err = appendString(dst, h.Value)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

// minHeaderWireSize is the smallest encoded size of one name/value pair: two
// u16 length prefixes with empty payloads.
const minHeaderWireSize = 4

// parseMetadata decodes a metadata blob and returns the headers plus bytes consumed.
// Strings are deep-copied and do not alias body.
func parseMetadata(body []byte) ([]Header, int, error) {
	if len(body) < 2 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	count := int(binary.BigEndian.Uint16(body[0:2]))
	off := 2
	if count == 0 {
		return nil, off, nil
	}
	// Each pair costs at least two length prefixes, so a count the remaining
	// bytes cannot possibly hold is a malformed header. Without this check a
	// well-framed 14-byte OPEN can drive a 65535-entry allocation (~2 MiB) that
	// fails on the first parseString — pure waste, repeatable per connection.
	if count > (len(body)-off)/minHeaderWireSize {
		return nil, 0, fmt.Errorf("%w: metadata count %d exceeds body %d", ErrInvalidLength, count, len(body)-off)
	}
	hdrs := make([]Header, 0, count)
	for i := 0; i < count; i++ {
		name, n, err := parseString(body[off:])
		if err != nil {
			return nil, 0, err
		}
		off += n
		value, n, err := parseString(body[off:])
		if err != nil {
			return nil, 0, err
		}
		off += n
		hdrs = append(hdrs, Header{Name: name, Value: value})
	}
	return hdrs, off, nil
}

// metadataWireSize returns the encoded size of hdrs, or an error if a field
// cannot be encoded under the u16 length limits.
func metadataWireSize(hdrs []Header) (int64, error) {
	if len(hdrs) > 0xffff {
		return 0, fmt.Errorf("envelope: metadata count %d exceeds uint16", len(hdrs))
	}
	var n int64 = 2
	for _, h := range hdrs {
		if len(h.Name) > 0xffff {
			return 0, fmt.Errorf("envelope: metadata name length %d exceeds uint16", len(h.Name))
		}
		if len(h.Value) > 0xffff {
			return 0, fmt.Errorf("envelope: metadata value length %d exceeds uint16", len(h.Value))
		}
		n += 2 + int64(len(h.Name)) + 2 + int64(len(h.Value))
	}
	return n, nil
}
