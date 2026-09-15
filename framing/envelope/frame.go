// Package envelope implements the argos envelope protocol: wire codec
// (OPEN / HEADERS / DATA / END / STATUS) and Sequential Call/Session state
// machines over ByteStreamCarrier and MessageCarrier.
//
// Reuse is always Sequential: one in-flight call per connection. The wire
// format carries a call ID on every frame and therefore allows a peer to
// open concurrent calls on one connection, but this implementation does not —
// AcceptCall is serial (the next OPEN is read only after Call.Close). A
// third-party client that writes two OPENs concurrently will see the second
// call wait, not fail. That gap is intentional (§4.5).
package envelope

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Type is the one-byte frame type on the wire.
type Type byte

const (
	TypeOpen    Type = 1
	TypeHeaders Type = 2
	TypeData    Type = 3
	TypeEnd     Type = 4
	TypeStatus  Type = 5
)

// FlagOpenEnd marks an OPEN frame that also ends the request (zero messages).
// It is the only OPEN flag defined by the wire format.
const FlagOpenEnd byte = 1 << 0

// Frame is one envelope control or data frame.
//
// Field use by Type:
//
//	OPEN:    Method, Flags, Headers
//	HEADERS: Headers
//	DATA:    Data (nil and empty slice both encode as zero-length payload)
//	END:     (no payload fields)
//	STATUS:  Code, Message, Headers (trailers)
type Frame struct {
	Type   Type
	CallID uint64

	Method  string
	Flags   byte
	Headers []Header
	Data    []byte
	Code    uint32
	Message string
}

// Sentinel and format errors.
var (
	// ErrTruncated reports that the input ended before a complete field.
	ErrTruncated = errors.New("envelope: truncated frame")
	// ErrInvalidLength reports a length field that cannot be satisfied by the buffer.
	ErrInvalidLength = errors.New("envelope: invalid length")
	// ErrUnknownType reports an unrecognized frame type byte.
	ErrUnknownType = errors.New("envelope: unknown frame type")
	// ErrTrailingBytes reports leftover bytes after a well-formed payload.
	ErrTrailingBytes = errors.New("envelope: trailing bytes after payload")
	// ErrEndPayload reports a non-empty END payload.
	ErrEndPayload = errors.New("envelope: END payload must be empty")
	// ErrFrameTooLarge reports that the frame exceeds the configured max.
	ErrFrameTooLarge = errors.New("envelope: frame exceeds max size")
	// ErrMetaTooLarge reports that metadata exceeds the configured max.
	ErrMetaTooLarge = errors.New("envelope: metadata exceeds max size")
)

const (
	headerSize = 1 + 8 // typ + callID
	lenPrefix  = 4
)

// MarshalFrame encodes f as a length-prefixed frame for ByteStreamCarrier:
//
//	uint32 BE length_of_body || body
func MarshalFrame(f Frame) ([]byte, error) {
	body, err := MarshalFrameBody(f)
	if err != nil {
		return nil, err
	}
	out := make([]byte, lenPrefix+len(body))
	binary.BigEndian.PutUint32(out, uint32(len(body)))
	copy(out[lenPrefix:], body)
	return out, nil
}

// MarshalFrameBody encodes f without a length prefix (MessageCarrier / UDP batch).
// Layout: typ(1) || callID(uint64 BE) || payload.
func MarshalFrameBody(f Frame) ([]byte, error) {
	payload, err := marshalPayload(f)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, headerSize+len(payload))
	out = append(out, byte(f.Type))
	out = appendU64(out, f.CallID)
	out = append(out, payload...)
	return out, nil
}

// AppendFrame appends a frame body (no length prefix) to dst for UDP batching.
func AppendFrame(dst []byte, f Frame) ([]byte, error) {
	body, err := MarshalFrameBody(f)
	if err != nil {
		return dst, err
	}
	return append(dst, body...), nil
}

// UnmarshalPrefixed reads one length-prefixed frame from r.
func UnmarshalPrefixed(r io.Reader) (Frame, error) {
	var hdr [lenPrefix]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, fmt.Errorf("%w: length prefix", ErrTruncated)
		}
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n < headerSize {
		return Frame{}, fmt.Errorf("%w: body length %d", ErrInvalidLength, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, fmt.Errorf("%w: frame body", ErrTruncated)
		}
		return Frame{}, err
	}
	return ParseFrameBody(body)
}

// ParseFrameBody parses one frame body (no length prefix).
// Returned strings and slices do not alias body.
func ParseFrameBody(body []byte) (Frame, error) {
	if len(body) < headerSize {
		return Frame{}, fmt.Errorf("%w: need %d bytes, got %d", ErrTruncated, headerSize, len(body))
	}
	f := Frame{
		Type:   Type(body[0]),
		CallID: binary.BigEndian.Uint64(body[1:9]),
	}
	payload := body[headerSize:]
	switch f.Type {
	case TypeOpen:
		return parseOpen(f, payload)
	case TypeHeaders:
		return parseHeaders(f, payload)
	case TypeData:
		// Always allocate so empty DATA is a non-nil empty slice, distinct
		// from "unset" and from OPEN|END (which never sets Data).
		f.Data = make([]byte, len(payload))
		copy(f.Data, payload)
		return f, nil
	case TypeEnd:
		if len(payload) != 0 {
			return Frame{}, ErrEndPayload
		}
		return f, nil
	case TypeStatus:
		return parseStatus(f, payload)
	default:
		return Frame{}, fmt.Errorf("%w: %d", ErrUnknownType, f.Type)
	}
}

// Validate checks frame body and metadata sizes against limits.
// A non-positive maxFrame or maxMeta disables that check.
func (f Frame) Validate(maxFrame, maxMeta int64) error {
	body, err := MarshalFrameBody(f)
	if err != nil {
		return err
	}
	if maxFrame > 0 && int64(len(body)) > maxFrame {
		return fmt.Errorf("%w: body %d > max %d", ErrFrameTooLarge, len(body), maxFrame)
	}
	if maxMeta > 0 {
		switch f.Type {
		case TypeOpen, TypeHeaders, TypeStatus:
			sz, err := metadataWireSize(f.Headers)
			if err != nil {
				return err
			}
			if sz > maxMeta {
				return fmt.Errorf("%w: metadata %d > max %d", ErrMetaTooLarge, sz, maxMeta)
			}
		}
	}
	return nil
}

func marshalPayload(f Frame) ([]byte, error) {
	switch f.Type {
	case TypeOpen:
		out := []byte{f.Flags}
		var err error
		out, err = appendString(out, f.Method)
		if err != nil {
			return nil, err
		}
		return appendMetadata(out, f.Headers)
	case TypeHeaders:
		return appendMetadata(nil, f.Headers)
	case TypeData:
		return append([]byte(nil), f.Data...), nil
	case TypeEnd:
		if len(f.Data) != 0 {
			return nil, ErrEndPayload
		}
		return nil, nil
	case TypeStatus:
		out := appendU32(nil, f.Code)
		var err error
		out, err = appendString(out, f.Message)
		if err != nil {
			return nil, err
		}
		return appendMetadata(out, f.Headers)
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownType, f.Type)
	}
}

func parseOpen(f Frame, payload []byte) (Frame, error) {
	if len(payload) < 1+2 {
		return Frame{}, fmt.Errorf("%w: OPEN flags/method", ErrTruncated)
	}
	f.Flags = payload[0]
	method, n, err := parseString(payload[1:])
	if err != nil {
		return Frame{}, err
	}
	f.Method = method
	off := 1 + n
	hdrs, consumed, err := parseMetadata(payload[off:])
	if err != nil {
		return Frame{}, err
	}
	off += consumed
	if off != len(payload) {
		return Frame{}, ErrTrailingBytes
	}
	f.Headers = hdrs
	return f, nil
}

func parseHeaders(f Frame, payload []byte) (Frame, error) {
	hdrs, consumed, err := parseMetadata(payload)
	if err != nil {
		return Frame{}, err
	}
	if consumed != len(payload) {
		return Frame{}, ErrTrailingBytes
	}
	f.Headers = hdrs
	return f, nil
}

func parseStatus(f Frame, payload []byte) (Frame, error) {
	if len(payload) < 4+2 {
		return Frame{}, fmt.Errorf("%w: STATUS code/message", ErrTruncated)
	}
	f.Code = binary.BigEndian.Uint32(payload[0:4])
	msg, n, err := parseString(payload[4:])
	if err != nil {
		return Frame{}, err
	}
	f.Message = msg
	off := 4 + n
	hdrs, consumed, err := parseMetadata(payload[off:])
	if err != nil {
		return Frame{}, err
	}
	off += consumed
	if off != len(payload) {
		return Frame{}, ErrTrailingBytes
	}
	f.Headers = hdrs
	return f, nil
}

func appendU16(dst []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(dst, b[:]...)
}

func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendU64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

func appendString(dst []byte, s string) ([]byte, error) {
	if len(s) > 0xffff {
		return nil, fmt.Errorf("%w: string length %d exceeds uint16", ErrInvalidLength, len(s))
	}
	dst = appendU16(dst, uint16(len(s)))
	return append(dst, s...), nil
}

func parseString(b []byte) (string, int, error) {
	if len(b) < 2 {
		return "", 0, fmt.Errorf("%w: string length", ErrTruncated)
	}
	n := int(binary.BigEndian.Uint16(b[0:2]))
	if len(b) < 2+n {
		return "", 0, fmt.Errorf("%w: string body want %d have %d", ErrTruncated, n, len(b)-2)
	}
	// Deep copy: string([]byte) already copies in current Go, but be explicit.
	s := string(append([]byte(nil), b[2:2+n]...))
	return s, 2 + n, nil
}
