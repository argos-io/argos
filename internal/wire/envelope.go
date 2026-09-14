package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	// FlagEnd marks the end of one direction of a call.
	FlagEnd byte = 1 << 0

	// FlagStatus marks the status trailer the responder writes once the call is
	// over. Its payload is a status, never codec bytes, so a receiver must never
	// hand it to a Codec. It is independent of FlagEnd: a call that ends without
	// ever closing its send side still gets a trailer.
	FlagStatus byte = 1 << 1

	envelopeFixedSize = 1 + 4 + 1 + 4
)

// Envelope is one message carried inside a transport frame.
type Envelope struct {
	Method   string
	StreamID uint32
	Flags    byte
	Metadata []byte
	Payload  []byte
}

// MarshalEnvelope encodes an envelope. Message boundaries are supplied by the
// transport and are not part of this encoding.
func MarshalEnvelope(env Envelope) ([]byte, error) {
	if len(env.Method) > 255 {
		return nil, fmt.Errorf("wire: method is %d bytes; maximum is 255", len(env.Method))
	}
	if !utf8.ValidString(env.Method) {
		return nil, errors.New("wire: method is not valid UTF-8")
	}
	if uint64(len(env.Metadata)) > uint64(^uint32(0)) {
		return nil, errors.New("wire: metadata is too large")
	}

	buf := make([]byte, envelopeFixedSize+len(env.Method)+len(env.Metadata)+len(env.Payload))
	offset := 0
	buf[offset] = byte(len(env.Method))
	offset++
	copy(buf[offset:], env.Method)
	offset += len(env.Method)
	binary.BigEndian.PutUint32(buf[offset:], env.StreamID)
	offset += 4
	buf[offset] = env.Flags
	offset++
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(env.Metadata)))
	offset += 4
	copy(buf[offset:], env.Metadata)
	offset += len(env.Metadata)
	copy(buf[offset:], env.Payload)
	return buf, nil
}

// UnmarshalEnvelope decodes one complete envelope blob.
func UnmarshalEnvelope(data []byte) (Envelope, error) {
	if len(data) < envelopeFixedSize {
		return Envelope{}, errors.New("wire: truncated envelope")
	}

	methodLen := int(data[0])
	offset := 1
	if len(data)-offset < methodLen+4+1+4 {
		return Envelope{}, errors.New("wire: truncated method")
	}
	method := string(data[offset : offset+methodLen])
	if !utf8.ValidString(method) {
		return Envelope{}, errors.New("wire: method is not valid UTF-8")
	}
	offset += methodLen
	streamID := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	flags := data[offset]
	offset++
	metadataLen := uint64(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if metadataLen > uint64(len(data)-offset) {
		return Envelope{}, errors.New("wire: truncated metadata")
	}
	metadataEnd := offset + int(metadataLen)

	return Envelope{
		Method:   method,
		StreamID: streamID,
		Flags:    flags,
		Metadata: append([]byte(nil), data[offset:metadataEnd]...),
		Payload:  append([]byte(nil), data[metadataEnd:]...),
	}, nil
}

// ValidateEnvelope checks untrusted envelope sections against caller limits.
func ValidateEnvelope(env Envelope, maxPayload, maxMetadata int64) error {
	payloadLimit := maxPayload
	if env.Flags == FlagStatus && payloadLimit > 0 && payloadLimit < 4 {
		// Status always carries a four-byte code. It is control metadata, not a
		// user message, so a small application payload limit must not make the
		// transport unable to report completion or failure.
		payloadLimit = 4
	}
	if payloadLimit > 0 && int64(len(env.Payload)) > payloadLimit {
		return fmt.Errorf("wire: payload is %d bytes; maximum is %d", len(env.Payload), payloadLimit)
	}
	if maxMetadata > 0 && int64(len(env.Metadata)) > maxMetadata {
		return fmt.Errorf("wire: metadata is %d bytes; maximum is %d", len(env.Metadata), maxMetadata)
	}
	return nil
}

// MarshalStatus encodes the terminal status payload.
func MarshalStatus(code uint32, description string) []byte {
	payload := make([]byte, 4+len(description))
	binary.BigEndian.PutUint32(payload, code)
	copy(payload[4:], description)
	return payload
}

// UnmarshalStatus decodes a terminal status payload.
func UnmarshalStatus(payload []byte) (uint32, string, error) {
	if len(payload) < 4 {
		return 0, "", errors.New("wire: truncated status")
	}
	return binary.BigEndian.Uint32(payload), string(payload[4:]), nil
}
