package grpc

import (
	"encoding/binary"
	"fmt"
	"io"
)

const lpmHeaderSize = 5

// WriteLPM writes one gRPC Length-Prefixed Message: 1-byte compressed flag,
// 4-byte big-endian length, then payload. A zero-length payload is a valid
// empty message (not end-of-stream).
func WriteLPM(w io.Writer, compressed bool, payload []byte) error {
	if payload == nil {
		payload = []byte{}
	}
	var hdr [lpmHeaderSize]byte
	if compressed {
		hdr[0] = 1
	}
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// ReadLPM reads one Length-Prefixed Message. io.EOF means the stream ended
// before a header (no more messages). A message with length 0 returns an
// empty non-nil payload and a nil error — it is not end-of-stream.
func ReadLPM(r io.Reader) (compressed bool, payload []byte, err error) {
	var hdr [lpmHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return false, nil, err
	}
	compressed = hdr[0] != 0
	if hdr[0] > 1 {
		return false, nil, fmt.Errorf("framing/grpc: invalid LPM compressed flag %d", hdr[0])
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	payload = make([]byte, n)
	if n == 0 {
		return compressed, payload, nil
	}
	if _, err := io.ReadFull(r, payload); err != nil {
		return false, nil, err
	}
	return compressed, payload, nil
}
