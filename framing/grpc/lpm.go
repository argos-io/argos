package grpc

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/argos-io/argos/status"
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

// ReadLPM reads one Length-Prefixed Message with no wire-size ceiling.
// Prefer ReadLPMLimited when MaxFrameSize / MaxMessageSize are known.
func ReadLPM(r io.Reader) (compressed bool, payload []byte, err error) {
	return ReadLPMLimited(r, 0)
}

// ReadLPMLimited reads one Length-Prefixed Message. If maxLen > 0 and the
// advertised payload length exceeds it, the call returns ResourceExhausted
// without allocating the payload buffer. io.EOF means the stream ended before
// a header. A message with length 0 returns an empty non-nil payload.
func ReadLPMLimited(r io.Reader, maxLen int64) (compressed bool, payload []byte, err error) {
	var hdr [lpmHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return false, nil, err
	}
	compressed = hdr[0] != 0
	if hdr[0] > 1 {
		return false, nil, fmt.Errorf("framing/grpc: invalid LPM compressed flag %d", hdr[0])
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if maxLen > 0 && int64(n) > maxLen {
		// Drain the claimed payload without allocating it so the peer Send can finish.
		_, _ = io.Copy(io.Discard, io.LimitReader(r, int64(n)))
		return false, nil, status.Error(status.ResourceExhausted,
			fmt.Sprintf("framing/grpc: LPM length %d > max %d", n, maxLen))
	}
	payload = make([]byte, n)
	if n == 0 {
		return compressed, payload, nil
	}
	if _, err := io.ReadFull(r, payload); err != nil {
		return false, nil, err
	}
	return compressed, payload, nil
}
