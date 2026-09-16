package grpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/argos-io/argos/status"
)

const lpmHeaderSize = 5

// errInvalidLPMFlag is returned when the compressed flag byte is not 0 or 1.
// The client may treat this as a non-gRPC HTTP body and resolve status via
// trailers / HTTP fallback; transport errors must not take that path.
var errInvalidLPMFlag = errors.New("framing/grpc: invalid LPM compressed flag")

// ErrLPMUnsynced reports that an oversize message left the stream misaligned,
// so no further message can be read from it. The receive direction is finished;
// the size rejection it wraps is still the status to report to the caller.
var ErrLPMUnsynced = fmt.Errorf("framing/grpc: LPM stream out of sync")

// maxDrain bounds how much of an oversize message is worth reading to keep the
// stream aligned. One extra message's slack, floored so a small maxLen still
// tolerates a normal overshoot.
func maxDrain(maxLen int64) int64 {
	const floor = 1 << 20
	if maxLen < floor {
		return floor
	}
	return maxLen
}

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
		return false, nil, fmt.Errorf("%w %d", errInvalidLPMFlag, hdr[0])
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if maxLen > 0 && int64(n) > maxLen {
		st := status.Error(status.ResourceExhausted,
			fmt.Sprintf("framing/grpc: LPM length %d > max %d", n, maxLen))
		// Draining keeps the stream aligned so a later message on the same
		// stream still parses, but the length is a 32-bit field the peer
		// chooses: draining whatever it claims lets it hold the stream for up
		// to 4 GiB of reads. Past maxDrain the stream is not worth
		// resynchronising - on http2 each call owns its stream, so the caller
		// tears it down - and ErrLPMUnsynced says so.
		if int64(n) > maxDrain(maxLen) {
			return false, nil, fmt.Errorf("%w: %w", ErrLPMUnsynced, st)
		}
		// A drain that stops short leaves the stream misaligned, so it must not
		// be reported as a plain size rejection. io.Copy reports a truncated
		// source as success, so compare the count rather than the error.
		drained, derr := io.Copy(io.Discard, io.LimitReader(r, int64(n)))
		if derr != nil {
			return false, nil, fmt.Errorf("%w: %w", ErrLPMUnsynced, derr)
		}
		if drained != int64(n) {
			return false, nil, fmt.Errorf("%w: drained %d of %d", ErrLPMUnsynced, drained, n)
		}
		return false, nil, st
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
