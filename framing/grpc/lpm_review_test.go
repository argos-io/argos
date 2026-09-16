package grpc_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/status"
)

// lpmHeader builds an LPM header advertising n payload bytes.
func lpmHeader(n uint32) []byte {
	h := make([]byte, 5)
	binary.BigEndian.PutUint32(h[1:], n)
	return h
}

// countingReader reports how many bytes were actually read off the wire.
type countingReader struct {
	r    *bytes.Reader
	read int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += n
	return n, err
}

// The length is a 32-bit field the peer chooses. Draining whatever it claims
// let a peer hold the stream for up to 4 GiB of reads for free, so the drain is
// bounded and anything past the bound ends the stream instead.
func TestOversizeLPMDrainIsBounded(t *testing.T) {
	const maxLen = 1024
	// Claim ~1 GiB but send only the header: a bounded implementation must not
	// try to read the claimed length.
	body := append(lpmHeader(1<<30), make([]byte, 4096)...)
	cr := &countingReader{r: bytes.NewReader(body)}

	_, _, err := grpcframing.ReadLPMLimited(cr, maxLen)
	if err == nil {
		t.Fatal("oversize LPM accepted")
	}
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted", status.CodeOf(err))
	}
	if !errors.Is(err, grpcframing.ErrLPMUnsynced) {
		t.Fatalf("err = %v, want it to report the stream is unsynced", err)
	}
	// Only the header should have been consumed.
	if cr.read > 64 {
		t.Fatalf("read %d bytes for a claimed 1 GiB message: the drain is unbounded", cr.read)
	}
}

// An overshoot within the bound is still drained, so a later message on the
// same stream parses.
func TestModestOversizeLPMKeepsStreamAligned(t *testing.T) {
	const maxLen = 1024
	over := make([]byte, 4096)
	var buf bytes.Buffer
	buf.Write(lpmHeader(uint32(len(over))))
	buf.Write(over)
	// A legal message follows.
	want := []byte("second")
	buf.Write(lpmHeader(uint32(len(want))))
	buf.Write(want)

	r := bytes.NewReader(buf.Bytes())
	_, _, err := grpcframing.ReadLPMLimited(r, maxLen)
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("first read code = %v, want ResourceExhausted", status.CodeOf(err))
	}
	if errors.Is(err, grpcframing.ErrLPMUnsynced) {
		t.Fatalf("a %d-byte overshoot should stay resynchronisable: %v", len(over), err)
	}
	_, got, err := grpcframing.ReadLPMLimited(r, maxLen)
	if err != nil {
		t.Fatalf("second read: %v (the drain lost stream alignment)", err)
	}
	if string(got) != string(want) {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

// A drain that fails part-way leaves the stream misaligned; that must not be
// reported as a plain size rejection.
func TestTruncatedOversizeLPMDrainReportsUnsynced(t *testing.T) {
	const maxLen = 1024
	// Claim 4096 (within the drain bound) but deliver only part of it.
	body := append(lpmHeader(4096), make([]byte, 100)...)
	_, _, err := grpcframing.ReadLPMLimited(bytes.NewReader(body), maxLen)
	if err == nil {
		t.Fatal("truncated oversize LPM accepted")
	}
	if !errors.Is(err, grpcframing.ErrLPMUnsynced) {
		t.Fatalf("err = %v, want it to report the stream is unsynced", err)
	}
}
