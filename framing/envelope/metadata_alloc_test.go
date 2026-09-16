package envelope_test

import (
	"errors"
	"runtime"
	"testing"

	"github.com/argos-io/argos/framing/envelope"
)

// A metadata count is wire data. Using it as an allocation size before checking
// it against the remaining bytes let a ~15-byte well-framed OPEN drive a
// 65535-entry (≈2 MiB) allocation that then failed on the first length prefix.
func TestParseFrameBodyBoundsMetadataAllocation(t *testing.T) {
	t.Parallel()
	body := make([]byte, 9+2)
	body[0] = byte(envelope.TypeHeaders)
	body[9], body[10] = 0xFF, 0xFF // count = 65535, zero pairs follow

	// Warm up so the measurement is not polluted by first-call allocations.
	for i := 0; i < 10; i++ {
		_, _ = envelope.ParseFrameBody(body)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const rounds = 200
	for i := 0; i < rounds; i++ {
		if _, err := envelope.ParseFrameBody(body); err == nil {
			t.Fatal("malformed metadata count accepted")
		}
	}
	runtime.ReadMemStats(&after)

	perParse := int64(after.TotalAlloc-before.TotalAlloc) / rounds
	if perParse > 4096 {
		t.Fatalf("each %d-byte frame allocated %d bytes; the count must be validated before allocating",
			len(body), perParse)
	}

	_, err := envelope.ParseFrameBody(body)
	if !errors.Is(err, envelope.ErrInvalidLength) {
		t.Fatalf("err = %v, want ErrInvalidLength", err)
	}
}
