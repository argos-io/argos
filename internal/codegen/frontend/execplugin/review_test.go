package execplugin

import (
	"strings"
	"testing"
)

// Plugin output is a subprocess we do not control. Capturing it without a
// ceiling let a buggy or hostile plugin exhaust memory in CI.
func TestCappedBufferStopsGrowingAtLimit(t *testing.T) {
	c := &cappedBuffer{limit: 16, what: "stdout"}

	n, err := c.Write([]byte("0123456789"))
	if err != nil || n != 10 {
		t.Fatalf("Write = (%d, %v), want (10, nil)", n, err)
	}
	if c.overflow != nil {
		t.Fatalf("overflow reported below the limit: %v", c.overflow)
	}

	// Crossing the limit keeps the write "successful" so the subprocess is not
	// blocked on a broken pipe, but records the overflow.
	big := make([]byte, 1<<20)
	n, err = c.Write(big)
	if err != nil || n != len(big) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(big))
	}
	if c.overflow == nil {
		t.Fatal("no overflow recorded after crossing the limit")
	}
	if !strings.Contains(c.overflow.Error(), "stdout") {
		t.Fatalf("overflow = %v, want it to name the stream", c.overflow)
	}
	if got := c.buf.Len(); got > c.limit {
		t.Fatalf("buffered %d bytes for a limit of %d", got, c.limit)
	}

	// Further writes stay cheap and keep the first overflow.
	first := c.overflow
	if _, err := c.Write(big); err != nil {
		t.Fatalf("Write after overflow: %v", err)
	}
	if c.overflow != first {
		t.Fatal("overflow error replaced by a later write")
	}
	if got := c.buf.Len(); got > c.limit {
		t.Fatalf("buffered %d bytes after overflow", got)
	}
}
