package fake

import (
	"io"
	"sync/atomic"
)

// ReentryDetector wraps an io.Reader and counts concurrent Read reentry.
// The race detector will not catch two legally concurrent Reads; this does.
type ReentryDetector struct {
	R       io.Reader
	reading atomic.Int32
	hits    atomic.Int64
}

// Read implements io.Reader.
func (d *ReentryDetector) Read(p []byte) (int, error) {
	if d.reading.Add(1) != 1 {
		d.hits.Add(1)
	}
	defer d.reading.Add(-1)
	return d.R.Read(p)
}

// ConcurrentHits returns how many times a Read overlapped another Read.
func (d *ReentryDetector) ConcurrentHits() int64 { return d.hits.Load() }
