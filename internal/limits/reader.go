// Package limits provides strict readers for untrusted message bodies.
package limits

import (
	"errors"
	"io"
)

const (
	// DefaultMessageSize is the default decoded message limit for codecs and
	// transports. Applications can raise it explicitly with transport options.
	DefaultMessageSize int64 = 4 << 20
)

// ErrTooLarge reports that a reader produced more than its configured limit.
var ErrTooLarge = errors.New("message exceeds configured limit")

// ErrInvalidRead reports an underlying Reader that violated io.Reader's
// contract by returning a count outside the supplied buffer.
var ErrInvalidRead = errors.New("reader returned an invalid byte count")

// Reader reads at most max bytes and probes for one additional byte after the
// limit so an exact-size message is accepted while an oversized one is not
// silently truncated.
type Reader struct {
	r         io.Reader
	remaining int64
}

// NewReader returns a strict size-limited reader. Non-positive limits use the
// package default.
func NewReader(r io.Reader, max int64) *Reader {
	if max <= 0 {
		max = DefaultMessageSize
	}
	return &Reader{r: r, remaining: max}
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n < 0 || n > len(one) {
			return 0, ErrInvalidRead
		}
		if n > 0 {
			return 0, ErrTooLarge
		}
		if err != nil {
			return 0, err
		}
		return 0, io.ErrNoProgress
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	if n < 0 || n > len(p) {
		return 0, ErrInvalidRead
	}
	if n > 0 {
		r.remaining -= int64(n)
	}
	if n == 0 && err == nil {
		return 0, io.ErrNoProgress
	}
	return n, err
}

// ReadAll reads r and rejects data beyond max.
func ReadAll(r io.Reader, max int64) ([]byte, error) {
	return io.ReadAll(NewReader(r, max))
}
