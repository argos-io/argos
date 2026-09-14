package limits

import "io"

// Writer bounds bytes forwarded to an underlying writer.
type Writer struct {
	w         io.Writer
	remaining int64
	tooLarge  bool
}

// NewWriter returns a writer that rejects data beyond max bytes. Non-positive
// limits use the package default.
func NewWriter(w io.Writer, max int64) *Writer {
	if max <= 0 {
		max = DefaultMessageSize
	}
	return &Writer{w: w, remaining: max}
}

// Write forwards a prefix that fits and reports ErrTooLarge for the rest.
func (w *Writer) Write(p []byte) (int, error) {
	if w.remaining == 0 && len(p) != 0 {
		w.tooLarge = true
		return 0, ErrTooLarge
	}
	if int64(len(p)) > w.remaining {
		w.tooLarge = true
		p = p[:w.remaining]
		n, err := w.w.Write(p)
		if n < 0 || n > len(p) {
			return n, io.ErrShortWrite
		}
		if n > 0 {
			w.remaining -= int64(n)
		}
		if err != nil {
			return n, err
		}
		return n, ErrTooLarge
	}
	n, err := w.w.Write(p)
	if n < 0 || n > len(p) {
		return n, io.ErrShortWrite
	}
	if n > 0 {
		w.remaining -= int64(n)
	}
	if err == nil && n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, err
}

// Exceeded reports whether a write attempted to exceed the configured limit.
func (w *Writer) Exceeded() bool { return w.tooLarge }
