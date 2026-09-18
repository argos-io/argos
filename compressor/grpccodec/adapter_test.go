package grpccodec

import (
	"bytes"
	"io"
	"testing"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/compressor/gzip"
	"google.golang.org/grpc/encoding"
)

func TestFromRoundTrip(t *testing.T) {
	t.Parallel()
	gc := From(gzip.New())
	if gc == nil {
		t.Fatal("From returned nil")
	}
	if got := gc.Name(); got != "gzip" {
		t.Fatalf("Name = %q, want gzip", got)
	}
	roundTripEncoding(t, gc, randomPayload(2048))
}

func TestToRoundTrip(t *testing.T) {
	t.Parallel()
	// Wrap our compressor through From then To to exercise the reverse path
	// without depending on grpc-go's process-level gzip registry.
	gc := From(gzip.New())
	c := To(gc)
	if c == nil {
		t.Fatal("To returned nil")
	}
	if got := c.Name(); got != "gzip" {
		t.Fatalf("Name = %q, want gzip", got)
	}
	roundTripArgos(t, c, randomPayload(2048))
}

func TestToWrapsNonCloser(t *testing.T) {
	t.Parallel()
	c := To(readerOnlyCompressor{})
	rc, err := c.Decompress(bytes.NewReader([]byte("plain")))
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if string(got) != "plain" {
		t.Fatalf("got %q", got)
	}
}

func TestFromToIdentity(t *testing.T) {
	t.Parallel()
	c := To(From(compressor.Identity))
	roundTripArgos(t, c, []byte("passthrough"))
}

func roundTripArgos(t *testing.T, c compressor.Compressor, payload []byte) {
	t.Helper()
	var compressed bytes.Buffer
	wc, err := c.Compress(&compressed)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if _, err := wc.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	rc, err := c.Decompress(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close reader: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d want %d", len(got), len(payload))
	}
}

func roundTripEncoding(t *testing.T, c encoding.Compressor, payload []byte) {
	t.Helper()
	var compressed bytes.Buffer
	wc, err := c.Compress(&compressed)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if _, err := wc.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	r, err := c.Decompress(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if rc, ok := r.(io.Closer); ok {
		if err := rc.Close(); err != nil {
			t.Fatalf("Close reader: %v", err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d want %d", len(got), len(payload))
	}
}

func randomPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*91 + 17)
	}
	return b
}

// readerOnlyCompressor is an encoding.Compressor whose Decompress returns a
// plain io.Reader (not ReadCloser), to exercise To's NopCloser wrap.
type readerOnlyCompressor struct{}

func (readerOnlyCompressor) Name() string { return "reader-only" }

func (readerOnlyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return nopWriteCloser{Writer: w}, nil
}

func (readerOnlyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	return r, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
