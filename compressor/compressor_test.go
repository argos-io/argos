package compressor_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/compressor/gzip"
)

func TestIdentityRoundTrip(t *testing.T) {
	t.Parallel()
	c := compressor.Identity
	if got := c.Name(); got != "identity" {
		t.Fatalf("Name = %q, want identity", got)
	}

	payload := []byte("hello identity")
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
	if !bytes.Equal(compressed.Bytes(), payload) {
		t.Fatalf("identity must passthrough; got %q", compressed.Bytes())
	}

	rc, err := c.Decompress(&compressed)
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
		t.Fatalf("round-trip = %q, want %q", got, payload)
	}
}

func TestGzipRoundTrip(t *testing.T) {
	t.Parallel()
	c := gzip.New()
	if got := c.Name(); got != "gzip" {
		t.Fatalf("Name = %q, want gzip", got)
	}

	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i * 37)
	}

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
	if bytes.Equal(compressed.Bytes(), payload) {
		t.Fatal("gzip should change wire bytes for this payload")
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
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestFind(t *testing.T) {
	t.Parallel()
	list := []compressor.Compressor{compressor.Identity, gzip.New()}

	got, ok := compressor.Find("gzip", list)
	if !ok {
		t.Fatal(`Find("gzip") = false, want true`)
	}
	if got.Name() != "gzip" {
		t.Fatalf("Find gzip Name = %q", got.Name())
	}

	if _, ok := compressor.Find("snappy", []compressor.Compressor{compressor.Identity}); ok {
		t.Fatal(`Find("snappy", [identity]) = true, want false`)
	}
	if _, ok := compressor.Find("gzip", nil); ok {
		t.Fatal(`Find("gzip", nil) = true, want false`)
	}
}
