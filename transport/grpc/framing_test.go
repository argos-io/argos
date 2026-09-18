package grpc

import (
	"testing"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/compressor/gzip"
	"github.com/argos-io/argos/internal/session"
)

func newTestFraming(t *testing.T, extra []compressor.Compressor, sendName string) session.Framing {
	t.Helper()
	f, err := newFraming(extra, sendName)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestNewFramingRejectsDuplicateCompressor(t *testing.T) {
	_, err := newFraming([]compressor.Compressor{gzip.New(), gzip.New()}, "")
	if err == nil {
		t.Fatal("expected duplicate compressor error")
	}
}

func TestNewFramingRejectsIdentityDuplicate(t *testing.T) {
	_, err := newFraming([]compressor.Compressor{compressor.Identity}, "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewFramingRejectsUnknownSendCompressor(t *testing.T) {
	_, err := newFraming(nil, "gzip")
	if err == nil {
		t.Fatal("expected error when send compressor is not configured")
	}
}
