package grpc_test

import (
	"bytes"
	"io"
	"testing"

	grpcframing "github.com/argos-io/argos/framing/grpc"
)

func TestLPMRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"empty", []byte{}},
		{"nil_as_empty", nil},
		{"hello", []byte("hello")},
		{"binary", []byte{0, 1, 2, 255}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := grpcframing.WriteLPM(&buf, false, tc.payload); err != nil {
				t.Fatalf("WriteLPM: %v", err)
			}
			compressed, got, err := grpcframing.ReadLPM(&buf)
			if err != nil {
				t.Fatalf("ReadLPM: %v", err)
			}
			if compressed {
				t.Fatal("compressed flag set")
			}
			want := tc.payload
			if want == nil {
				want = []byte{}
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("payload = %v, want %v", got, want)
			}
			// Empty message must not be treated as EOF: a second read on empty
			// buffer is EOF only because the stream ended, not because of LPM.
			_, _, err = grpcframing.ReadLPM(&buf)
			if err != io.EOF {
				t.Fatalf("second ReadLPM err = %v, want EOF (stream ended)", err)
			}
		})
	}
}

func TestLPMEmptyNotEOF(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := grpcframing.WriteLPM(&buf, false, []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := grpcframing.WriteLPM(&buf, false, []byte("next")); err != nil {
		t.Fatal(err)
	}
	_, p1, err := grpcframing.ReadLPM(&buf)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if len(p1) != 0 {
		t.Fatalf("first payload len = %d, want 0", len(p1))
	}
	_, p2, err := grpcframing.ReadLPM(&buf)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if string(p2) != "next" {
		t.Fatalf("second = %q, want next", p2)
	}
}

func TestLPMRejectsCompressed(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := grpcframing.WriteLPM(&buf, true, []byte("x")); err != nil {
		t.Fatal(err)
	}
	compressed, payload, err := grpcframing.ReadLPM(&buf)
	if err != nil {
		t.Fatalf("ReadLPM: %v", err)
	}
	if !compressed {
		t.Fatal("want compressed=true")
	}
	if string(payload) != "x" {
		t.Fatalf("payload = %q", payload)
	}
}
