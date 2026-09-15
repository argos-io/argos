package grpc_test

import (
	"testing"

	grpcframing "github.com/argos-io/argos/framing/grpc"
)

func TestEncodeDecodeGrpcMessage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		raw, encoded string
	}{
		{"", ""},
		{"ok", "ok"},
		{"hello world", "hello world"},
		{"100%", "100%25"},
		{"a\tb", "a%09b"},
		{"café", "caf%C3%A9"},
		{"space and % percent", "space and %25 percent"},
	} {
		got := grpcframing.EncodeGrpcMessage(tc.raw)
		if got != tc.encoded {
			t.Errorf("Encode(%q) = %q, want %q", tc.raw, got, tc.encoded)
		}
		decoded := grpcframing.DecodeGrpcMessage(tc.encoded)
		if decoded != tc.raw {
			t.Errorf("Decode(%q) = %q, want %q", tc.encoded, decoded, tc.raw)
		}
	}
	// Invalid UTF-8 is replaced then percent-encoded (grpc-go compatible).
	got := grpcframing.EncodeGrpcMessage("\x00\xff")
	want := "%00%EF%BF%BD"
	if got != want {
		t.Errorf("Encode(invalid utf8) = %q, want %q", got, want)
	}
}

func TestDecodeGrpcMessageToleratesBadPercent(t *testing.T) {
	t.Parallel()
	// Incomplete / invalid hex kept as-is (must not lose companion grpc-status).
	for _, in := range []string{"%zz", "%A", "ok%", "%%"} {
		got := grpcframing.DecodeGrpcMessage(in)
		if got != in {
			t.Errorf("Decode(%q) = %q, want unchanged", in, got)
		}
	}
	if got := grpcframing.DecodeGrpcMessage("%41"); got != "A" {
		t.Errorf("Decode(%%41) = %q, want A", got)
	}
}
