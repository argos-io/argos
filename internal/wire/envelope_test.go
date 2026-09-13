package wire

import (
	"bytes"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	want := Envelope{
		Method:   "echo.v1.EchoService/Echo",
		StreamID: 42,
		Flags:    FlagEnd,
		Metadata: []byte{0, 1, 2, 3},
		Payload:  []byte("hello"),
	}

	blob, err := MarshalEnvelope(want)
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	got, err := UnmarshalEnvelope(blob)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}

	if got.Method != want.Method || got.StreamID != want.StreamID || got.Flags != want.Flags {
		t.Fatalf("round trip header = %#v, want %#v", got, want)
	}
	if !bytes.Equal(got.Metadata, want.Metadata) {
		t.Fatalf("metadata = %v, want %v", got.Metadata, want.Metadata)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("payload = %q, want %q", got.Payload, want.Payload)
	}
}

func TestStatusRoundTrip(t *testing.T) {
	payload := MarshalStatus(5, "broken")
	code, description, err := UnmarshalStatus(payload)
	if err != nil {
		t.Fatalf("UnmarshalStatus: %v", err)
	}
	if code != 5 || description != "broken" {
		t.Fatalf("status = (%d, %q), want (5, %q)", code, description, "broken")
	}
}
