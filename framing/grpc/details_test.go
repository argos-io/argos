package grpc_test

import (
	"encoding/base64"
	"strings"
	"testing"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/status"
)

func TestEncodeDecodeStatusDetailsBinRoundTrip(t *testing.T) {
	t.Parallel()
	detailVal := []byte{0x08, 0x2a} // arbitrary opaque payload
	err := status.WithDetails(
		status.Error(status.NotFound, "no such echo"),
		status.Detail{
			TypeURL: "type.googleapis.com/echo.v1.ErrorInfo",
			Value:   detailVal,
		},
	)

	wire, ok := grpcframing.EncodeStatusDetailsBin(err)
	if !ok {
		t.Fatal("EncodeStatusDetailsBin: want ok")
	}
	if wire == "" || strings.Contains(wire, "=") {
		t.Fatalf("wire value must be non-empty unpadded base64, got %q", wire)
	}

	got := grpcframing.DecodeStatusDetailsBin(status.NotFound, "ignored trailer message", wire)
	if status.CodeOf(got) != status.NotFound {
		t.Fatalf("CodeOf = %v, want NotFound", status.CodeOf(got))
	}
	// Matching codes: adopt message from details proto, not grpc-message.
	if got.Error() != "no such echo" {
		t.Fatalf("message = %q, want from details proto", got.Error())
	}
	details := status.DetailsOf(got)
	if len(details) != 1 {
		t.Fatalf("len(DetailsOf) = %d, want 1", len(details))
	}
	if details[0].TypeURL != "type.googleapis.com/echo.v1.ErrorInfo" {
		t.Fatalf("TypeURL = %q", details[0].TypeURL)
	}
	if string(details[0].Value) != string(detailVal) {
		t.Fatalf("Value = %q, want %q", details[0].Value, detailVal)
	}
}

func TestEncodeStatusDetailsBinOmitsOKAndEmpty(t *testing.T) {
	t.Parallel()
	if _, ok := grpcframing.EncodeStatusDetailsBin(nil); ok {
		t.Fatal("nil must not emit details-bin")
	}
	if _, ok := grpcframing.EncodeStatusDetailsBin(status.Error(status.OK, "")); ok {
		t.Fatal("OK must not emit details-bin")
	}
	if _, ok := grpcframing.EncodeStatusDetailsBin(status.Error(status.Internal, "boom")); ok {
		t.Fatal("status without details must not emit details-bin")
	}
	// OK with details attached still must not emit (§7.2).
	with := status.WithDetails(status.Error(status.OK, "ok"), status.Detail{TypeURL: "t", Value: []byte("v")})
	if _, ok := grpcframing.EncodeStatusDetailsBin(with); ok {
		t.Fatal("OK+details must not emit details-bin")
	}
}

func TestDecodeStatusDetailsBinCodeConflict(t *testing.T) {
	t.Parallel()
	pb := &spb.Status{
		Code:    int32(status.Canceled),
		Message: "from details",
		Details: []*anypb.Any{{TypeUrl: "type.googleapis.com/x", Value: []byte("v")}},
	}
	raw, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	wire := base64.RawStdEncoding.EncodeToString(raw)

	got := grpcframing.DecodeStatusDetailsBin(status.NotFound, "from trailer", wire)
	if status.CodeOf(got) != status.Internal {
		t.Fatalf("CodeOf = %v, want Internal on code conflict", status.CodeOf(got))
	}
	if !strings.Contains(got.Error(), "grpc-status-details-bin mismatch") {
		t.Fatalf("diagnostic message missing: %q", got.Error())
	}
	if len(status.DetailsOf(got)) != 0 {
		t.Fatal("conflict status must not carry original details")
	}
}

func TestDecodeStatusDetailsBinCorruptedFallback(t *testing.T) {
	t.Parallel()

	t.Run("bad_base64", func(t *testing.T) {
		got := grpcframing.DecodeStatusDetailsBin(status.Unavailable, "down", "!!!not-base64!!!")
		if status.CodeOf(got) != status.Unavailable {
			t.Fatalf("CodeOf = %v, want Unavailable", status.CodeOf(got))
		}
		if got.Error() != "down" {
			t.Fatalf("message = %q, want grpc-message fallback", got.Error())
		}
		if len(status.DetailsOf(got)) != 0 {
			t.Fatal("corrupt details must be discarded")
		}
	})

	t.Run("bad_protobuf", func(t *testing.T) {
		wire := base64.RawStdEncoding.EncodeToString([]byte("not a status proto"))
		got := grpcframing.DecodeStatusDetailsBin(status.Aborted, "stop", wire)
		if status.CodeOf(got) != status.Aborted || got.Error() != "stop" {
			t.Fatalf("got %v %q", status.CodeOf(got), got.Error())
		}
		if len(status.DetailsOf(got)) != 0 {
			t.Fatal("corrupt protobuf must discard details")
		}
	})

	t.Run("missing", func(t *testing.T) {
		got := grpcframing.DecodeStatusDetailsBin(status.Internal, "boom")
		if status.CodeOf(got) != status.Internal || got.Error() != "boom" {
			t.Fatalf("got %v %q", status.CodeOf(got), got.Error())
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		pb := &spb.Status{Code: int32(status.Internal), Message: "d"}
		raw, _ := proto.Marshal(pb)
		wire := base64.RawStdEncoding.EncodeToString(raw)
		got := grpcframing.DecodeStatusDetailsBin(status.Internal, "boom", wire, wire)
		if status.CodeOf(got) != status.Internal || got.Error() != "boom" {
			t.Fatalf("duplicate must fall back to trailer message, got %q", got.Error())
		}
		if len(status.DetailsOf(got)) != 0 {
			t.Fatal("duplicate must discard details")
		}
	})

	t.Run("padded_base64_accepted", func(t *testing.T) {
		pb := &spb.Status{
			Code:    int32(status.PermissionDenied),
			Message: "denied",
			Details: []*anypb.Any{{TypeUrl: "type.googleapis.com/y", Value: []byte("z")}},
		}
		raw, _ := proto.Marshal(pb)
		padded := base64.StdEncoding.EncodeToString(raw)
		got := grpcframing.DecodeStatusDetailsBin(status.PermissionDenied, "ignored", padded)
		if status.CodeOf(got) != status.PermissionDenied || got.Error() != "denied" {
			t.Fatalf("got %v %q", status.CodeOf(got), got.Error())
		}
		if len(status.DetailsOf(got)) != 1 {
			t.Fatal("padded wire must decode details")
		}
	})
}

func TestDecodeStatusDetailsBinDifferentMessageAccepted(t *testing.T) {
	t.Parallel()
	pb := &spb.Status{
		Code:    int32(status.InvalidArgument),
		Message: "from details proto",
	}
	raw, _ := proto.Marshal(pb)
	wire := base64.RawStdEncoding.EncodeToString(raw)
	got := grpcframing.DecodeStatusDetailsBin(status.InvalidArgument, "from grpc-message header", wire)
	if got.Error() != "from details proto" {
		t.Fatalf("message mismatch is not a protocol error; want details message, got %q", got.Error())
	}
}

func TestStatusDetailsBinKey(t *testing.T) {
	t.Parallel()
	if grpcframing.StatusDetailsBinKey != "grpc-status-details-bin" {
		t.Fatalf("StatusDetailsBinKey = %q", grpcframing.StatusDetailsBinKey)
	}
}
