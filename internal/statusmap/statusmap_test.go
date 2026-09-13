package statusmap

import (
	"net/http"
	"testing"

	"github.com/argos-io/argos/errs"
)

func TestGRPCStatus(t *testing.T) {
	tests := []struct {
		code errs.Code
		want uint32
	}{
		{errs.NotFound, 5},
		{errs.Unauthenticated, 16},
		{errs.Code(1001), 2},
	}
	for _, tc := range tests {
		if got := GRPCStatus(tc.code); got != tc.want {
			t.Fatalf("GRPCStatus(%v) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

func TestFromGRPCStatus(t *testing.T) {
	if FromGRPCStatus(5) != errs.NotFound {
		t.Fatal("expected NotFound")
	}
	if FromGRPCStatus(99) != errs.Unknown {
		t.Fatal("unknown grpc status must map to Unknown")
	}
}

func TestHTTPStatus(t *testing.T) {
	tests := []struct {
		code errs.Code
		want int
	}{
		{errs.OK, http.StatusOK},
		{errs.InvalidArgument, http.StatusBadRequest},
		{errs.Unauthenticated, http.StatusUnauthorized},
		{errs.NotFound, http.StatusNotFound},
		{errs.Unimplemented, http.StatusNotImplemented},
		{errs.Internal, http.StatusInternalServerError},
		{errs.Unknown, http.StatusInternalServerError},
		{errs.Code(999), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		if got := HTTPStatus(tc.code); got != tc.want {
			t.Fatalf("HTTPStatus(%v) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

func TestGRPCStatusAllBuiltins(t *testing.T) {
	tests := []struct {
		code errs.Code
		want uint32
	}{
		{errs.OK, 0},
		{errs.InvalidArgument, 3},
		{errs.Unauthenticated, 16},
		{errs.NotFound, 5},
		{errs.Unimplemented, 12},
		{errs.Internal, 13},
		{errs.Unknown, 2},
	}
	for _, tc := range tests {
		if got := GRPCStatus(tc.code); got != tc.want {
			t.Fatalf("GRPCStatus(%v) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

func TestFromGRPCStatusRoundTrip(t *testing.T) {
	codes := []errs.Code{
		errs.OK,
		errs.InvalidArgument,
		errs.Unauthenticated,
		errs.NotFound,
		errs.Unimplemented,
		errs.Internal,
		errs.Unknown,
	}
	for _, code := range codes {
		if got := FromGRPCStatus(GRPCStatus(code)); got != code {
			t.Fatalf("round-trip %v = %v", code, got)
		}
	}
}
