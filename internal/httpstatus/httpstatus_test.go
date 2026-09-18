package httpstatus

import (
	"testing"

	"github.com/argos-io/argos/status"
)

// Every built-in Code 0–16 must have a positive HTTP correspondence.
func TestToHTTP_EveryCodeMapped(t *testing.T) {
	for c := status.Code(0); c <= 16; c++ {
		httpStatus := ToHTTP(c)
		if httpStatus <= 0 {
			t.Errorf("ToHTTP(%d) = %d, want > 0", c, httpStatus)
		}
	}
}

func TestToHTTP_Table(t *testing.T) {
	cases := []struct {
		code status.Code
		http int
	}{
		{status.OK, 200},
		{status.Canceled, 499},
		{status.Unknown, 500},
		{status.InvalidArgument, 400},
		{status.DeadlineExceeded, 504},
		{status.NotFound, 404},
		{status.AlreadyExists, 409},
		{status.PermissionDenied, 403},
		{status.ResourceExhausted, 429},
		{status.FailedPrecondition, 400},
		{status.Aborted, 409},
		{status.OutOfRange, 400},
		{status.Unimplemented, 501},
		{status.Internal, 500},
		{status.Unavailable, 503},
		{status.DataLoss, 500},
		{status.Unauthenticated, 401},
	}
	for _, tc := range cases {
		if got := ToHTTP(tc.code); got != tc.http {
			t.Errorf("ToHTTP(%v) = %d, want %d", tc.code, got, tc.http)
		}
	}
	if got := ToHTTP(99); got != 500 {
		t.Errorf("ToHTTP(99) = %d, want 500", got)
	}
}

func TestFromHTTP_Known(t *testing.T) {
	cases := []struct {
		http int
		code status.Code
	}{
		{200, status.OK},
		{400, status.InvalidArgument},
		{401, status.Unauthenticated},
		{403, status.PermissionDenied},
		{404, status.NotFound},
		{408, status.Canceled},
		{409, status.AlreadyExists},
		{429, status.ResourceExhausted},
		{499, status.Canceled},
		{500, status.Unknown},
		{501, status.Unimplemented},
		{503, status.Unavailable},
		{504, status.DeadlineExceeded},
	}
	for _, tc := range cases {
		if got := FromHTTP(tc.http); got != tc.code {
			t.Errorf("FromHTTP(%d) = %v, want %v", tc.http, got, tc.code)
		}
	}
}

func TestFromHTTP_Unknown(t *testing.T) {
	for _, httpStatus := range []int{0, 201, 418, 502, 599, -1} {
		if got := FromHTTP(httpStatus); got != status.Unknown {
			t.Errorf("FromHTTP(%d) = %v, want Unknown", httpStatus, got)
		}
	}
}

// Round-trip for Codes whose ToHTTP target is uniquely reversed by FromHTTP.
func TestRoundTrip_WhereDefined(t *testing.T) {
	unique := []status.Code{
		status.OK,
		status.Canceled,
		status.Unknown,
		status.InvalidArgument,
		status.DeadlineExceeded,
		status.NotFound,
		status.AlreadyExists,
		status.PermissionDenied,
		status.ResourceExhausted,
		status.Unimplemented,
		status.Unavailable,
		status.Unauthenticated,
	}
	for _, c := range unique {
		httpStatus := ToHTTP(c)
		if got := FromHTTP(httpStatus); got != c {
			t.Errorf("FromHTTP(ToHTTP(%v)) = %v, want %v (http=%d)", c, got, c, httpStatus)
		}
	}
}
