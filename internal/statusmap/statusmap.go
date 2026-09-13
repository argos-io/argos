// Package statusmap holds errs.Code ↔ wire status conversions for http1/http2.
package statusmap

import (
	"net/http"

	"github.com/argos-io/argos/errs"
)

// HTTPStatus maps errs.Code to an HTTP status code.
func HTTPStatus(code errs.Code) int {
	switch code {
	case errs.OK:
		return http.StatusOK
	case errs.InvalidArgument:
		return http.StatusBadRequest
	case errs.Unauthenticated:
		return http.StatusUnauthorized
	case errs.NotFound:
		return http.StatusNotFound
	case errs.Unimplemented:
		return http.StatusNotImplemented
	case errs.Internal, errs.Unknown:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

// GRPCStatus maps errs.Code to a grpc-status trailer value.
func GRPCStatus(code errs.Code) uint32 {
	switch code {
	case errs.OK:
		return 0
	case errs.InvalidArgument:
		return 3
	case errs.Unauthenticated:
		return 16
	case errs.NotFound:
		return 5
	case errs.Unimplemented:
		return 12
	case errs.Internal:
		return 13
	case errs.Unknown:
		return 2
	default:
		return 2
	}
}

// FromGRPCStatus maps a grpc-status trailer value back to errs.Code.
func FromGRPCStatus(status uint32) errs.Code {
	switch status {
	case 0:
		return errs.OK
	case 3:
		return errs.InvalidArgument
	case 16:
		return errs.Unauthenticated
	case 5:
		return errs.NotFound
	case 12:
		return errs.Unimplemented
	case 13:
		return errs.Internal
	case 2:
		return errs.Unknown
	default:
		return errs.Unknown
	}
}
