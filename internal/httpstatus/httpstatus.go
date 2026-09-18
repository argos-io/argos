// Package httpstatus provides the single bidirectional map between HTTP
// status codes and status.Code.
//
// Outbound (ToHTTP) is used by framing/httpunary. Inbound (FromHTTP) is the
// fallback when framing/grpc receives an HTTP response without grpc-status.
//
// ToHTTP follows google.rpc.Code HTTP mappings and grpc-gateway's
// HTTPStatusFromCode. FromHTTP reverses those unique mappings; where several
// Codes share one HTTP status, the reverse prefers the most common Code
// (documented per case). Unknown or unmapped HTTP statuses yield Unknown.
package httpstatus

import (
	"net/http"

	"github.com/argos-io/argos/status"
)

// nginx non-standard status for client closed request (gRPC Canceled).
const statusClientClosedRequest = 499

// ToHTTP returns the HTTP status corresponding to c.
//
// Mapping matches googleapis google/rpc/code.proto HTTP comments and
// grpc-ecosystem/grpc-gateway runtime.HTTPStatusFromCode. Codes outside
// 0–16 (and any unrecognized value) map to 500.
func ToHTTP(c status.Code) int {
	switch c {
	case status.OK:
		return http.StatusOK // 200
	case status.Canceled:
		// google.rpc.Code and grpc-gateway use 499 (nginx "Client Closed Request").
		return statusClientClosedRequest
	case status.Unknown:
		return http.StatusInternalServerError // 500
	case status.InvalidArgument:
		return http.StatusBadRequest // 400
	case status.DeadlineExceeded:
		return http.StatusGatewayTimeout // 504
	case status.NotFound:
		return http.StatusNotFound // 404
	case status.AlreadyExists:
		return http.StatusConflict // 409
	case status.PermissionDenied:
		return http.StatusForbidden // 403
	case status.ResourceExhausted:
		return http.StatusTooManyRequests // 429
	case status.FailedPrecondition:
		// Deliberately not 412 Precondition Failed (googleapis / grpc-gateway).
		return http.StatusBadRequest // 400
	case status.Aborted:
		return http.StatusConflict // 409
	case status.OutOfRange:
		return http.StatusBadRequest // 400
	case status.Unimplemented:
		return http.StatusNotImplemented // 501
	case status.Internal:
		return http.StatusInternalServerError // 500
	case status.Unavailable:
		return http.StatusServiceUnavailable // 503
	case status.DataLoss:
		return http.StatusInternalServerError // 500
	case status.Unauthenticated:
		return http.StatusUnauthorized // 401
	default:
		return http.StatusInternalServerError // 500
	}
}

// FromHTTP returns the status.Code corresponding to an HTTP status.
//
// This is a semantic reverse of ToHTTP for httpunary / grpc HTTP fallback,
// not grpc-go's transport HTTPStatusConvTab (which e.g. maps 404→Unimplemented
// for HTTP/2 framing failures). Where multiple Codes share an HTTP status,
// the reverse prefers:
//
//	400 → InvalidArgument (also FailedPrecondition, OutOfRange outbound)
//	409 → AlreadyExists (also Aborted outbound)
//	500 → Unknown (also Internal, DataLoss outbound)
//
// 408 is accepted as Canceled alongside 499. Unrecognized statuses → Unknown.
func FromHTTP(httpStatus int) status.Code {
	switch httpStatus {
	case http.StatusOK: // 200
		return status.OK
	case http.StatusBadRequest: // 400
		return status.InvalidArgument
	case http.StatusUnauthorized: // 401
		return status.Unauthenticated
	case http.StatusForbidden: // 403
		return status.PermissionDenied
	case http.StatusNotFound: // 404
		return status.NotFound
	case http.StatusRequestTimeout, statusClientClosedRequest: // 408, 499
		return status.Canceled
	case http.StatusConflict: // 409
		return status.AlreadyExists
	case http.StatusTooManyRequests: // 429
		return status.ResourceExhausted
	case http.StatusInternalServerError: // 500
		return status.Unknown
	case http.StatusNotImplemented: // 501
		return status.Unimplemented
	case http.StatusServiceUnavailable: // 503
		return status.Unavailable
	case http.StatusGatewayTimeout: // 504
		return status.DeadlineExceeded
	default:
		return status.Unknown
	}
}
