// Package status defines argos status codes and errors.
//
// Code values match the gRPC numeric space. The package does not import
// protobuf or genproto; error details use the neutral Detail type.
package status

import (
	"errors"
	"fmt"
)

// Code is an open status space. Built-in values occupy 0–16 and match gRPC.
type Code uint32

const (
	OK                 Code = 0
	Canceled           Code = 1
	Unknown            Code = 2
	InvalidArgument    Code = 3
	DeadlineExceeded   Code = 4
	NotFound           Code = 5
	AlreadyExists      Code = 6
	PermissionDenied   Code = 7
	ResourceExhausted  Code = 8
	FailedPrecondition Code = 9
	Aborted            Code = 10
	OutOfRange         Code = 11
	Unimplemented      Code = 12
	Internal           Code = 13
	Unavailable        Code = 14
	DataLoss           Code = 15
	Unauthenticated    Code = 16
)

// StatusError is a status error recoverable with errors.As.
// Same-Code plain status errors match via errors.Is (message ignored).
type StatusError struct {
	code    Code
	msg     string
	details []Detail
	cause   error
}

// Error implements the error interface.
func (e *StatusError) Error() string {
	if e == nil {
		return ""
	}
	if e.msg != "" {
		return e.msg
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return fmt.Sprintf("status: code = %d", e.code)
}

// Is reports whether target is a plain StatusError with the same Code.
// Message text is ignored (gRPC-style). Sentinel-carrying StatusErrors only
// match another StatusError that unwraps to the same private cause.
func (e *StatusError) Is(target error) bool {
	t, ok := target.(*StatusError)
	if !ok {
		return false
	}
	if e.cause != nil || t.cause != nil {
		return e.cause != nil && t.cause != nil && e.cause == t.cause && e.code == t.code
	}
	return e.code == t.code
}

// Unwrap returns the underlying cause, if any (used by sentinel errors).
func (e *StatusError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Code returns the status code.
func (e *StatusError) Code() Code {
	if e == nil {
		return OK
	}
	return e.code
}

// Message returns the status message string (may be empty).
func (e *StatusError) Message() string {
	if e == nil {
		return ""
	}
	return e.msg
}

// Error returns an error carrying code and msg. It is recoverable with errors.As
// to *status.StatusError. Same-Code errors match via errors.Is (message ignored).
func Error(code Code, msg string) error {
	return &StatusError{code: code, msg: msg}
}

// CodeOf returns the Code of a status error.
// CodeOf(nil) is OK; non-status errors yield Unknown.
func CodeOf(err error) Code {
	if err == nil {
		return OK
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.code
	}
	return Unknown
}

// private sentinel causes — public vars wrap these so errors.Is works
// against the public var while CodeOf still sees the wrapping Code.
var (
	errCardinality       = errors.New("status: cardinality mismatch")
	errSessionsExhausted = errors.New("status: sessions exhausted")
	errCallsExhausted    = errors.New("status: calls exhausted")
)

// Sentinels — errors.Is-distinguishable and carry a wrapping Code.
var (
	// ErrCardinality is returned when a unary/client-streaming response count
	// does not match the method shape. It carries Internal.
	ErrCardinality error = &StatusError{
		code:  Internal,
		msg:   "status: cardinality mismatch",
		cause: errCardinality,
	}

	// ErrSessionsExhausted is returned when MaxSessionsPerEndpoint is exhausted
	// (or reconnect retries are spent). It carries ResourceExhausted.
	ErrSessionsExhausted error = &StatusError{
		code:  ResourceExhausted,
		msg:   "status: sessions exhausted",
		cause: errSessionsExhausted,
	}

	// ErrCallsExhausted is returned when MaxConcurrentCalls or MaxBufferedBytes
	// is exhausted. It carries ResourceExhausted.
	ErrCallsExhausted error = &StatusError{
		code:  ResourceExhausted,
		msg:   "status: calls exhausted",
		cause: errCallsExhausted,
	}
)

// Detail is a neutral error detail. Framing layers that speak protobuf
// translate these to/from wire formats; this package never imports protobuf.
type Detail struct {
	TypeURL string
	Value   []byte
}

// WithDetails returns a copy of err with details attached.
// Value slices are deep-copied. WithDetails(nil, ...) returns a clear local
// usage error whose CodeOf is not OK.
func WithDetails(err error, details ...Detail) error {
	if err == nil {
		return &StatusError{code: Internal, msg: "status: WithDetails called with nil error"}
	}
	copied := copyDetails(details)
	var se *StatusError
	if errors.As(err, &se) {
		out := &StatusError{
			code:    se.code,
			msg:     se.msg,
			cause:   se.cause,
			details: append(copyDetails(se.details), copied...),
		}
		return out
	}
	return &StatusError{
		code:    Unknown,
		msg:     err.Error(),
		cause:   err,
		details: copied,
	}
}

// DetailsOf returns a deep copy of details attached to err, or nil.
func DetailsOf(err error) []Detail {
	var se *StatusError
	if !errors.As(err, &se) {
		return nil
	}
	return copyDetails(se.details)
}

func copyDetails(in []Detail) []Detail {
	if len(in) == 0 {
		return nil
	}
	out := make([]Detail, len(in))
	for i := range in {
		out[i].TypeURL = in[i].TypeURL
		if in[i].Value != nil {
			out[i].Value = append([]byte(nil), in[i].Value...)
		}
	}
	return out
}
