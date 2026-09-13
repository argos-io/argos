// Package errs defines argos status codes and errors.
package errs

import "errors"

// Code is argos's own status space. Any int is legal; built-in values occupy 0–6.
type Code int

const (
	OK              Code = 0
	InvalidArgument Code = 1
	Unauthenticated Code = 2
	NotFound        Code = 3
	Unimplemented   Code = 4
	Internal        Code = 5
	Unknown         Code = 6
)

type codedError struct {
	code Code
	msg  string
}

func (e *codedError) Error() string { return e.msg }

// Is reports whether target carries the same Code. Message text is ignored,
// matching gRPC-style status equality (any NotFound matches any NotFound).
func (e *codedError) Is(target error) bool {
	t, ok := target.(*codedError)
	return ok && e.code == t.code
}

// Error returns an error carrying code and msg. It is recoverable with errors.As.
func Error(code Code, msg string) error {
	return &codedError{code: code, msg: msg}
}

// CodeOf returns the Code of an argos error, or Unknown for any other error.
func CodeOf(err error) Code {
	var ce *codedError
	if errors.As(err, &ce) {
		return ce.code
	}
	return Unknown
}
