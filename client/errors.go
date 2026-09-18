package client

import (
	"context"
	"errors"
	"strings"

	"github.com/argos-io/argos/status"
)

// ErrCallClosed is returned by CallStream operations after CallStream.Close.
var ErrCallClosed = errors.New("client: call closed")

// mappedStatus wraps a status error and an underlying cause so both
// status.CodeOf / errors.As(*StatusError) and errors.As(cause) work.
type mappedStatus struct {
	status error
	cause  error
}

func (e *mappedStatus) Error() string {
	if e == nil {
		return ""
	}
	if e.status != nil {
		return e.status.Error()
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return "client: mapped status"
}

func (e *mappedStatus) Unwrap() []error {
	if e == nil {
		return nil
	}
	out := make([]error, 0, 2)
	if e.status != nil {
		out = append(out, e.status)
	}
	if e.cause != nil {
		out = append(out, e.cause)
	}
	return out
}

func joinStatus(code status.Code, cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return &mappedStatus{
		status: status.Error(code, msg),
		cause:  cause,
	}
}

// mapEstablishErr maps connection-establishment failures (§7.3 fourth path).
// Dial/TLS/handshake I/O → Unavailable; HandshakeTimeout → DeadlineExceeded;
// narrow-interface assert failures stay as axis setup errors; already-status
// errors (e.g. ErrSessionsExhausted) pass through.
func mapEstablishErr(err error) error {
	if err == nil {
		return nil
	}
	if status.CodeOf(err) != status.Unknown {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return joinStatus(status.DeadlineExceeded, err)
	}
	if isNarrowInterfaceAssert(err) {
		return err
	}
	return joinStatus(status.Unavailable, err)
}

func isNarrowInterfaceAssert(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "requires ") ||
		strings.Contains(msg, "missing ") ||
		strings.Contains(msg, "narrow interface")
}
