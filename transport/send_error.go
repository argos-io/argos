package transport

import "errors"

// sendError is the single concrete SendError every transport produces, so
// framing can recognise a send-direction failure without each transport
// inventing its own type and without switching on concrete transports.
type sendError struct {
	err         error
	receiveOpen bool
}

func (e *sendError) Error() string { return e.err.Error() }

// Unwrap keeps the underlying I/O error reachable with errors.Is/errors.As;
// SendError describes the direction, not the cause.
func (e *sendError) Unwrap() error { return e.err }

func (e *sendError) ReceiveOpen() bool { return e.receiveOpen }

// NewSendError classifies err as a send-direction failure. receiveOpen follows
// the conservative rule in SendError: pass false only when the receive
// direction of this exchange is known to be unrecoverable, and true when that
// cannot be decided immediately.
func NewSendError(err error, receiveOpen bool) SendError {
	if err == nil {
		err = errors.New("transport: send failed")
	}
	return &sendError{err: err, receiveOpen: receiveOpen}
}

// WrapSendError is NewSendError for a nil-able error: it returns nil for nil,
// passes an existing SendError through unchanged (a deeper layer already
// classified it), and otherwise wraps err with the given receiveOpen.
func WrapSendError(err error, receiveOpen bool) error {
	if err == nil {
		return nil
	}
	var se SendError
	if errors.As(err, &se) {
		return err
	}
	return &sendError{err: err, receiveOpen: receiveOpen}
}

// AsSendError reports whether err is, or wraps, a SendError. Framing uses it to
// decide between "the send direction is finished but the exchange still
// receives" and "the carrier is unrecoverable".
func AsSendError(err error) (SendError, bool) {
	var se SendError
	if errors.As(err, &se) {
		return se, true
	}
	return nil, false
}
