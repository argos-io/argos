package wholebody

import (
	"encoding/json"
	"errors"

	"github.com/argos-io/argos/status"
)

// maxErrorBody is the upper bound for encoded / decoded status JSON bodies.
// Keeps error payloads well under typical MaxMessageSize defaults.
const maxErrorBody = 16 << 10 // 16 KiB

// errorWire is the bounded JSON error body for non-OK HTTP responses.
type errorWire struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// EncodeErrorBody marshals err into a bounded JSON body.
// Non-status errors become Unknown with err.Error() as the message.
func EncodeErrorBody(err error) []byte {
	if err == nil {
		return nil
	}
	code := status.CodeOf(err)
	msg := err.Error()
	var se *status.StatusError
	if errors.As(err, &se) && se != nil {
		msg = se.Message()
	}
	if len(msg) > maxErrorBody/2 {
		msg = msg[:maxErrorBody/2]
	}
	b, mErr := json.Marshal(errorWire{Code: int32(code), Message: msg})
	if mErr != nil {
		return []byte(`{"code":2,"message":"encode error"}`)
	}
	if len(b) > maxErrorBody {
		b = b[:maxErrorBody]
	}
	return b
}

// DecodeErrorBody parses a bounded JSON error body into a status error.
// Falls back to httpFallback when the body is empty or malformed.
func DecodeErrorBody(body []byte, httpFallback status.Code) error {
	if len(body) == 0 {
		return status.Error(httpFallback, "")
	}
	if len(body) > maxErrorBody {
		body = body[:maxErrorBody]
	}
	var w errorWire
	if err := json.Unmarshal(body, &w); err != nil {
		return status.Error(httpFallback, string(body))
	}
	code := status.Code(w.Code)
	if code == status.OK {
		code = httpFallback
	}
	return status.Error(code, w.Message)
}
