package grpc

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/argos-io/argos/internal/httpstatus"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// EncodeStatusTrailers builds trailers carrying grpc-status / grpc-message
// (and optional grpc-status-details-bin) plus encoded user trailer metadata.
func EncodeStatusTrailers(err error, user metadata.Metadata) transport.Headers {
	code := status.OK
	msg := ""
	if err != nil {
		code = status.CodeOf(err)
		msg = statusMessage(err)
	}
	out := EncodeTrailers(user)
	out = append(out, transport.Header{
		Name:  "grpc-status",
		Value: strconv.FormatUint(uint64(code), 10),
	})
	if msg != "" {
		out = append(out, transport.Header{
			Name:  "grpc-message",
			Value: EncodeGrpcMessage(msg),
		})
	}
	if wire, ok := EncodeStatusDetailsBin(err); ok {
		out = append(out, transport.Header{
			Name:  StatusDetailsBinKey,
			Value: wire,
		})
	}
	return out
}

// statusFromHeaders looks for grpc-status in hs (trailers or trailers-only
// headers). found is false when the key is absent.
func statusFromHeaders(hs transport.Headers) (code status.Code, msg string, details []string, found bool) {
	var statusVal, messageVal string
	var hasStatus bool
	for _, h := range hs {
		switch strings.ToLower(h.Name) {
		case "grpc-status":
			statusVal = h.Value
			hasStatus = true
		case "grpc-message":
			messageVal = h.Value
		case StatusDetailsBinKey:
			details = append(details, h.Value)
		}
	}
	if !hasStatus {
		return 0, "", nil, false
	}
	n, err := strconv.ParseUint(statusVal, 10, 32)
	if err != nil {
		// Illegal status text → Unknown; keep decoded message if present.
		return status.Unknown, DecodeGrpcMessage(messageVal), details, true
	}
	return status.Code(n), DecodeGrpcMessage(messageVal), details, true
}

// resolveCallStatus picks grpc-status from trailers, then response headers
// (trailers-only). When missing, maps the HTTP status via httpstatus.FromHTTP.
// HTTP 200 without grpc-status is never treated as success.
func resolveCallStatus(httpStatus int, headers, trailers transport.Headers) error {
	if code, msg, details, ok := statusFromHeaders(trailers); ok {
		return DecodeStatusDetailsBin(code, msg, details...)
	}
	if code, msg, details, ok := statusFromHeaders(headers); ok {
		return DecodeStatusDetailsBin(code, msg, details...)
	}
	return httpFallbackStatus(httpStatus)
}

func httpFallbackStatus(httpStatus int) error {
	if httpStatus == 0 {
		httpStatus = http.StatusOK
	}
	if httpStatus == http.StatusOK {
		return status.Error(status.Unknown, "grpc: missing grpc-status in response")
	}
	code := httpstatus.FromHTTP(httpStatus)
	return status.Error(code, fmt.Sprintf("grpc: missing grpc-status; HTTP %d", httpStatus))
}
