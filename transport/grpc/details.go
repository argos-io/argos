package grpc

import (
	"encoding/base64"
	"errors"
	"fmt"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/argos-io/argos/status"
)

// StatusDetailsBinKey is the gRPC trailer carrying a serialized google.rpc.Status.
const StatusDetailsBinKey = "grpc-status-details-bin"

// EncodeStatusDetailsBin encodes err as the wire value of grpc-status-details-bin
// (unpadded base64 of a google.rpc.Status protobuf). ok is false when the
// trailer must not be emitted: nil error, OK code, or no attached details (§7.2).
//
// The Status protobuf uses the same code and message as the caller should put
// in grpc-status / grpc-message.
func EncodeStatusDetailsBin(err error) (wireValue string, ok bool) {
	raw, ok := encodeStatusDetailsRaw(err)
	if !ok {
		return "", false
	}
	return base64.RawStdEncoding.EncodeToString(raw), true
}

// DecodeStatusDetailsBin reconstructs a status error from grpc-status code,
// grpc-message text, and zero or more wire values of grpc-status-details-bin
// (padded or unpadded base64). Semantics match §7.2 / grpc-go NewWithProto:
//
//   - missing or multiple values → status without details
//   - corrupt protobuf / bad base64 → status without details (no panic)
//   - code mismatch → status.Internal with diagnostic message
//   - code match → adopt message and details from the Status proto
func DecodeStatusDetailsBin(code status.Code, message string, wireValues ...string) error {
	raws := make([][]byte, 0, len(wireValues))
	for _, w := range wireValues {
		b, err := decodeBinHeader(w)
		if err != nil {
			// Treat base64 failure as corrupt details: fall back (§7.2 / acceptance).
			return fallbackStatus(code, message)
		}
		raws = append(raws, b)
	}
	return decodeStatusDetailsRaw(code, message, raws...)
}

func encodeStatusDetailsRaw(err error) ([]byte, bool) {
	if err == nil {
		return nil, false
	}
	code := status.CodeOf(err)
	if code == status.OK {
		return nil, false
	}
	details := status.DetailsOf(err)
	if len(details) == 0 {
		return nil, false
	}

	msg := statusMessage(err)
	pb := &spb.Status{
		Code:    int32(code),
		Message: msg,
		Details: make([]*anypb.Any, 0, len(details)),
	}
	for _, d := range details {
		pb.Details = append(pb.Details, &anypb.Any{
			TypeUrl: d.TypeURL,
			Value:   append([]byte(nil), d.Value...),
		})
	}
	raw, err := proto.Marshal(pb)
	if err != nil {
		return nil, false
	}
	return raw, true
}

func decodeStatusDetailsRaw(code status.Code, message string, rawProtos ...[]byte) error {
	if len(rawProtos) != 1 {
		return fallbackStatus(code, message)
	}
	st := &spb.Status{}
	if err := proto.Unmarshal(rawProtos[0], st); err != nil {
		return fallbackStatus(code, message)
	}
	if status.Code(st.Code) != code {
		return status.Error(status.Internal, fmt.Sprintf(
			"grpc-status-details-bin mismatch: grpc-status=%v, grpc-message=%q, grpc-status-details-bin={code:%d message:%q details:%d}",
			code, message, st.Code, st.Message, len(st.Details),
		))
	}
	// Codes match: adopt message and details from the Status proto (§7.2).
	out := status.Error(code, st.Message)
	if len(st.Details) == 0 {
		return out
	}
	details := make([]status.Detail, 0, len(st.Details))
	for _, any := range st.Details {
		if any == nil {
			continue
		}
		details = append(details, status.Detail{
			TypeURL: any.TypeUrl,
			Value:   append([]byte(nil), any.Value...),
		})
	}
	if len(details) == 0 {
		return out
	}
	return status.WithDetails(out, details...)
}

func fallbackStatus(code status.Code, message string) error {
	if code == status.OK {
		return nil
	}
	return status.Error(code, message)
}

func statusMessage(err error) string {
	var se *status.StatusError
	if errors.As(err, &se) {
		if m := se.Message(); m != "" {
			return m
		}
	}
	return err.Error()
}
