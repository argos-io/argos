package grpc

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	spaceByte   = ' '
	tildeByte   = '~'
	percentByte = '%'
)

// EncodeGrpcMessage percent-encodes a grpc-message value byte-wise per the
// gRPC HTTP/2 spec (grpc-go compatible). Printable ASCII except '%' passes
// through; all other bytes become %XX. Does not use url.PathEscape.
func EncodeGrpcMessage(msg string) string {
	if msg == "" {
		return ""
	}
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if !(c >= spaceByte && c <= tildeByte && c != percentByte) {
			return encodeGrpcMessageUnchecked(msg)
		}
	}
	return msg
}

func encodeGrpcMessageUnchecked(msg string) string {
	var sb strings.Builder
	for len(msg) > 0 {
		r, size := utf8.DecodeRuneInString(msg)
		for _, b := range []byte(string(r)) {
			if size > 1 {
				fmt.Fprintf(&sb, "%%%02X", b)
				continue
			}
			if b >= spaceByte && b <= tildeByte && b != percentByte {
				sb.WriteByte(b)
			} else {
				fmt.Fprintf(&sb, "%%%02X", b)
			}
		}
		msg = msg[size:]
	}
	return sb.String()
}

// DecodeGrpcMessage reverses EncodeGrpcMessage. Malformed percent sequences
// are left intact; a corrupt message never discards an already-parsed status.
func DecodeGrpcMessage(msg string) string {
	if msg == "" {
		return ""
	}
	for i := 0; i < len(msg); i++ {
		if msg[i] == percentByte && i+2 < len(msg) {
			return decodeGrpcMessageUnchecked(msg)
		}
	}
	return msg
}

func decodeGrpcMessageUnchecked(msg string) string {
	var sb strings.Builder
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if c == percentByte && i+2 < len(msg) {
			parsed, err := strconv.ParseUint(msg[i+1:i+3], 16, 8)
			if err != nil {
				sb.WriteByte(c)
			} else {
				sb.WriteByte(byte(parsed))
				i += 2
			}
		} else {
			sb.WriteByte(c)
		}
	}
	return sb.String()
}
