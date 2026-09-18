package grpc

import "strings"

const baseContentType = "application/grpc"

// ContentType builds the gRPC content-type header value for subtype.
// An empty subtype yields "application/grpc"; otherwise "application/grpc+"+subtype.
// subtype is assumed lower-case (callers should normalize).
func ContentType(subtype string) string {
	if subtype == "" {
		return baseContentType
	}
	return baseContentType + "+" + subtype
}

// ContentSubtype parses a gRPC content-type. contentType should already be
// lower-case. ok is false when the value is not a valid gRPC content-type.
//
// Matches grpc-go grpcutil.ContentSubtype: "application/grpc",
// "application/grpc+proto", and "application/grpc;…" forms are accepted.
func ContentSubtype(contentType string) (subtype string, ok bool) {
	if contentType == baseContentType {
		return "", true
	}
	if !strings.HasPrefix(contentType, baseContentType) {
		return "", false
	}
	switch contentType[len(baseContentType)] {
	case '+', ';':
		return contentType[len(baseContentType)+1:], true
	default:
		return "", false
	}
}

// CodecContentSubtype maps a SessionSpec / codec registry name to the gRPC
// content subtype used on the wire. "protobuf" maps to "proto" for grpc-go
// interoperability; other names pass through lower-cased.
func CodecContentSubtype(codecName string) string {
	switch strings.ToLower(codecName) {
	case "", "protobuf", "proto":
		return "proto"
	default:
		return strings.ToLower(codecName)
	}
}
