package grpc

// IsReservedHeader reports whether hdr is a gRPC/HTTP2 header that must not
// appear as user metadata. Names are assumed lower-case.
//
// Matches grpc-go isReservedHeader: pseudo-headers (leading ':') and the
// listed protocol keys. Intentionally NOT reserved (may pass via metadata):
// grpc-previous-rpc-attempts, grpc-retry-pushback-ms, grpc-accept-encoding.
func IsReservedHeader(hdr string) bool {
	if hdr != "" && hdr[0] == ':' {
		return true
	}
	switch hdr {
	case "content-type",
		"user-agent",
		"grpc-message-type",
		"grpc-encoding",
		"grpc-message",
		"grpc-status",
		"grpc-timeout",
		"te":
		return true
	default:
		return false
	}
}

// IsWhitelistedHeader reports whether a reserved header should still be
// exposed to application metadata (grpc-go whitelist).
func IsWhitelistedHeader(hdr string) bool {
	switch hdr {
	case ":authority", "user-agent":
		return true
	default:
		return false
	}
}
