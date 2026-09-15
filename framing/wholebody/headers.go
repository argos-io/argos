package wholebody

import (
	"strings"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

// ContentType returns the HTTP Content-Type for a codec name.
// Empty or "json" → application/json; otherwise application/<name>.
func ContentType(codecName string) string {
	if codecName == "" || codecName == "json" {
		return "application/json"
	}
	return "application/" + codecName
}

// reservedHeader reports protocol / hop-by-hop keys Framing owns.
func reservedHeader(key string) bool {
	switch strings.ToLower(key) {
	case "content-type", "content-length", "transfer-encoding",
		"connection", "keep-alive", "proxy-connection",
		"te", "trailer", "upgrade", "host":
		return true
	default:
		return false
	}
}

// EncodeMetadata encodes user metadata into opaque transport headers.
func EncodeMetadata(md metadata.Metadata) transport.Headers {
	if len(md) == 0 {
		return nil
	}
	var out transport.Headers
	for k, vs := range md {
		key := strings.ToLower(k)
		if reservedHeader(key) {
			continue
		}
		for _, v := range vs {
			out = append(out, transport.Header{Name: key, Value: v})
		}
	}
	return out
}

// DecodeMetadata decodes transport headers into user metadata.
func DecodeMetadata(hs transport.Headers) metadata.Metadata {
	if len(hs) == 0 {
		return metadata.Metadata{}
	}
	out := make(metadata.Metadata)
	for _, h := range hs {
		key := strings.ToLower(h.Name)
		if reservedHeader(key) {
			continue
		}
		out[key] = append(out[key], h.Value)
	}
	return out
}

// BuildRequestPreface builds OpenStream preface: path, content-type, metadata.
func BuildRequestPreface(m descriptor.Method, codecName string, outgoing metadata.Metadata) transport.RequestPreface {
	hs := transport.Headers{
		{Name: "content-type", Value: ContentType(codecName)},
	}
	hs = append(hs, EncodeMetadata(outgoing)...)
	return transport.RequestPreface{
		RequestTarget: MethodPath(m),
		Headers:       hs,
	}
}
