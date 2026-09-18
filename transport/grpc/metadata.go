package grpc

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

const binHdrSuffix = "-bin"

// EncodeMetadata encodes user metadata into opaque transport headers.
//
// Rules (§7.4 / grpc-go compatible):
//   - keys are lower-cased
//   - reserved keys are stripped (not emitted)
//   - -bin values are base64-encoded without padding (RawStdEncoding)
//   - each value becomes its own transport.Header (no comma-joining on encode)
func EncodeMetadata(md metadata.Metadata) transport.Headers {
	if len(md) == 0 {
		return nil
	}
	var out transport.Headers
	for k, vs := range md {
		key := strings.ToLower(k)
		if IsReservedHeader(key) {
			continue
		}
		for _, v := range vs {
			out = append(out, transport.Header{
				Name:  key,
				Value: encodeMetadataValue(key, v),
			})
		}
	}
	return out
}

// DecodeMetadata decodes transport headers into user metadata.
//
// Rules:
//   - reserved keys are skipped unless whitelisted (:authority, user-agent)
//   - -bin values accept padded and unpadded base64
//   - comma-joined -bin values are split then decoded (§7.4)
//
// A malformed binary value never discards the rest of the set: like grpc-go
// (internal/transport/http2_client.go), the offending value is skipped, every
// other header is still decoded, and the first failure is reported alongside
// the result. The returned Metadata is therefore always non-nil.
func DecodeMetadata(hs transport.Headers) (metadata.Metadata, error) {
	if len(hs) == 0 {
		return metadata.Metadata{}, nil
	}
	out := make(metadata.Metadata)
	var firstErr error
	for _, h := range hs {
		key := strings.ToLower(h.Name)
		if IsReservedHeader(key) && !IsWhitelistedHeader(key) {
			continue
		}
		vals, err := decodeMetadataValues(key, h.Value)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("grpc: malformed binary metadata %q in header %q: %w", h.Value, key, err)
			}
			continue
		}
		out[key] = append(out[key], vals...)
	}
	return out, firstErr
}

func encodeMetadataValue(key, v string) string {
	if strings.HasSuffix(key, binHdrSuffix) {
		return base64.RawStdEncoding.EncodeToString([]byte(v))
	}
	return v
}

func decodeMetadataValues(key, v string) ([]string, error) {
	if !strings.HasSuffix(key, binHdrSuffix) {
		return []string{v}, nil
	}
	// PROTOCOL-HTTP2: multiple binary values may be comma-joined in one field.
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		b, err := decodeBinHeader(p)
		if err != nil {
			return nil, err
		}
		out = append(out, string(b))
	}
	return out, nil
}

func decodeBinHeader(v string) ([]byte, error) {
	if len(v)%4 == 0 {
		return base64.StdEncoding.DecodeString(v)
	}
	return base64.RawStdEncoding.DecodeString(v)
}
