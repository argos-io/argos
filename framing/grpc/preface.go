package grpc

import (
	"fmt"
	"strings"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

// PrefaceOptions configures BuildRequestPreface.
type PrefaceOptions struct {
	Method         descriptor.Method
	Outgoing       metadata.Metadata // typically CallMetadata.OutgoingHeaders()
	Timeout        time.Duration     // zero omits grpc-timeout
	ContentSubtype string            // e.g. "proto", "json"; empty → application/grpc
	// SendCompressor, if non-empty, sets grpc-encoding.
	SendCompressor string
	// AcceptCompressors, if non-empty, sets grpc-accept-encoding (comma-joined).
	AcceptCompressors []string
}

// BuildRequestPreface builds a transport.RequestPreface for OpenStream:
// path, content-type, te: trailers, optional grpc-timeout / compression, and
// encoded user metadata (§4.7 / §7.4).
func BuildRequestPreface(opts PrefaceOptions) transport.RequestPreface {
	path := MethodPath(opts.Method)
	hs := transport.Headers{
		{Name: "content-type", Value: ContentType(opts.ContentSubtype)},
		{Name: "te", Value: "trailers"},
	}
	if opts.Timeout > 0 {
		hs = append(hs, transport.Header{Name: "grpc-timeout", Value: EncodeTimeout(opts.Timeout)})
	}
	if opts.SendCompressor != "" {
		hs = append(hs, transport.Header{Name: "grpc-encoding", Value: opts.SendCompressor})
	}
	if len(opts.AcceptCompressors) > 0 {
		hs = append(hs, transport.Header{
			Name:  "grpc-accept-encoding",
			Value: strings.Join(opts.AcceptCompressors, ","),
		})
	}
	hs = append(hs, EncodeMetadata(opts.Outgoing)...)
	return transport.RequestPreface{
		RequestTarget: path,
		Headers:       hs,
	}
}

// RequestInfo is the decoded view of an inbound gRPC request's path and headers.
type RequestInfo struct {
	Service        string
	Method         string
	Metadata       metadata.Metadata
	Timeout        time.Duration
	HasTimeout     bool
	ContentSubtype string
	Encoding       string // grpc-encoding, if present
	AcceptEncoding string // grpc-accept-encoding, if present
}

// ParseRequestHeaders decodes an inbound request target and headers.
func ParseRequestHeaders(target string, hs transport.Headers) (RequestInfo, error) {
	var info RequestInfo
	svc, meth, err := ParseMethodPath(target)
	if err != nil {
		return info, err
	}
	info.Service, info.Method = svc, meth

	md := make(metadata.Metadata)
	var contentType string
	for _, h := range hs {
		key := strings.ToLower(h.Name)
		switch key {
		case "content-type":
			contentType = strings.ToLower(h.Value)
		case "grpc-timeout":
			d, err := DecodeTimeout(h.Value)
			if err != nil {
				return RequestInfo{}, fmt.Errorf("framing/grpc: malformed grpc-timeout: %w", err)
			}
			info.Timeout = d
			info.HasTimeout = true
		case "grpc-encoding":
			info.Encoding = h.Value
		case "grpc-accept-encoding":
			info.AcceptEncoding = h.Value
		case "te":
			// required on client; ignore on parse
		default:
			if IsReservedHeader(key) && !IsWhitelistedHeader(key) {
				continue
			}
			vals, err := decodeMetadataValues(key, h.Value)
			if err != nil {
				return RequestInfo{}, fmt.Errorf("framing/grpc: malformed binary metadata %q in header %q: %w", h.Value, key, err)
			}
			md[key] = append(md[key], vals...)
		}
	}
	if contentType == "" {
		return RequestInfo{}, fmt.Errorf("framing/grpc: missing content-type")
	}
	subtype, ok := ContentSubtype(contentType)
	if !ok {
		return RequestInfo{}, fmt.Errorf("framing/grpc: invalid gRPC content-type %q", contentType)
	}
	info.ContentSubtype = subtype
	info.Metadata = md
	return info, nil
}

// EncodeResponseHeaders builds initial response headers: content-type plus
// encoded user metadata (reserved keys stripped).
func EncodeResponseHeaders(contentSubtype string, md metadata.Metadata) transport.Headers {
	hs := transport.Headers{
		{Name: "content-type", Value: ContentType(contentSubtype)},
	}
	return append(hs, EncodeMetadata(md)...)
}

// EncodeTrailers encodes user trailer metadata (reserved keys stripped).
// Status trailers (grpc-status / grpc-message / details) are Task 3.4.
func EncodeTrailers(md metadata.Metadata) transport.Headers {
	return EncodeMetadata(md)
}

// DecodeResponseHeaders decodes inbound response headers into user metadata,
// content subtype, and compression encoding. Reserved protocol keys other than
// the whitelist are not placed in Metadata.
func DecodeResponseHeaders(hs transport.Headers) (md metadata.Metadata, contentSubtype string, encoding string, err error) {
	md = make(metadata.Metadata)
	var contentType string
	for _, h := range hs {
		key := strings.ToLower(h.Name)
		switch key {
		case "content-type":
			contentType = strings.ToLower(h.Value)
		case "grpc-encoding":
			encoding = h.Value
		default:
			if IsReservedHeader(key) && !IsWhitelistedHeader(key) {
				continue
			}
			vals, derr := decodeMetadataValues(key, h.Value)
			if derr != nil {
				return nil, "", "", fmt.Errorf("framing/grpc: malformed binary metadata %q in header %q: %w", h.Value, key, derr)
			}
			md[key] = append(md[key], vals...)
		}
	}
	if contentType != "" {
		var ok bool
		contentSubtype, ok = ContentSubtype(contentType)
		if !ok {
			return nil, "", "", fmt.Errorf("framing/grpc: invalid gRPC content-type %q", contentType)
		}
	}
	return md, contentSubtype, encoding, nil
}

// DecodeTrailers decodes inbound trailers into user metadata.
// Status keys (grpc-status, grpc-message, grpc-status-details-bin) are left
// for Task 3.4; they are reserved / protocol and not returned here except
// grpc-status-details-bin which ends in -bin and is not in IsReservedHeader —
// callers that need status should inspect raw headers in 3.4.
func DecodeTrailers(hs transport.Headers) (metadata.Metadata, error) {
	return DecodeMetadata(hs)
}
