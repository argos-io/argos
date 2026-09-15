package grpc

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/status"
)

func acceptNames(list []compressor.Compressor) []string {
	names := make([]string, 0, len(list))
	for _, c := range list {
		if c != nil {
			names = append(names, c.Name())
		}
	}
	return names
}

func parseAcceptEncoding(v string) map[string]struct{} {
	out := make(map[string]struct{})
	if v == "" {
		return out
	}
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out[p] = struct{}{}
	}
	return out
}

func peerAccepts(accept map[string]struct{}, name string) bool {
	if name == "" || name == compressor.Identity.Name() {
		return true
	}
	_, ok := accept[name]
	return ok
}

// resolveSendCompressor picks the outbound compressor for a call.
// Clients use the configured send name. Servers only use a non-identity
// compressor when the peer's grpc-accept-encoding lists it; otherwise identity.
func resolveSendCompressor(list []compressor.Compressor, sendName string, peerAccept map[string]struct{}, isServer bool) compressor.Compressor {
	c, ok := compressor.Find(sendName, list)
	if !ok || c == nil {
		return compressor.Identity
	}
	if !isServer {
		return c
	}
	if !peerAccepts(peerAccept, c.Name()) {
		return compressor.Identity
	}
	return c
}

func compressMessage(c compressor.Compressor, data []byte) (compressed bool, payload []byte, err error) {
	if c == nil || c.Name() == compressor.Identity.Name() {
		return false, data, nil
	}
	var buf bytes.Buffer
	wc, err := c.Compress(&buf)
	if err != nil {
		return false, nil, err
	}
	if _, err := wc.Write(data); err != nil {
		_ = wc.Close()
		return false, nil, err
	}
	if err := wc.Close(); err != nil {
		return false, nil, err
	}
	return true, buf.Bytes(), nil
}

func decompressMessage(c compressor.Compressor, data []byte, maxMsg int64) ([]byte, error) {
	if c == nil {
		return nil, status.Error(status.Unimplemented, "framing/grpc: missing compressor for compressed message")
	}
	rc, err := c.Decompress(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var r io.Reader = rc
	limit := int64(-1)
	if maxMsg > 0 {
		limit = maxMsg + 1
		r = io.LimitReader(rc, limit)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if maxMsg > 0 && int64(len(out)) > maxMsg {
		return nil, status.Error(status.ResourceExhausted,
			fmt.Sprintf("framing/grpc: decompressed payload %d > max %d", len(out), maxMsg))
	}
	return out, nil
}

func unsupportedEncoding(enc string) error {
	if enc == "" {
		enc = "unknown"
	}
	return status.Error(status.Unimplemented,
		fmt.Sprintf("framing/grpc: compression algorithm %q not enabled", enc))
}
