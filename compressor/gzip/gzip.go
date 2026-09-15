// Package gzip implements compressor.Compressor with the gzip algorithm.
//
// It is provided with the library but must be enabled explicitly via
// binding/grpc Options; it does not self-register.
package gzip

import (
	"compress/gzip"
	"io"

	"github.com/argos-io/argos/compressor"
)

// Name is the compressor name used in gRPC content-coding headers.
const Name = "gzip"

type gzipCompressor struct{}

// New returns a gzip Compressor. Name is "gzip".
func New() compressor.Compressor { return gzipCompressor{} }

func (gzipCompressor) Name() string { return Name }

func (gzipCompressor) Compress(dst io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(dst), nil
}

func (gzipCompressor) Decompress(src io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(src)
}
