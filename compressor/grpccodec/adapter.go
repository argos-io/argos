// Package grpccodec adapts argos compressor.Compressor to grpc-go's
// encoding.Compressor and the reverse.
//
// The two interfaces differ only in Decompress: argos returns io.ReadCloser;
// grpc-go returns io.Reader (optionally ReadCloser).
package grpccodec

import (
	"io"

	"github.com/argos-io/argos/compressor"
	"google.golang.org/grpc/encoding"
)

// From wraps an argos Compressor as a grpc-go encoding.Compressor.
func From(c compressor.Compressor) encoding.Compressor {
	if c == nil {
		return nil
	}
	return fromAdapter{c: c}
}

type fromAdapter struct {
	c compressor.Compressor
}

func (a fromAdapter) Name() string { return a.c.Name() }

func (a fromAdapter) Compress(w io.Writer) (io.WriteCloser, error) {
	return a.c.Compress(w)
}

func (a fromAdapter) Decompress(r io.Reader) (io.Reader, error) {
	return a.c.Decompress(r)
}

// To wraps a grpc-go encoding.Compressor as an argos Compressor.
// If Decompress returns a non-closer Reader, it is wrapped with io.NopCloser.
func To(c encoding.Compressor) compressor.Compressor {
	if c == nil {
		return nil
	}
	return toAdapter{c: c}
}

type toAdapter struct {
	c encoding.Compressor
}

func (a toAdapter) Name() string { return a.c.Name() }

func (a toAdapter) Compress(dst io.Writer) (io.WriteCloser, error) {
	return a.c.Compress(dst)
}

func (a toAdapter) Decompress(src io.Reader) (io.ReadCloser, error) {
	r, err := a.c.Decompress(src)
	if err != nil {
		return nil, err
	}
	if rc, ok := r.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(r), nil
}
