// Package compressor defines message compression algorithms for gRPC session.
//
// Only framing/grpc and binding/grpc may import this package. Algorithms are
// injected via binding/grpc Options; there is no process-level registry.
package compressor

import "io"

// Compressor compresses and decompresses message payloads.
//
// The signature matches grpc-go's encoding.Compressor except Decompress returns
// io.ReadCloser (for pooling and budget return). Name must be immutable and
// implementations must support concurrent Compress/Decompress calls.
type Compressor interface {
	Name() string
	Compress(dst io.Writer) (io.WriteCloser, error)
	Decompress(src io.Reader) (io.ReadCloser, error)
}

// Find returns the first Compressor in list whose Name equals name.
// Unknown algorithms are not found; the caller decides how to reject.
func Find(name string, list []Compressor) (Compressor, bool) {
	for _, c := range list {
		if c != nil && c.Name() == name {
			return c, true
		}
	}
	return nil, false
}

// Identity is the no-op compressor. Its Name is "identity".
var Identity Compressor = identity{}

type identity struct{}

func (identity) Name() string { return "identity" }

func (identity) Compress(dst io.Writer) (io.WriteCloser, error) {
	return nopWriteCloser{Writer: dst}, nil
}

func (identity) Decompress(src io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(src), nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
