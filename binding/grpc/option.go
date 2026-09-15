package grpc

import (
	"crypto/tls"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/compressor"
)

// Option configures New.
type Option interface {
	apply(*options)
}

type options struct {
	serverTLS   *tls.Config
	clientTLS   *tls.Config
	authority   string
	compressors []compressor.Compressor
	sendName    string
	codec       codec.Codec
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }

// WithTLS sets the server-side TLS config (certificates, optional ClientCAs).
// It is an alias for WithServerTLS and matches the §4.2 README name.
func WithTLS(cfg *tls.Config) Option {
	return WithServerTLS(cfg)
}

// WithServerTLS sets the server TLS config passed to transport/http2.WithServerTLS.
func WithServerTLS(cfg *tls.Config) Option {
	return optionFunc(func(o *options) { o.serverTLS = cfg })
}

// WithClientTLS sets the client TLS config (RootCAs, certificates for mTLS,
// optional ServerName) passed to transport/http2.WithClientTLS.
func WithClientTLS(cfg *tls.Config) Option {
	return optionFunc(func(o *options) { o.clientTLS = cfg })
}

// WithAuthority sets the TLS ServerName / SNI (and preferred authority hint)
// applied to the client TLS config when ServerName is empty.
func WithAuthority(authority string) Option {
	return optionFunc(func(o *options) { o.authority = authority })
}

// WithCompressor adds message compressors to the framing/grpc list.
// Identity is always present; gzip and custom algorithms must be passed here.
func WithCompressor(cs ...compressor.Compressor) Option {
	return optionFunc(func(o *options) {
		o.compressors = append(o.compressors, cs...)
	})
}

// WithSendCompressor selects the outbound grpc-encoding. The name must be
// present after WithCompressor (identity is always available). Empty means
// identity.
func WithSendCompressor(name string) Option {
	return optionFunc(func(o *options) { o.sendName = name })
}

// WithCodec selects the Codec for the binding. Default is codec/protobuf.
// Pass codec/json.New() for application/grpc+json.
func WithCodec(c codec.Codec) Option {
	return optionFunc(func(o *options) { o.codec = c })
}
