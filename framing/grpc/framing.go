// Package grpc implements the gRPC framing protocol over HTTP/2.
//
// Reuse is always Concurrent. Client sessions open multiplexed streams via
// StreamConn; server sessions accept one HTTP request CarrierConn per Conn.
package grpc

import (
	"context"
	"fmt"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// Framing implements framing.Framing for gRPC × HTTP/2.
// Reuse is always Concurrent (multiplexed streams on one connection).
type Framing struct {
	compressors []compressor.Compressor
	sendName    string
}

// New returns a gRPC Framing. Reuse() is always framing.Concurrent.
// By default only the identity compressor is enabled. Duplicate compressor
// names or a send compressor that is not in the list return an error.
func New(opts ...Option) (framing.Framing, error) {
	return applyOptions(opts)
}

// Reuse implements framing.Framing.
func (f *Framing) Reuse() framing.ReuseModel { return framing.Concurrent }

func (f *Framing) compressorList() []compressor.Compressor {
	if f == nil || len(f.compressors) == 0 {
		return []compressor.Compressor{compressor.Identity}
	}
	return f.compressors
}

func (f *Framing) outboundSendName() string {
	if f == nil || f.sendName == "" {
		return compressor.Identity.Name()
	}
	return f.sendName
}

// NewClientSession implements framing.Framing. No handshake I/O.
func (f *Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	sc, ok := c.(transport.StreamConn)
	if !ok {
		return nil, fmt.Errorf("framing/grpc: client requires StreamConn")
	}
	return &clientSession{
		framing:     f,
		conn:        sc,
		cfg:         spec.Config,
		subtype:     CodecContentSubtype(spec.CodecName),
		compressors: f.compressorList(),
		sendName:    f.outboundSendName(),
		reusable:    true,
	}, nil
}

// NewServerSession implements framing.Framing. No handshake I/O.
// c must be a CarrierConn for one inbound HTTP request.
func (f *Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("framing/grpc: server requires CarrierConn")
	}
	return &serverSession{
		framing:     f,
		conn:        cc,
		carrier:     cc.Carrier(),
		cfg:         spec.Config,
		subtype:     CodecContentSubtype(spec.CodecName),
		compressors: f.compressorList(),
		sendName:    f.outboundSendName(),
	}, nil
}

var (
	_ framing.Framing       = (*Framing)(nil)
	_ framing.ClientSession = (*clientSession)(nil)
	_ framing.ServerSession = (*serverSession)(nil)
	_ framing.Call          = (*call)(nil)
	_ framing.ServerCall    = (*serverCall)(nil)
)
