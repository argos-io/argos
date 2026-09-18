// Package grpc implements the gRPC framing protocol over HTTP/2.
//
// Reuse is always Concurrent. Client sessions open multiplexed streams via
// StreamConn; server sessions accept one HTTP request CarrierConn per Conn.
package grpc

import (
	"context"
	"fmt"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/transport"
)

// framing implements session.Framing for gRPC × HTTP/2.
type framing struct {
	compressors []compressor.Compressor
	sendName    string
}

func newFraming(extra []compressor.Compressor, sendName string) (session.Framing, error) {
	list := []compressor.Compressor{compressor.Identity}
	seen := map[string]struct{}{compressor.Identity.Name(): {}}
	for _, c := range extra {
		if c == nil {
			return nil, fmt.Errorf("grpc: nil compressor")
		}
		name := c.Name()
		if name == "" {
			return nil, fmt.Errorf("grpc: compressor with empty name")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("grpc: duplicate compressor %q", name)
		}
		seen[name] = struct{}{}
		list = append(list, c)
	}
	if sendName == "" {
		sendName = compressor.Identity.Name()
	}
	if _, ok := compressor.Find(sendName, list); !ok {
		return nil, fmt.Errorf("grpc: send compressor %q not configured", sendName)
	}
	return &framing{compressors: list, sendName: sendName}, nil
}

func (f *framing) Reuse() session.ReuseModel { return session.Concurrent }

func (f *framing) compressorList() []compressor.Compressor {
	if f == nil || len(f.compressors) == 0 {
		return []compressor.Compressor{compressor.Identity}
	}
	return f.compressors
}

func (f *framing) outboundSendName() string {
	if f == nil || f.sendName == "" {
		return compressor.Identity.Name()
	}
	return f.sendName
}

func (f *framing) NewClientSession(ctx context.Context, c transport.Conn, spec session.SessionSpec) (session.ClientSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	sc, ok := c.(transport.StreamConn)
	if !ok {
		return nil, fmt.Errorf("grpc: client requires StreamConn")
	}
	return &clientSession{
		framing:     f,
		conn:        sc,
		cfg:         spec.Options,
		subtype:     CodecContentSubtype(spec.CodecName),
		compressors: f.compressorList(),
		sendName:    f.outboundSendName(),
		reusable:    true,
	}, nil
}

func (f *framing) NewServerSession(ctx context.Context, c transport.Conn, spec session.SessionSpec) (session.ServerSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("grpc: server requires CarrierConn")
	}
	return &serverSession{
		framing:     f,
		conn:        cc,
		carrier:     cc.Carrier(),
		cfg:         spec.Options,
		subtype:     CodecContentSubtype(spec.CodecName),
		compressors: f.compressorList(),
		sendName:    f.outboundSendName(),
	}, nil
}

var (
	_ session.Framing       = (*framing)(nil)
	_ session.ClientSession = (*clientSession)(nil)
	_ session.ServerSession = (*serverSession)(nil)
	_ session.Call          = (*call)(nil)
	_ session.ServerCall    = (*serverCall)(nil)
)
