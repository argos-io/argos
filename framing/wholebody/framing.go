// Package wholebody implements unary HTTP framing over a StreamConn /
// CarrierConn pair (typically transport/http1).
//
// Reuse is always Concurrent: the client Conn is an endpoint handle whose
// OpenStream calls are pooled by net/http; each server Conn is one request.
// Only descriptor.Unary is supported — Accept rejects other shapes before
// Filter/handler. SendHeaders is unsupported (no early HTTP 200). Finish
// commits once via UnaryResponseWriter.WriteResponse.
package wholebody

import (
	"context"
	"fmt"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// Framing implements framing.Framing for wholebody × HTTP/1.
type Framing struct{}

// New returns a wholebody Framing. Reuse() is always framing.Concurrent.
func New() framing.Framing { return Framing{} }

// Reuse implements framing.Framing.
func (Framing) Reuse() framing.ReuseModel { return framing.Concurrent }

// NewClientSession implements framing.Framing. No handshake I/O.
func (Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	sc, ok := c.(transport.StreamConn)
	if !ok {
		return nil, fmt.Errorf("framing/wholebody: client requires StreamConn")
	}
	return &clientSession{
		conn:     sc,
		cfg:      spec.Config,
		codec:    spec.CodecName,
		reusable: true,
	}, nil
}

// NewServerSession implements framing.Framing. No handshake I/O.
// c must be a CarrierConn for one inbound HTTP request.
func (Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("framing/wholebody: server requires CarrierConn")
	}
	return &serverSession{
		conn:    cc,
		carrier: cc.Carrier(),
		cfg:     spec.Config,
		codec:   spec.CodecName,
	}, nil
}

var (
	_ framing.Framing       = Framing{}
	_ framing.ClientSession = (*clientSession)(nil)
	_ framing.ServerSession = (*serverSession)(nil)
	_ framing.Call          = (*call)(nil)
	_ framing.ServerCall    = (*serverCall)(nil)
)
