// Package grpc implements the gRPC framing protocol over HTTP/2.
//
// Task 3.3 delivers request/response header codecs (path, content subtype,
// grpc-timeout, -bin metadata, reserved keys). LPM framing, trailers status,
// and Call/Session state machines arrive in Task 3.4+.
package grpc

import (
	"context"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// Framing implements framing.Framing for gRPC × HTTP/2.
// Reuse is always Concurrent (multiplexed streams on one connection).
type Framing struct{}

// New returns a gRPC Framing. Reuse() is always framing.Concurrent.
func New() framing.Framing {
	return &Framing{}
}

// Reuse implements framing.Framing.
func (f *Framing) Reuse() framing.ReuseModel { return framing.Concurrent }

// NewClientSession implements framing.Framing. No handshake I/O; Call/Session
// state machines land in Task 3.4.
func (f *Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	_ = ctx
	_ = c
	_ = spec
	return nil, status.Error(status.Unimplemented, "framing/grpc: Call/Session not implemented (Task 3.4)")
}

// NewServerSession implements framing.Framing. No handshake I/O; Call/Session
// state machines land in Task 3.4.
func (f *Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	_ = ctx
	_ = c
	_ = spec
	return nil, status.Error(status.Unimplemented, "framing/grpc: Call/Session not implemented (Task 3.4)")
}

var _ framing.Framing = (*Framing)(nil)
