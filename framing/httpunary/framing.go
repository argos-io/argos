// Package httpunary implements unary HTTP framing over StreamConn / CarrierConn
// (typically transport/http1 on the server; client OpenStream also works with http2).
//
// Reuse is always Concurrent. Only descriptor.Unary is supported. SendHeaders is
// unsupported on HTTP/1 (UnaryResponseWriter commits at Finish).
//
// Two routing modes share the same Call implementation:
//   - RPC path (New / NewRPC): POST /{Service}/{Method}, same shape as gRPC :path.
//   - REST (NewREST): configurable HTTP verb and path templates per method.
package httpunary

import (
	"context"
	"fmt"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// Framing implements framing.Framing for HTTP unary exchanges.
type Framing struct {
	router      Router
	contentType func(codecName string) string
	encodeError func(err error) (contentType string, body []byte)
}

// New returns RPC-path HTTP unary framing (same as NewRPC).
func New() framing.Framing { return NewRPC() }

// NewRPC returns the default RPC-style path router.
func NewRPC() framing.Framing {
	return Framing{router: rpcRouter{}}
}

// NewREST returns Framing that routes by HTTP method and path template.
func NewREST(c RESTConfig) (framing.Framing, error) {
	r, err := newRESTRouter(c.Bindings)
	if err != nil {
		return nil, err
	}
	f := Framing{router: r}
	if c.ContentType != nil {
		f.contentType = c.ContentType
	}
	if c.EncodeError != nil {
		f.encodeError = c.EncodeError
	}
	return f, nil
}

// Reuse implements framing.Framing.
func (Framing) Reuse() framing.ReuseModel { return framing.Concurrent }

func (f Framing) pickContentType() func(string) string {
	if f.contentType != nil {
		return f.contentType
	}
	return ContentType
}

// NewClientSession implements framing.Framing. No handshake I/O.
func (f Framing) NewClientSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ClientSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	sc, ok := c.(transport.StreamConn)
	if !ok {
		return nil, fmt.Errorf("framing/httpunary: client requires StreamConn")
	}
	return &clientSession{
		conn:        sc,
		cfg:         spec.Config,
		codec:       spec.CodecName,
		router:      f.router,
		contentType: f.pickContentType(),
		encodeError: f.encodeError,
		reusable:    true,
	}, nil
}

// NewServerSession implements framing.Framing. No handshake I/O.
func (f Framing) NewServerSession(ctx context.Context, c transport.Conn, spec framing.SessionSpec) (framing.ServerSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("framing/httpunary: server requires CarrierConn")
	}
	return &serverSession{
		conn:        cc,
		carrier:     cc.Carrier(),
		cfg:         spec.Config,
		codec:       spec.CodecName,
		router:      f.router,
		contentType: f.pickContentType(),
		encodeError: f.encodeError,
	}, nil
}

var (
	_ framing.Framing       = Framing{}
	_ framing.ClientSession = (*clientSession)(nil)
	_ framing.ServerSession = (*serverSession)(nil)
	_ framing.Call          = (*call)(nil)
	_ framing.ServerCall    = (*serverCall)(nil)
)
