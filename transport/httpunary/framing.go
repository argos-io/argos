// Package httpunary implements unary HTTP framing over StreamConn / CarrierConn
// (typically transport/http1 on the server; client OpenStream also works with http2).
//
// Reuse is always Concurrent. Only descriptor.Unary is supported. SendHeaders is
// unsupported on HTTP/1 (UnaryResponseWriter commits at Finish).
//
// Routing is configured on Transport (default RPC paths; optional REST via WithREST).
package httpunary

import (
	"context"
	"fmt"

	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/transport"
)

type framing struct {
	router      Router
	contentType func(codecName string) string
	encodeError func(err error) (contentType string, body []byte)
}

func newRPC() session.Framing {
	return framing{router: rpcRouter{}}
}

func newREST(c RESTConfig) (session.Framing, error) {
	r, err := newRESTRouter(c.Bindings)
	if err != nil {
		return nil, err
	}
	f := framing{router: r}
	if c.ContentType != nil {
		f.contentType = c.ContentType
	}
	if c.EncodeError != nil {
		f.encodeError = c.EncodeError
	}
	return f, nil
}

// Reuse implements session.Framing.
func (framing) Reuse() session.ReuseModel { return session.Concurrent }

func (f framing) pickContentType() func(string) string {
	if f.contentType != nil {
		return f.contentType
	}
	return ContentType
}

// NewClientSession implements session.Framing. No handshake I/O.
func (f framing) NewClientSession(ctx context.Context, c transport.Conn, spec session.SessionSpec) (session.ClientSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	sc, ok := c.(transport.StreamConn)
	if !ok {
		return nil, fmt.Errorf("httpunary: client requires StreamConn")
	}
	return &clientSession{
		conn:        sc,
		cfg:         spec.Options,
		codec:       spec.CodecName,
		router:      f.router,
		contentType: f.pickContentType(),
		encodeError: f.encodeError,
		reusable:    true,
	}, nil
}

// NewServerSession implements session.Framing. No handshake I/O.
func (f framing) NewServerSession(ctx context.Context, c transport.Conn, spec session.SessionSpec) (session.ServerSession, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	cc, ok := c.(transport.CarrierConn)
	if !ok {
		return nil, fmt.Errorf("httpunary: server requires CarrierConn")
	}
	return &serverSession{
		conn:        cc,
		carrier:     cc.Carrier(),
		cfg:         spec.Options,
		codec:       spec.CodecName,
		router:      f.router,
		contentType: f.pickContentType(),
		encodeError: f.encodeError,
	}, nil
}

var (
	_ session.Framing       = framing{}
	_ session.ClientSession = (*clientSession)(nil)
	_ session.ServerSession = (*serverSession)(nil)
	_ session.Call          = (*call)(nil)
	_ session.ServerCall    = (*serverCall)(nil)
)
