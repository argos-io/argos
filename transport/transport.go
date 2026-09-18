// Package transport defines Conn and Carrier narrow interfaces.
//
// Transport produces Conn only; it does not import descriptor, framing, or
// codec. It neither recognizes methods or call shapes, nor decides how many
// calls a connection should carry.
package transport

import (
	"context"
	"io"
	"time"
)

// Header is a single opaque name/value pair. Transport does not interpret
// names or values (gRPC -bin, reserved keys, and percent-encoding are handled
// outside this package).
type Header struct {
	Name, Value string
}

// Headers is an opaque sequence of headers.
type Headers []Header

// RequestPreface is produced by Framing before opening a stream; Transport
// does not interpret its contents. Dial only establishes a connection;
// preface is submitted with OpenStream.
type RequestPreface struct {
	RequestTarget string  // HTTP :path; empty for other transports
	Method        string  // HTTP method; empty means POST on OpenStream
	Headers       Headers // already encoded by Framing
}

// DialSpec is constructed by the composition layer; Endpoint comes from a
// Resolver. It remains a struct so fields can be added without changing
// signatures.
type DialSpec struct {
	Endpoint string
}

// ServerOption configures Transport.Serve.
// Concrete options are defined in this package; the apply method is
// unexported so the set is sealed.
type ServerOption interface {
	applyServer(*serveConfig)
}

// ClientOption configures Transport.Dial.
// Concrete options are defined in this package; the apply method is
// unexported so the set is sealed.
type ClientOption interface {
	applyClient(*dialConfig)
}

type serveConfig struct {
	listenAddress string

	httpReadHeaderTimeout time.Duration
	httpIdleTimeout       time.Duration
}

type dialConfig struct{}

type serverOptionFunc func(*serveConfig)

func (f serverOptionFunc) applyServer(c *serveConfig) { f(c) }

// WithListenAddress sets the address Transport.Serve listens on
// (host:port, e.g. "127.0.0.1:0"). Concrete transports read it via
// ApplyServerOptions.
func WithListenAddress(addr string) ServerOption {
	return serverOptionFunc(func(c *serveConfig) { c.listenAddress = addr })
}

// WithHTTPTimeouts sets the HTTP-level timeouts for the HTTP-based transports
// (ws, http1, http2). They bound connections that never reach onConn: a peer
// that connects and sends nothing (or half a request line) otherwise holds a
// goroutine and an fd forever, because the composition layer's connection
// admission and idle timers only start once onConn is called.
func WithHTTPTimeouts(readHeader, idle time.Duration) ServerOption {
	return serverOptionFunc(func(c *serveConfig) {
		c.httpReadHeaderTimeout = readHeader
		c.httpIdleTimeout = idle
	})
}

// ServerSettings is the resolved view of ServerOption values for concrete
// transport implementations.
type ServerSettings struct {
	ListenAddress         string
	HTTPReadHeaderTimeout time.Duration
	HTTPIdleTimeout       time.Duration
}

// ApplyServerOptions applies sealed ServerOption values and returns the
// resulting settings. Concrete transports (e.g. transport/tcp) call this
// inside Serve.
func ApplyServerOptions(opts ...ServerOption) ServerSettings {
	var c serveConfig
	for _, o := range opts {
		if o != nil {
			o.applyServer(&c)
		}
	}
	return ServerSettings{
		ListenAddress:         c.listenAddress,
		HTTPReadHeaderTimeout: c.httpReadHeaderTimeout,
		HTTPIdleTimeout:       c.httpIdleTimeout,
	}
}

// Transport produces Conn only. It does not import descriptor, framing, or
// codec.
type Transport interface {
	Serve(ctx context.Context,
		onConn func(context.Context, Conn),
		opts ...ServerOption) error
	Dial(ctx context.Context, spec DialSpec,
		opts ...ClientOption) (Conn, error)
	// Shutdown stops accepting new connections and waits for existing onConn
	// callbacks to finish; on deadline it interrupts unfinished connections.
	Shutdown(ctx context.Context) error
	// Close is idempotent; it interrupts current connections and releases the
	// listener, connections, internal pools, and background work.
	Close() error
}

// Conn expresses only the lifecycle of one connection. After a successful
// Session create, the Session takes ownership of it.
type Conn interface {
	// Close is idempotent; it closes this connection and unblocks all waiters on it.
	Close() error
}

// CarrierConn is a connection that itself carries one exchange at a time:
// tcp, ws, udp, and sequential-reuse protocols such as RESP. Only one Call
// holds it at a time; Session enforces that.
type CarrierConn interface {
	Conn
	Carrier() Carrier
}

// StreamConn can open multiple independent exchange streams: http2, and
// http1 where net/http pools connections internally—for the latter a Conn is
// an endpoint handle and OpenStream starts one request.
type StreamConn interface {
	Conn
	OpenStream(ctx context.Context, p RequestPreface) (Carrier, error)
}

// Carrier expresses one exchange. It does not own the connection and has no
// Close: the unit of closing is Conn, owned by Session.
type Carrier interface {
	// Abort is idempotent; it interrupts in-flight I/O waits on this exchange.
	//
	// Only the framing.Call that holds it or the Session that created it may
	// call Abort. The client/server composition layer, Filters, and application
	// code must not: to cancel a call use Call.Close; to tear down a connection
	// use Session.Close. Session is the sole creator of Carrier and therefore
	// always knows whether Abort has occurred; the interface needs no Aborted().
	//
	// On multiplexed carriers it affects only this stream. On sequential-reuse
	// carriers (CarrierConn) there is no safe mid-exchange interrupt—the only
	// workable implementation sets read/write deadlines to the past, which
	// permanently poisons the connection—so Abort necessarily makes
	// Session.Reusable() false, not merely "usually".
	Abort() error
}

// ByteStreamCarrier is a byte-stream exchange (tcp, http1 body, http2 body).
// Byte streams and message sequences are distinct capabilities and must not
// share a single io.Writer contract.
type ByteStreamCarrier interface {
	Carrier
	io.Reader
	io.Writer
}

// MessageCarrier is a sequence of independent messages (e.g. WebSocket).
// RecvMessage/SendMessage are deliberately named so a concrete type may
// implement several narrow carrier interfaces without method collisions.
type MessageCarrier interface {
	Carrier
	RecvMessage() ([]byte, error)
	SendMessage([]byte) error
}

// DatagramCarrier is one full datagram per call (e.g. UDP).
type DatagramCarrier interface {
	Carrier
	RecvDatagram() ([]byte, error)
	SendDatagram([]byte) error
}

// SendCloser ends the send direction: TCP FIN or HTTP request-body END_STREAM.
type SendCloser interface {
	Carrier
	CloseSend() error
}

// RequestHeaderReader exposes inbound request headers (http1/http2 server).
// Headers are opaque to Transport.
type RequestHeaderReader interface {
	Carrier
	RequestTarget() string
	RequestMethod() string
	RequestHeaders() Headers
}

// ResponseHeaderReader exposes inbound response headers (http1/http2 client).
type ResponseHeaderReader interface {
	Carrier
	ResponseStatus() (int, error)
	ResponseHeaders() (Headers, error)
}

// ResponseTrailerReader exposes inbound trailers (http2 client).
type ResponseTrailerReader interface {
	Carrier
	ResponseTrailers() (Headers, error)
}

// ResponseWriter merges header and trailer writing for http2 servers.
// Splitting them would create two entry points for submitting initial headers.
type ResponseWriter interface {
	Carrier
	// WriteHeaders submits and flushes initial headers; the peer need not wait
	// for body/Finish to read them.
	WriteHeaders(status int, headers Headers) error
	// Finish must be able to complete a trailers-only response with a single
	// HEADERS+END_STREAM when initial headers have not been written.
	Finish(status int, initial, trailers Headers) error
}

// UnaryResponseWriter submits a full response at once (http1 server), avoiding
// an early 200 on the wire.
type UnaryResponseWriter interface {
	Carrier
	WriteResponse(status int, headers Headers, body []byte) error
}

// SendError describes a transport-level send failure only; it does not map to
// any RPC status. It is conservative: ReceiveOpen returns false only when the
// receive direction of this exchange is known to be unrecoverable; when that
// cannot be decided immediately it must return true and let a subsequent Recv
// produce a definitive result. Either path yields correct final behavior.
type SendError interface {
	error
	ReceiveOpen() bool
}
