// Package framing defines Session and Call interfaces for protocol framing.
//
// Framing sits on transport.Conn and produces per-call byte movers. It does
// not import codec (only a codec name string), compressor, stream, client,
// server, or probe. Concrete protocols live in subpackages.
package framing

import (
	"context"
	"errors"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

// Config is an instance-level immutable snapshot passed at assembly time.
// Framing does not read global configuration. Fields apply to every call on
// the session; they are not per-call overrides.
//
// Only protocol-agnostic limits belong here. Compression is a framing/grpc
// construction concern for gRPC framing (§3.1-10); Config
// must not grow compression fields.
type Config struct {
	MaxMessageSize  int64
	MaxFrameSize    int64
	MaxMetadataSize int64
	// MaxInboundMetadataSize bounds metadata received from a peer. Independent of
	// MaxMetadataSize, which is the outbound limit.
	MaxInboundMetadataSize int64
	ReadAheadMessages      int // complete DATA messages allowed ahead per receive direction; must be >= 1 when validated
	// OpenTimeout is the server AcceptCall budget from the first byte of a call
	// through OPEN parse. Zero means the Framing implementation's default.
	OpenTimeout time.Duration
	// MaxDrainBytes caps residual-frame discard between AcceptCall iterations.
	// Zero means the Framing implementation's default.
	MaxDrainBytes int64
}

// SessionSpec is everything the composition layer gives Framing for one
// connection. It has no CallMetadata: metadata is call-scoped.
type SessionSpec struct {
	CodecName string
	Config    Config
}

// CallSpec is everything the composition layer gives a Session for one call.
// CallMetadata is created by the composition layer and also placed on the call
// context, so metadata has one authority and context has one producer.
//
// CodecName and Config are instance-level and already supplied via SessionSpec
// at session create; they are not repeated per call.
type CallSpec struct {
	Metadata metadata.CallMetadata
}

// ReuseModel declares how many in-flight calls a protocol allows on one
// connection. It constrains only the client session pool's borrow/return
// policy; the server expresses reuse via its AcceptCall loop and does not
// read this value.
type ReuseModel uint8

const (
	// OneCallPerConn: one connection carries one call, then closes
	// (e.g. envelope × udp).
	OneCallPerConn ReuseModel = iota
	// Sequential: connection is reusable, but only one in-flight call at a
	// time (e.g. envelope × {tcp, ws}, resp).
	Sequential
	// Concurrent: one connection carries many concurrent calls
	// (e.g. grpc × http2, wholebody × http1).
	Concurrent
)

// Framing is the public extension point. Implement it and satisfy the needed
// Conn/Carrier narrow interfaces to plug in a new protocol; the composition
// layer adds no branches for concrete Framing types (§3.1-12).
//
// A Framing is a strategy and a session factory, never a resource owner: it
// has no Close and nothing ever closes it, so it must not hold anything that
// needs releasing. Resources belong to the Conn it is handed and to the
// Session and Call it builds, which have their own Close (§4.1).
type Framing interface {
	// Reuse returns the reuse model. It must be constant: the same Framing
	// instance always returns the same value. The composition layer reads it
	// once at client.New / Binding start.
	Reuse() ReuseModel
	// NewClientSession completes connection-level handshake and auth (MySQL
	// greeting+auth, Redis HELLO/AUTH, protocol-initiated TLS upgrade). For
	// protocols without handshake (envelope, grpc, wholebody) it only asserts
	// narrow interfaces and performs no I/O.
	// On success the Session owns Conn; on failure the composition layer closes Conn.
	NewClientSession(ctx context.Context, c transport.Conn, spec SessionSpec) (ClientSession, error)
	// NewServerSession is the server-side counterpart of NewClientSession.
	NewServerSession(ctx context.Context, c transport.Conn, spec SessionSpec) (ServerSession, error)
}

// ClientSession is the client protocol instance for one connection.
type ClientSession interface {
	// OpenCall opens one call on this session. HTTP :path and per-call headers
	// are Framing-internal details passed to StreamConn.OpenStream.
	//
	// Capacity is tracked by the session pool from Reuse() (§4.1). On the
	// normal path borrow never overshoots, so ErrSessionBusy / ErrSessionSpent
	// are fallbacks only: they appear when the pool's view briefly disagrees
	// with session reality (e.g. peer lowered SETTINGS_MAX_CONCURRENT_STREAMS).
	// The pool switches sessions; retries are capped by MaxSessionsPerEndpoint.
	// Neither sentinel is exposed to Open's caller.
	OpenCall(ctx context.Context, m descriptor.Method, spec CallSpec) (Call, error)
	// Reusable reports whether this session can still carry a new call.
	// It returns false in exactly these five cases (all required):
	//  1. connection damaged (I/O error, protocol parse failure, or Carrier.Abort);
	//  2. MaxSessionLifetime exceeded;
	//  3. protocol entered exclusive state (e.g. Redis SUBSCRIBE, open MySQL txn);
	//  4. previous call closed without reading protocol terminal state
	//     (carrier hygiene — residual frames may remain; see §2.4);
	//  5. idle watchdog saw peer close (§2.6).
	// Reusable only reads state; it performs no I/O and may run concurrent with
	// in-flight calls. The session pool reads it once before borrow and once on
	// return, both under the pool lock, mutually exclusive with idle reclaim.
	Reusable() bool
	// Close is idempotent; it closes this session and its Conn and unblocks
	// waiters. For Concurrent sessions with in-flight calls the pool does not
	// call Close — real teardown waits until the refcount hits zero (§4.6).
	Close() error
}

// ServerSession is the server protocol instance for one connection.
type ServerSession interface {
	// AcceptCall takes the next call on this connection.
	//
	// ctx is the composition layer's accept ctx (§2.1), not the connection
	// ctx: canceling it only stops accepting; in-flight calls are unaffected.
	// Normal end of connection returns io.EOF. OneCallPerConn returns a call
	// once, then io.EOF. This is the sole accept entry point; Framing must not
	// start a second accept loop (§3.1-17).
	//
	// spec carries the CallMetadata pre-built by the composition layer for the
	// "next" call; inbound metadata must be written into it, never a privately
	// created handle. Each loop iteration builds a fresh one (including the
	// iteration that returns io.EOF).
	//
	// Implementer obligations for sequential reuse:
	//
	//  (a) Residual drain before parse. A prior call may have ended after an
	//      early handler return while the peer still sends DATA/END. AcceptCall
	//      must boundedly discard frames belonging to the finished call before
	//      parsing the next OPEN; exceeding MaxDrainBytes is a connection-level
	//      error and the server closes the connection.
	//
	//  (b) OpenTimeout from the first byte. Block indefinitely (accept ctx only)
	//      until the first byte of this call arrives — otherwise keep-alive
	//      connections are killed; after the first byte, start OpenTimeout and
	//      return a connection-level error on expiry. Only Session can tell
	//      "not started" from "started then stalled" (§2.6).
	//
	//  (c) ErrCallRejected continues the loop. An illegal but well-framed call
	//      (unknown resp command, bad OPEN fields with intact length) returns
	//      ErrCallRejected wrapping a status; the composition layer writes that
	//      status to the peer when possible and continues AcceptCall. Any error
	//      other than io.EOF or ErrCallRejected is connection-level and ends the loop.
	AcceptCall(ctx context.Context, spec CallSpec) (ServerCall, error)
	// Close is idempotent; it closes this session and its Conn.
	Close() error
}

// Session-level stable local errors; must be detectable with errors.Is.
// ErrSessionBusy and ErrSessionSpent are consumed only inside
// internal/sessionpool and must never leak to Open's caller. Receiving them at
// an illegal time indicates a Framing bug; the composition layer wraps as
// Internal and reports via the connection observer (§7.5).
var (
	// ErrSessionBusy means the session cannot carry another call right now;
	// the pool should try another session.
	ErrSessionBusy = errors.New("framing: session busy")
	// ErrSessionSpent means a OneCallPerConn session has already been used.
	ErrSessionSpent = errors.New("framing: session spent")
	// ErrCallRejected is AcceptCall-only: this call is illegal but the
	// connection remains usable. Wrap a status.Error; the composition layer
	// writes it to the peer then continues the AcceptCall loop.
	ErrCallRejected = errors.New("framing: call rejected")
)

// Call is one call's framing instance (Session product on a Carrier).
// It moves bytes only; it does not recognize business types, produce context,
// or close Conn/Carrier.
type Call interface {
	// Method returns the canonical full name (dot-separated), same shape as
	// descriptor.Method.FullName. Server discovers it from OPEN / request
	// target; client takes it from descriptor.Method.
	Method() string
	// Deadline returns the inbound deadline (envelope OPEN or grpc-timeout).
	// The composition layer derives the call ctx from it; Call does not
	// produce context.
	Deadline() (time.Time, bool)
	// SendHeaders is responder-only; it submits current initial metadata
	// (empty headers allowed). wholebody/http1, envelope/udp, and the
	// initiator return a stable status.Unimplemented error.
	SendHeaders() error
	// Recv returns payload and a non-nil idempotent release on success;
	// the caller must release after consuming bytes. On failure both are nil.
	// Close does not reclaim still-borrowed payloads. Normal receive end is io.EOF.
	Recv() (payload []byte, release func(), err error)
	// Send borrows payload only until return; if the Call must retain bytes it
	// copies and bills separately. Success means Framing fully accepted the
	// message; whether it hits the wire immediately is protocol-defined.
	// If send is finished but receive remains open, return a transport.SendError
	// with ReceiveOpen() true; stream.Wrap maps that to stream.ErrSendClosed.
	// framing does not import stream.
	Send(payload []byte) error
	// HalfClose is initiator-only; idempotent. Failure semantics match Send.
	HalfClose() error
	// Finish is responder-only; exactly once per call.
	Finish(err error) error
	// Close is idempotent; it unblocks in-flight Recv/Send and returns the
	// carrier to the Session. It does not close Conn or Carrier — otherwise a
	// Sequential protocol would tear down a reusable connection after the
	// first call. Whether the connection closes is Session's decision from Reuse().
	//
	// Synchronous join: when Close returns, this call's receive goroutine must
	// have exited and must no longer hold any reference to the Carrier.
	// "Unblock" is wake semantics, not "already exited" — under sequential reuse
	// the carrier is handed to the next call immediately; a woken-but-not-exited
	// reader races the next call on the same socket (frame tear; -race may miss it).
	//
	// Buffer ownership: release only call-private buffers (decode queue, send
	// scratch). The Session owns the cross-call carrier read buffer — TCP has no
	// message boundaries, so one Read may contain this call's STATUS tail and the
	// next call's OPEN head; discarding that here drops the next call's start.
	//
	// Carrier hygiene: if this call closes without reading protocol terminal
	// state (no STATUS / no io.EOF), Session.Reusable() must become false (§2.4).
	Close() error
}

// ServerCall extends Call with Accept. Only the server needs Accept: it parses
// method from the wire, routes, then obtains descriptor.Method. The client's
// OpenCall already holds Carrier and Method together.
type ServerCall interface {
	Call
	// Accept runs after routing and before Filter/handler. It performs no
	// business dispatch and no I/O; it only checks Shape and carrier
	// capability. It does not check method name — that matches by construction.
	Accept(m descriptor.Method) error
}
