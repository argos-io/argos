package transport

import (
	"context"
	"errors"
	"time"

	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
)

// Transport is what the composition layer configures alongside Codec: bytes,
// frames, handshake, reuse and connection maintenance. Argos is Transport ×
// Codec; there is no separate framing or link layer in the product model.
//
// Transport owns connectivity. The client never sees a raw Conn: it opens
// calls at an endpoint, and Call.Close returns what Transport borrowed.
// Idle limits, lifetime and liveness belong to Transport because only it can
// read the wire while a connection sits unused.
//
// Transport must not import codec.
//
// Lifetime is the ctx an implementation is constructed with; there is no Close
// on the interface. Serve stops when its ctx is done; one Transport may be
// shared by several Clients because none of them owns it.
type Transport interface {
	// OpenCall opens one call at endpoint, dialing or reusing a connection as
	// the protocol's reuse model allows. Call.Close releases what it borrowed.
	OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec CallSpec) (Call, error)
	// Serve accepts inbound connections until ctx is done, calling onConn once
	// per connection. The connection arrives before handshake: the composition
	// layer owns the handshake timeout and reports handshake failure, so it
	// calls ServerConn.Handshake itself.
	Serve(ctx context.Context, onConn func(context.Context, ServerConn), opts ...ServerOption) error
	// CallConcurrency reports whether one connection may run several handlers
	// at once. It must be constant: the composition layer reads it once when a
	// listen surface starts, and uses it only to decide whether to dispatch
	// handlers on their own goroutines.
	CallConcurrency() Concurrency
	// CodecName reports the codec identity this axis is paired with, which is
	// fixed when it is constructed. It is compared against the Codec's own name
	// so that pairing an axis with a codec it does not carry is an assembly
	// error rather than a body a peer decodes with the wrong codec.
	//
	// It names the codec, not the wire spelling: the gRPC axis reports
	// "protobuf" and derives the "proto" content subtype from it. So this is
	// comparable with codec.Named and needs no alias table for the check.
	//
	// Empty means the protocol does not name a codec on the wire (RESP2 has no
	// such field); there is nothing to compare and the check is skipped.
	CodecName() string
}

// ClientConn is one outbound connection's protocol instance: handshake done,
// ready to carry calls. It is what a Transport pools internally — the
// composition layer never sees it, because connectivity is the Transport's business.
type ClientConn interface {
	// OpenCall opens one call on this connection. On ErrConnBusy or
	// ErrConnSpent the caller tries another connection; neither sentinel may
	// reach application code.
	OpenCall(ctx context.Context, m descriptor.Method, spec CallSpec) (Call, error)
	// Reusable reports whether this connection can still carry a new call. It
	// returns false when the connection is damaged, its lifetime is up, the
	// protocol entered an exclusive state (Redis SUBSCRIBE, open transaction),
	// a previous call ended without reading the protocol's terminal state, or
	// an idle watchdog saw the peer go away.
	//
	// Only the protocol can answer this, which is why pooling belongs to the
	// Transport: reading the wire while a connection is idle means understanding
	// what the bytes are.
	//
	// Reusable only reads state; it performs no I/O and may run concurrent
	// with in-flight calls.
	Reusable() bool
	// Close is idempotent; it closes this connection and unblocks waiters. A
	// pool must not Close a connection that still has calls in flight.
	Close() error
}

// Session-level sentinels. They are consumed by whatever pools connections and
// must never reach the caller of a call.
var (
	// ErrConnBusy means this connection cannot carry another call right now.
	ErrConnBusy = errors.New("transport: connection busy")
	// ErrConnSpent means a OneCallPerConn connection has already been used.
	ErrConnSpent = errors.New("transport: connection spent")
)

// Concurrency is how many in-flight calls one connection allows. Client-side
// borrow/return policy is internal to the Transport; this value exists because the
// server has to decide how to dispatch handlers.
type Concurrency uint8

const (
	// OneCallPerConn: one connection carries one call, then closes.
	OneCallPerConn Concurrency = iota
	// Sequential: connection is reusable, one in-flight call at a time.
	Sequential
	// Concurrent: one connection carries many concurrent calls.
	Concurrent
)

// CallSpec is everything the composition layer gives a Transport for one call.
// CallMetadata is created by the composition layer and also placed on the call
// context, so metadata has one authority and context has one producer.
type CallSpec struct {
	Metadata metadata.CallMetadata
}

// ServerConn is one inbound connection's protocol instance.
type ServerConn interface {
	// Handshake completes connection-level handshake and auth (Redis
	// HELLO/AUTH, MySQL greeting, protocol-initiated TLS upgrade). Protocols
	// without a handshake perform no I/O. It runs once, before the first
	// AcceptCall; on failure the caller closes this ServerConn.
	Handshake(ctx context.Context) error
	// AcceptCall takes the next call on this connection.
	//
	// ctx is the accept ctx, not the connection ctx: canceling it only stops
	// accepting; in-flight calls are unaffected. Normal end of connection
	// returns io.EOF. OneCallPerConn returns a call once, then io.EOF. This is
	// the sole accept entry point; an implementation must not start a second
	// accept loop.
	//
	// spec carries the CallMetadata pre-built for the "next" call; inbound
	// metadata must be written into it, never a privately created handle.
	//
	// Implementer obligations for sequential reuse:
	//
	//  (a) Residual drain before parse. A prior call may have ended after an
	//      early handler return while the peer still sends payload. AcceptCall
	//      must boundedly discard what belonged to the finished call before
	//      parsing the next one; exceeding the drain limit is a connection
	//      level error.
	//
	//  (b) Open timeout from the first byte. Block indefinitely (accept ctx
	//      only) until the first byte of this call arrives — otherwise
	//      keep-alive connections are killed; after the first byte, start the
	//      open timeout and return a connection-level error on expiry. Only
	//      the connection can tell "not started" from "started then stalled".
	//
	//  (c) ErrCallRejected continues the loop. An illegal but well-framed call
	//      returns ErrCallRejected wrapping a status; the composition layer
	//      writes that status to the peer when possible and keeps accepting.
	//      Any error other than io.EOF or ErrCallRejected is connection-level
	//      and ends the loop.
	AcceptCall(ctx context.Context, spec CallSpec) (ServerCall, error)
	// Close is idempotent; it closes this connection. It is also what releases
	// a connection whose Handshake failed.
	Close() error
}

// Call is one call on a connection. It moves bytes only; it does not recognize
// business types, produce context, or close the connection.
type Call interface {
	// Method returns the canonical full name (dot-separated), same shape as
	// descriptor.Method.FullName. A server discovers it from the wire; a
	// client takes it from descriptor.Method.
	Method() string
	// Deadline returns the inbound deadline (e.g. grpc-timeout). The
	// composition layer derives the call ctx from it; Call does not produce
	// context.
	Deadline() (time.Time, bool)
	// SendHeaders is responder-only; it submits current initial metadata
	// (empty headers allowed). Protocols without a header phase and the
	// initiator return a stable status.Unimplemented error.
	SendHeaders() error
	// Recv returns payload and a non-nil idempotent release on success; the
	// caller must release after consuming bytes. On failure both are nil.
	// Close does not reclaim still-borrowed payloads. Normal receive end is
	// io.EOF.
	Recv() (payload []byte, release func(), err error)
	// Send borrows payload only until return; if the Call must retain bytes it
	// copies and bills separately. Success means the Transport fully accepted the
	// message; whether it hits the wire immediately is protocol-defined.
	// If send is finished but receive remains open, return a SendError with
	// ReceiveOpen() true; stream.Wrap maps that to stream.ErrSendClosed.
	Send(payload []byte) error
	// HalfClose is initiator-only; idempotent. Failure semantics match Send.
	HalfClose() error
	// Finish is responder-only; exactly once per call.
	Finish(err error) error
	// Close is idempotent; it unblocks in-flight Recv/Send and gives the
	// connection back to the Transport — on the client side that is what returns it
	// to the Transport's pool. It does not close the connection itself: a
	// Sequential protocol would otherwise tear down a reusable connection
	// after the first call.
	//
	// Synchronous join: when Close returns, this call's receive goroutine must
	// have exited and must no longer touch the connection. "Unblock" is wake
	// semantics, not "already exited" — under sequential reuse the connection
	// is handed to the next call immediately, and a woken-but-not-exited
	// reader races it on the same socket.
	//
	// Buffer ownership: release only call-private buffers. The cross-call read
	// buffer belongs to the connection — a byte stream has no message
	// boundaries, so one Read may hold this call's tail and the next call's
	// head.
	Close() error
}

// ServerCall extends Call with Accept. Only the server needs it: it parses the
// method from the wire, routes, then obtains descriptor.Method. A client's
// OpenCall already holds the connection and the Method together.
type ServerCall interface {
	Call
	// Accept runs after routing and before Filter/handler. It performs no
	// business dispatch and no I/O; it only checks Shape and wire capability.
	// It does not check the method name — that matches by construction.
	Accept(m descriptor.Method) error
}

// BudgetSetter is implemented by Calls that honor per-call byte budgets. The
// composition layer installs the budget after admission (server) or through the
// OpenCall ctx (client).
type BudgetSetter interface {
	Call
	SetBudget(b budget.Budget)
}

// ErrCallRejected is AcceptCall-only: this call is illegal but the connection
// remains usable. Wrap a status error; the composition layer writes it to the
// peer then keeps accepting.
var ErrCallRejected = errors.New("transport: call rejected")
