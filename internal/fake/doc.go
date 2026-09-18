// Package fake provides test doubles for transport.Conn / Carrier,
// session.Framing, and transport.Transport. It is for tests and later internal
// packages only — production packages must not import it.
//
// Doubles cover byte-stream, message, datagram, and HTTP-like carriers, plus a
// minimal length-prefixed Framing that supports all three ReuseModels, optional
// handshake, Close instrumentation, Read reentry detection, and scripted
// OpenCall / AcceptCall errors for session-pool fallback tests.
package fake
