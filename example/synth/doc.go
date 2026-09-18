// Package synth is a synthetic transport.Transport used as an assembly-gate shape (§1.1-4).
//
// It deliberately combines properties that real protocols rarely pack together:
//
//   - greeting-first handshake inside ServerConn.Handshake (HandshakeTimeout)
//   - Sequential reuse over a byte-stream CarrierConn (tcp)
//   - no metadata channel (SendHeaders → Unimplemented)
//   - method name from a custom on-wire field (not an HTTP path)
//   - connection-level state via ConnState + FromContext (7.4)
//   - exclusive mode: Reusable() false; one bidi call, many exchanges (7.4)
//
// This package is a test asset. Core packages must not import it.
package synth
