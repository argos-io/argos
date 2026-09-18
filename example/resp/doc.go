// Package resp is a milestone-⑦ gate: a minimal RESP2 protocol axis over tcp.
// It owns its transport, handshake, reuse model and connection pool, so a
// binding here is just "axis + codec".
//
// It proves three protocol properties the composition layer already supports:
//
//  1. Sequential reuse — HELLO/AUTH once per connection, then many
//     PING/GET/SET calls on it.
//  2. Connection-level handshake — Redis HELLO 2 (and optional AUTH) run
//     before any business OpenCall.
//  3. Missing capability without a kernel hole — SendHeaders returns
//     status.Unimplemented and subsequent calls on the same connection still
//     work.
//  4. Exclusive long stream — SUBSCRIBE is server-streaming; Reusable() is
//     false for the call lifetime and the connection is closed (not pooled)
//     when the call ends; pushes arrive as subsequent Recv messages.
//
// This package is a test asset, not a production Redis client/server.
package resp
