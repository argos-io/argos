// Package resp is a milestone-⑦ gate: a minimal RESP2 framing over tcp,
// assembled via BindingFunc without changing core public interfaces.
//
// It proves three protocol properties the composition layer already supports:
//
//  1. Sequential reuse — HELLO/AUTH once in NewClientSession, then many
//     PING/GET/SET calls on one connection.
//  2. Connection-level handshake — Redis HELLO 2 (and optional AUTH) run
//     before any business OpenCall.
//  3. Missing capability without a kernel hole — SendHeaders returns
//     status.Unimplemented and subsequent calls on the same session still work.
//  4. Exclusive long stream — SUBSCRIBE is server-streaming; Reusable() is
//     false for the call lifetime and the connection is closed (not pooled)
//     when the call ends; pushes arrive as subsequent Recv messages.
//
// This package is a test asset, not a production Redis client/server.
// Task 7.2b: no transport/, framing/ (root), client/, or server/ public
// interfaces were modified to implement it.
package resp
