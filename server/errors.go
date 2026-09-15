package server

import "errors"

// Connection-context cancel causes (§2.1). Handlers and observers use
// context.Cause to distinguish why a connection ctx ended.
var (
	// ErrPeerGone means the peer disconnected or the connection broke.
	ErrPeerGone = errors.New("server: peer gone")
	// ErrServerShutdown means Server.Shutdown interrupted the connection
	// after its drain deadline (or Server.Close forced teardown).
	ErrServerShutdown = errors.New("server: shutdown")
	// ErrSessionExpired means MaxInboundConnAge (or equivalent lifetime)
	// elapsed for this inbound connection.
	ErrSessionExpired = errors.New("server: session expired")
)
