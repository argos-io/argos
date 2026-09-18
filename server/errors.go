package server

import "errors"

// Connection-context cancel causes (§2.1). Handlers and observers use
// context.Cause to distinguish why a connection ctx ended.
var (
	// ErrPeerGone means the peer disconnected or the connection broke.
	ErrPeerGone = errors.New("server: peer gone")
	// ErrServerShutdown is the cause Server.Shutdown cancels Run's context
	// with. Run treats it as a clean stop.
	ErrServerShutdown = errors.New("server: shutdown")
	// ErrSessionExpired means MaxInboundConnAge (or equivalent lifetime)
	// elapsed for this inbound connection.
	ErrSessionExpired = errors.New("server: session expired")
)
