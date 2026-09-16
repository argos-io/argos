// Package argos holds Config, the Option sets that layer over it, and
// BindingFunc.
//
// Config is a plain struct with a process-wide default behind DefaultConfig:
// a program tunes it during start-up and every client.New / server.New that
// names no Config of its own starts from it. A call site that needs something
// else passes options — WithConfig to name a different base, or any of the
// With* options to change single fields. Zero fields are filled with the
// built-in defaults, so a Config literal only names what it changes.
//
// There is no protocol registry: BindingFunc constructs a fresh
// Transport×Framing×Codec triple per Client/Binding; it must not Dial or
// Serve. Configuration is code only — no file format, no reload.
//
// Connection-dimension defaults (MaxIdleSessions, SessionIdleTimeout,
// MaxSessionLifetime, MaxInboundConns, MaxInboundConnIdle, MaxInboundConnAge)
// are confirmed by task 7.5 load evidence in example/resp/LOAD.md (§6.1).
package argos
