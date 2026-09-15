// Package argos holds instance-level Config, Option, and BindingFunc.
//
// There is no process-level Configure slot or protocol registry: each
// client.New / server.New receives an explicit *Config snapshot from New.
// BindingFunc constructs a fresh Transport×Framing×Codec triple per
// Client/Binding; it must not Dial or Serve.
//
// Connection-dimension defaults (MaxIdleSessions, SessionIdleTimeout,
// MaxSessionLifetime, MaxInboundConns, MaxInboundConnIdle, MaxInboundConnAge)
// are confirmed by task 7.5 load evidence in example/resp/LOAD.md (§6.1).
package argos
