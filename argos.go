// Package argos holds Config, the Option sets that layer over it, and the
// Transport / Framing / Codec factory types that make up a protocol.
//
// Config is a plain struct with a process-wide default behind DefaultConfig.
// Per-service settings (Transport / Framing / Codec, client target, server
// listen address) live in Config.Services. Application code registers
// implementations with server.Register; generated stubs call client.New with
// WithServiceName.
//
// Configuration is code only — no file format, no reload.
//
// Connection-dimension defaults are confirmed by task 7.5 load evidence in
// example/resp/LOAD.md (§6.1).
package argos
