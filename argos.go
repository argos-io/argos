// Package argos holds Options, the Option sets that layer over it, and helpers
// for wiring a protocol as Transport × Codec (session/framing lives inside the
// transport axis).
//
// Options is a plain struct with a process-wide default behind DefaultOptions.
// Per-service settings (transport and codec names, client target, server listen
// address) live in Options.Services. Application code registers
// implementations with server.Register; generated stubs call client.New with
// WithServiceName.
//
// Configuration is code only — no file format, no reload.
//
// Composition-layer limits default to zero (no cap). Session and pool limits
// live on each transport axis; their defaults are zero as well.
package argos
