package transport

import "time"

// Limits are the session-scoped wire limits an axis enforces on every call:
// frame and message caps, metadata caps, read-ahead, the server's open budget,
// and the residual-frame drain cap.
//
// They are fixed when the axis is constructed and this is the only place they
// are set. Nothing installs or overrides them later: one axis may be shared by
// several Clients and a listen surface, so a bind-time write would let whichever
// bound last decide what every other user gets. argos.Options does not carry
// these numbers — the axis is the single source, which is why passing a
// mismatched value is not caught anywhere, it is simply not expressible.
//
// For the size fields zero means "no limit". An axis constructor seeds the
// baseline from the same numbers the internal defaults use, so a bare axis
// already enforces them rather than running unbounded.
type Limits struct {
	MaxMessageSize         int64
	MaxFrameSize           int64
	MaxMetadataSize        int64
	MaxInboundMetadataSize int64
	ReadAheadMessages      int
	OpenTimeout            time.Duration
	MaxDrainBytes          int64
}

// PoolLimits are the client-side connection pool limits an axis enforces: how
// many connections one endpoint may hold, how many may sit idle, and when idle
// and lifetime reclaim close them.
//
// They are fixed at construction for the same reason Limits are. The reuse
// model is not here: how many calls one connection may carry is the protocol's
// own answer and the axis sets it. An axis that does not pool reports the zero
// value, and a zero field where it does pool means it declines to bound that
// dimension.
type PoolLimits struct {
	MaxSessionsPerEndpoint int
	MaxIdleSessions        int
	SessionIdleTimeout     time.Duration
	MaxSessionLifetime     time.Duration
	HandshakeTimeout       time.Duration
}
