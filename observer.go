package argos

// Phase identifies when a per-call local transport error was observed (§7.5).
// New Phase values are not breaking; observers must tolerate unknown values.
type Phase uint8

const (
	PhaseOpen Phase = iota // open / parse OPEN
	PhaseDispatch          // route and Filter/handler
	PhaseFinish            // write final status
	PhaseCleanup           // Close and resource reclaim
	PhaseLeak              // CallStream GC'd without Close (§4.2)
)

// CallInfo carries diagnostic fields for WithCallErrorObserver.
type CallInfo struct {
	Service   string
	Method    string
	Peer      string
	SessionID string // owning session; correlates calls on one bad connection
	Phase     Phase
}

// Side identifies which endpoint reported a connection-level error.
type Side uint8

const (
	SideClient Side = iota
	SideServer
)

// ConnPhase identifies when a connection-level error was observed (§7.5).
// New ConnPhase values are not breaking; observers must tolerate unknown values.
type ConnPhase uint8

const (
	ConnPhaseDial ConnPhase = iota
	ConnPhaseHandshake
	ConnPhaseAccept
	ConnPhaseAdmit
	ConnPhaseIdleReclaim
	ConnPhaseClose
)

// ConnInfo carries diagnostic fields for WithConnErrorObserver.
type ConnInfo struct {
	Side      Side
	Binding   string // server: which Binding; client: empty
	Endpoint  string // client: Resolver endpoint; server: empty
	Peer      string
	SessionID string // correlates with CallInfo.SessionID
	Phase     ConnPhase
}

// CallErrorObserver returns the configured observer, or nil.
func (c *Config) CallErrorObserver() func(CallInfo, error) {
	if c == nil {
		return nil
	}
	return c.callErrorObserver
}

// ConnErrorObserver returns the configured observer, or nil.
func (c *Config) ConnErrorObserver() func(ConnInfo, error) {
	if c == nil {
		return nil
	}
	return c.connErrorObserver
}

// NotifyCallError invokes the call-error observer if set.
// Panics in the observer are recovered so per-call errors never escape.
func NotifyCallError(cfg *Config, info CallInfo, err error) {
	if cfg == nil {
		return
	}
	fn := cfg.callErrorObserver
	if fn == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(info, err)
}

// NotifyConnError invokes the connection-error observer if set.
// Panics in the observer are recovered.
//
// Contract (§3.1-22 / §7.5): each connection-level error that belongs to no
// call is reported exactly once and must not make Transport.Serve return.
// Enforcement of the Serve contract lives in server; this helper only notifies.
func NotifyConnError(cfg *Config, info ConnInfo, err error) {
	if cfg == nil {
		return
	}
	fn := cfg.connErrorObserver
	if fn == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(info, err)
}
