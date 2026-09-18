package argos

import (
	"fmt"
	"math"
	"time"

	"github.com/argos-io/argos/filter"
)

const (
	kiB = 1024
	miB = 1024 * kiB
	giB = 1024 * miB
)

// Options holds the tunables of a Client or Server. It is a plain struct: write
// the fields that matter and leave the rest zero, and each constructor fills
// the zeros with the built-in defaults from Defaults.
//
// Side ownership (§6): client-only and server-only fields coexist on one
// Options so a single value can configure both sides. Validation checks field
// values only; it never rejects a mixed Options.
//
// client.New / server.New copy the Options they are given, so a running Client
// or Server never observes a later write. Writing to a Options that another
// goroutine is passing to a constructor is still a data race: tune it during
// start-up, before anything is built.
type Options struct {
	// Call-dimension limits.
	//
	// MaxFrameSize, MaxMessageSize and ReadAheadMessages are the inputs to the
	// per-call admission reservation (see PerCall) — the composition layer's own
	// byte budget, not an enforcement knob: what a call actually may buffer is
	// decided by the axis the service is bound to. They stay on Options because
	// the budget is instance-wide while limits are per-axis, so there is no
	// single axis to read them from.
	MaxFrameSize      int64
	MaxMessageSize    int64
	ReadAheadMessages int

	MaxHeaderBytes     int64
	MaxConcurrentCalls int
	MaxBufferedBytes   int64

	// Connection-dimension limits.
	HandshakeTimeout   time.Duration
	ConnReadBufferSize int64
	MaxInboundConns    int           // server-only
	MaxInboundConnIdle time.Duration // server-only; no off state
	MaxInboundConnAge  time.Duration // server-only; no off state

	// HTTPReadHeaderTimeout and HTTPIdleTimeout are the HTTP-level limits for
	// the HTTP-based transports (ws, http1, http2). They apply before onConn
	// runs, so they are what actually bounds a peer that connects and sends
	// nothing — MaxInboundConns and MaxInboundConnIdle only see connections
	// that completed a request or upgrade.
	HTTPReadHeaderTimeout time.Duration // server-only; no off state
	HTTPIdleTimeout       time.Duration // server-only; no off state

	// ListenAddress is the server-only bind address passed to Transport.Serve
	// as transport.WithListenAddress (e.g. "127.0.0.1:0"). Empty is ignored;
	// concrete transports that require a listen address fail Serve themselves.
	ListenAddress string

	// Filters are server-side Filter chain entries (outermost first).
	Filters []filter.Filter
	// OpenFilters are client-side OpenFilter chain entries (outermost first).
	OpenFilters []filter.OpenFilter
	// Services holds per-service protocol, client target, and server listen
	// settings keyed by IDL full name. Clients select one with WithServiceName;
	// server.Run starts listeners declared for each registered service.
	Services map[string]ServiceOptions

	// CallErrorObserver receives per-call local transport errors (§7.5).
	CallErrorObserver func(CallInfo, error)
	// ConnErrorObserver receives connection-level errors that belong to no
	// call (§7.5). Reporting must not make Transport.Serve return (§3.1-22);
	// that contract is enforced by server, not here.
	ConnErrorObserver func(ConnInfo, error)

	// serviceName, targetOverride and axis overrides hold call-site choices for
	// one Client. They are not exported: a shared Options has no single service
	// name, and Clone must not inherit another Client's selection.
	serviceName          string
	targetOverride       string
	overrideTransport    string
	overrideCodec        string
	hasTransportOverride bool
	hasCodecOverride     bool
}

// defaultOptions is the process-wide default. It is mutable on purpose: a
// program tunes it once during start-up and every later client.New /
// server.New that does not name an Options of its own starts from it.
var defaultOptions = Defaults()

// DefaultOptions returns the process-wide default Options. The pointer is the
// live default, not a copy: write to its fields to change what an
// unconfigured client.New / server.New will use.
//
//	argos.DefaultOptions().MaxMessageSize = 8 << 20
//
// Tune it before constructing anything. Constructors copy it, so a change
// never reaches a Client or Server that already exists.
func DefaultOptions() *Options { return &defaultOptions }

// Defaults returns the built-in defaults, unaffected by any write to the
// process default. It is the starting point for an Options that must not
// inherit process-wide tuning.
func Defaults() Options {
	return Options{
		MaxFrameSize:       4 * miB,
		MaxMessageSize:     4 * miB,
		ReadAheadMessages:  1,
		MaxHeaderBytes:     1 * miB,
		MaxConcurrentCalls: 64,
		MaxBufferedBytes:   1 * giB,
		HandshakeTimeout:   10 * time.Second,
		ConnReadBufferSize: 64 * kiB,
		MaxInboundConns:    1024,
		MaxInboundConnIdle: 50 * time.Second,
		MaxInboundConnAge:  30 * time.Minute,

		HTTPReadHeaderTimeout: 10 * time.Second,
		HTTPIdleTimeout:       50 * time.Second,
	}
}

// Clone returns a copy of c whose slices and Services map do not alias c's, so
// an Option appending a Filter to the copy cannot reach the original.
// A nil receiver clones the built-in defaults.
//
// The copy carries no service selection: WithServiceName, WithTarget and
// per-axis overrides belong to the Client that named them, not to the configuration.
// Without this, an Options taken from one Client and reused as another's
// WithClientOptions base would silently make the second Client open calls for the
// first one's service, and the missing-service-name guard would never fire.
func (c *Options) Clone() *Options {
	if c == nil {
		out := Defaults()
		return &out
	}
	out := *c
	out.serviceName = ""
	out.targetOverride = ""
	out.overrideTransport = ""
	out.overrideCodec = ""
	out.hasTransportOverride = false
	out.hasCodecOverride = false
	if c.Filters != nil {
		out.Filters = append([]filter.Filter(nil), c.Filters...)
	}
	if c.OpenFilters != nil {
		out.OpenFilters = append([]filter.OpenFilter(nil), c.OpenFilters...)
	}
	if c.Services != nil {
		out.Services = make(map[string]ServiceOptions, len(c.Services))
		for k, v := range c.Services {
			out.Services[k] = cloneServiceOptions(v)
		}
	}
	return &out
}

// ClientOptions builds the Options for one Client: it starts from the Options
// named by WithClientOptions, or from the process default, layers opts over a copy of
// it, fills zero fields with the built-in defaults, and validates the result.
//
// WithClientOptions is order-independent — it names the base wherever it appears in
// the list, and every other option applies on top of it. Naming more than one
// base is allowed; the last non-nil one wins.
func ClientOptions(opts ...ClientOption) (*Options, error) {
	base := DefaultOptions()
	for _, o := range opts {
		if o == nil {
			return nil, fmt.Errorf("argos: nil ClientOption")
		}
		if b, ok := o.(optionsBase); ok && b.baseOptions() != nil {
			base = b.baseOptions()
		}
	}
	cfg := base.Clone()
	// Every option applies, including the one that named the base: skipping
	// them by type would silently drop the fields of a future option that both
	// names a base and sets something.
	for _, o := range opts {
		o.applyClient(cfg)
	}
	return finish(cfg)
}

// ServerOptions builds the Options for one Server or one of its bindings. It is
// ClientOptions for the server side; see there for how WithClientOptions is resolved.
func ServerOptions(opts ...ServerOption) (*Options, error) {
	base := DefaultOptions()
	for _, o := range opts {
		if o == nil {
			return nil, fmt.Errorf("argos: nil ServerOption")
		}
		if b, ok := o.(optionsBase); ok && b.baseOptions() != nil {
			base = b.baseOptions()
		}
	}
	cfg := base.Clone()
	for _, o := range opts {
		o.applyServer(cfg)
	}
	return finish(cfg)
}

func finish(cfg *Options) (*Options, error) {
	cfg.fillDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// SelectedService reports the service this Client opens calls for and the
// merged ServiceOptions. Per-axis call-site overrides win over the Services
// entry; WithTarget wins over ServiceOptions.Target.
func (c *Options) SelectedService() (string, ServiceOptions) {
	if c == nil {
		return "", ServiceOptions{}
	}
	sel := ServiceOptions{}
	if sc, ok := c.Services[c.serviceName]; ok {
		sel = sc
	}
	if c.hasTransportOverride {
		sel.Transport = c.overrideTransport
	}
	if c.hasCodecOverride {
		sel.Codec = c.overrideCodec
	}
	if c.targetOverride != "" {
		sel.Target = c.targetOverride
	}
	return c.serviceName, sel
}

// fillDefaults replaces every zero field with its built-in default.
func (c *Options) fillDefaults() {
	d := Defaults()

	c.MaxFrameSize = orDefault(c.MaxFrameSize, d.MaxFrameSize)
	c.MaxMessageSize = orDefault(c.MaxMessageSize, d.MaxMessageSize)
	c.ReadAheadMessages = orDefault(c.ReadAheadMessages, d.ReadAheadMessages)
	c.MaxHeaderBytes = orDefault(c.MaxHeaderBytes, d.MaxHeaderBytes)
	c.MaxConcurrentCalls = orDefault(c.MaxConcurrentCalls, d.MaxConcurrentCalls)
	c.MaxBufferedBytes = orDefault(c.MaxBufferedBytes, d.MaxBufferedBytes)

	c.HandshakeTimeout = orDefault(c.HandshakeTimeout, d.HandshakeTimeout)
	c.ConnReadBufferSize = orDefault(c.ConnReadBufferSize, d.ConnReadBufferSize)
	c.MaxInboundConns = orDefault(c.MaxInboundConns, d.MaxInboundConns)
	c.MaxInboundConnIdle = orDefault(c.MaxInboundConnIdle, d.MaxInboundConnIdle)
	c.MaxInboundConnAge = orDefault(c.MaxInboundConnAge, d.MaxInboundConnAge)
	c.HTTPReadHeaderTimeout = orDefault(c.HTTPReadHeaderTimeout, d.HTTPReadHeaderTimeout)
	c.HTTPIdleTimeout = orDefault(c.HTTPIdleTimeout, d.HTTPIdleTimeout)
}

type tunable interface {
	int | int64 | time.Duration
}

func orDefault[T tunable](v, def T) T {
	if v == 0 {
		return def
	}
	return v
}

// PerCall returns the admission reservation size (§2.6):
//
//	MaxFrameSize + (ReadAheadMessages+1)×MaxMessageSize + MaxMessageSize
func (c *Options) PerCall() (int64, error) {
	if c == nil {
		return 0, fmt.Errorf("argos: nil Options")
	}
	return perCall(c.MaxFrameSize, c.MaxMessageSize, c.ReadAheadMessages)
}

func perCall(maxFrame, maxMsg int64, readAhead int) (int64, error) {
	if readAhead < 1 {
		return 0, fmt.Errorf("argos: ReadAheadMessages must be >= 1")
	}
	// (ReadAheadMessages+1)*MaxMessageSize
	aheadPlusOne := int64(readAhead) + 1
	part, ok := mulInt64(aheadPlusOne, maxMsg)
	if !ok {
		return 0, fmt.Errorf("argos: perCall overflow (ReadAheadMessages, MaxMessageSize)")
	}
	sum, ok := addInt64(maxFrame, part)
	if !ok {
		return 0, fmt.Errorf("argos: perCall overflow (MaxFrameSize, MaxMessageSize, ReadAheadMessages)")
	}
	sum, ok = addInt64(sum, maxMsg)
	if !ok {
		return 0, fmt.Errorf("argos: perCall overflow (MaxFrameSize, MaxMessageSize, ReadAheadMessages)")
	}
	return sum, nil
}

func (c *Options) validate() error {
	for _, f := range []struct {
		name string
		val  int64
	}{
		{"MaxFrameSize", c.MaxFrameSize},
		{"MaxMessageSize", c.MaxMessageSize},
		{"ReadAheadMessages", int64(c.ReadAheadMessages)},
		{"MaxHeaderBytes", c.MaxHeaderBytes},
		{"MaxConcurrentCalls", int64(c.MaxConcurrentCalls)},
		{"MaxBufferedBytes", c.MaxBufferedBytes},
		{"HandshakeTimeout", int64(c.HandshakeTimeout)},
		{"ConnReadBufferSize", c.ConnReadBufferSize},
		{"MaxInboundConns", int64(c.MaxInboundConns)},
		{"MaxInboundConnIdle", int64(c.MaxInboundConnIdle)},
		{"MaxInboundConnAge", int64(c.MaxInboundConnAge)},
		{"HTTPReadHeaderTimeout", int64(c.HTTPReadHeaderTimeout)},
		{"HTTPIdleTimeout", int64(c.HTTPIdleTimeout)},
	} {
		if f.val < 0 {
			return fmt.Errorf("argos: %s must not be negative", f.name)
		}
	}

	if c.MaxFrameSize <= 0 {
		return fmt.Errorf("argos: MaxFrameSize must be > 0")
	}
	if c.MaxMessageSize <= 0 {
		return fmt.Errorf("argos: MaxMessageSize must be > 0")
	}
	if c.MaxHeaderBytes <= 0 {
		return fmt.Errorf("argos: MaxHeaderBytes must be > 0")
	}
	if c.ReadAheadMessages < 1 {
		return fmt.Errorf("argos: ReadAheadMessages must be >= 1")
	}
	if c.MaxConcurrentCalls < 1 {
		return fmt.Errorf("argos: MaxConcurrentCalls must be >= 1")
	}
	if c.MaxBufferedBytes <= 0 {
		return fmt.Errorf("argos: MaxBufferedBytes must be > 0")
	}
	if c.HandshakeTimeout <= 0 {
		return fmt.Errorf("argos: HandshakeTimeout must be > 0")
	}
	if c.ConnReadBufferSize <= 0 {
		return fmt.Errorf("argos: ConnReadBufferSize must be > 0")
	}
	if c.MaxInboundConns < 1 {
		return fmt.Errorf("argos: MaxInboundConns must be > 0")
	}
	if c.MaxInboundConnIdle <= 0 {
		return fmt.Errorf("argos: MaxInboundConnIdle must be > 0")
	}
	if c.MaxInboundConnAge <= 0 {
		return fmt.Errorf("argos: MaxInboundConnAge must be > 0")
	}
	if c.HTTPReadHeaderTimeout <= 0 {
		return fmt.Errorf("argos: HTTPReadHeaderTimeout must be > 0")
	}
	if c.HTTPIdleTimeout <= 0 {
		return fmt.Errorf("argos: HTTPIdleTimeout must be > 0")
	}

	pc, err := perCall(c.MaxFrameSize, c.MaxMessageSize, c.ReadAheadMessages)
	if err != nil {
		return err
	}
	product, ok := mulInt64(int64(c.MaxConcurrentCalls), pc)
	if !ok || product > c.MaxBufferedBytes {
		return fmt.Errorf("argos: MaxConcurrentCalls × perCall > MaxBufferedBytes (MaxConcurrentCalls, MaxFrameSize, MaxMessageSize, ReadAheadMessages, MaxBufferedBytes)")
	}
	return nil
}

func addInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func mulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > 0 {
		if b > 0 {
			if a > math.MaxInt64/b {
				return 0, false
			}
		} else if b < math.MinInt64/a {
			return 0, false
		}
	} else {
		if b > 0 {
			if a < math.MinInt64/b {
				return 0, false
			}
		} else if a != 0 && b < math.MaxInt64/a {
			return 0, false
		}
	}
	return a * b, true
}
