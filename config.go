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

// Disabled turns off an optional limit. Zero already means "use the built-in
// default" so that a Config literal only has to name what it changes, which
// leaves no way to ask for "keep no idle sessions" by writing 0 — hence this
// sentinel. Only MaxIdleSessions, SessionIdleTimeout and MaxSessionLifetime
// have an off state; every other field rejects it.
const Disabled = -1

// Config holds the tunables of a Client or Server. It is a plain struct: write
// the fields that matter and leave the rest zero, and each constructor fills
// the zeros with the built-in defaults from Defaults.
//
// Side ownership (§6): client-only and server-only fields coexist on one
// Config so a single value can configure both sides. Validation checks field
// values only; it never rejects a mixed Config.
//
// client.New / server.New copy the Config they are given, so a running Client
// or Server never observes a later write. Writing to a Config that another
// goroutine is passing to a constructor is still a data race: tune it during
// start-up, before anything is built.
type Config struct {
	// Call-dimension limits.
	MaxFrameSize    int64
	MaxMessageSize  int64
	MaxMetadataSize int64
	// MaxInboundMetadataSize bounds metadata a peer may send us. It is
	// deliberately independent of MaxMetadataSize, which only constrains our own
	// outbound metadata: enforcing the outbound value on receive would reject
	// peers (grpc-go defaults to 16 MiB) that our own sender would never exceed.
	MaxInboundMetadataSize int64
	MaxHeaderBytes         int64
	ReadAheadMessages      int
	MaxConcurrentCalls     int
	MaxBufferedBytes       int64
	OpenTimeout            time.Duration

	// Connection-dimension limits.
	HandshakeTimeout       time.Duration
	MaxDrainBytes          int64
	ConnReadBufferSize     int64
	MaxSessionsPerEndpoint int
	MaxIdleSessions        int           // client-only; Disabled keeps none
	SessionIdleTimeout     time.Duration // client-only; Disabled turns it off
	MaxSessionLifetime     time.Duration // client-only; Disabled turns it off
	MaxInboundConns        int           // server-only
	MaxInboundConnIdle     time.Duration // server-only; no off state
	MaxInboundConnAge      time.Duration // server-only; no off state

	// HTTPReadHeaderTimeout and HTTPIdleTimeout are the HTTP-level limits for
	// the HTTP-based transports (ws, http1, http2). They apply before onConn
	// runs, so they are what actually bounds a peer that connects and sends
	// nothing — MaxInboundConns and MaxInboundConnIdle only see connections
	// that completed a request or upgrade.
	HTTPReadHeaderTimeout time.Duration // server-only; no off state
	HTTPIdleTimeout       time.Duration // server-only; no off state

	// Binding is the fallback factory for services without their own. A
	// Services entry wins over it, and WithBinding wins over both.
	// Nil is allowed until a Client path requires it.
	Binding BindingFunc

	// ListenAddress is the server-only bind address passed to Transport.Serve
	// as transport.WithListenAddress (e.g. "127.0.0.1:0"). Empty is ignored;
	// concrete transports that require a listen address fail Serve themselves.
	ListenAddress string

	// Filters are server-side Filter chain entries (outermost first).
	Filters []filter.Filter
	// OpenFilters are client-side OpenFilter chain entries (outermost first).
	OpenFilters []filter.OpenFilter
	// Services holds per-service Binding/Target entries keyed by IDL full name.
	// A Client selects one of them with WithServiceName.
	Services map[string]ServiceConfig

	// CallErrorObserver receives per-call local transport errors (§7.5).
	CallErrorObserver func(CallInfo, error)
	// ConnErrorObserver receives connection-level errors that belong to no
	// call (§7.5). Reporting must not make Transport.Serve return (§3.1-22);
	// that contract is enforced by server, not here.
	ConnErrorObserver func(ConnInfo, error)

	// serviceName, targetOverride and bindingOverride hold what
	// WithServiceName / WithTarget / WithBinding selected at the call site.
	// They are not exported fields because a Config shared by many services
	// has no single service name, and because the call site has to be able to
	// win over a Services entry that the shared Config already carries.
	serviceName     string
	targetOverride  string
	bindingOverride BindingFunc
}

// defaultConfig is the process-wide default. It is mutable on purpose: a
// program tunes it once during start-up and every later client.New /
// server.New that does not name a Config of its own starts from it.
var defaultConfig = Defaults()

// DefaultConfig returns the process-wide default Config. The pointer is the
// live default, not a copy: write to its fields to change what an
// unconfigured client.New / server.New will use.
//
//	argos.DefaultConfig().MaxMessageSize = 8 << 20
//
// Tune it before constructing anything. Constructors copy it, so a change
// never reaches a Client or Server that already exists.
func DefaultConfig() *Config { return &defaultConfig }

// Defaults returns the built-in defaults, unaffected by any write to the
// process default. It is the starting point for a Config that must not
// inherit process-wide tuning.
func Defaults() Config {
	return Config{
		MaxFrameSize:           4 * miB,
		MaxMessageSize:         4 * miB,
		MaxMetadataSize:        256 * kiB,
		MaxInboundMetadataSize: 4 * miB,
		MaxHeaderBytes:         1 * miB,
		ReadAheadMessages:      1,
		MaxConcurrentCalls:     64,
		MaxBufferedBytes:       1 * giB,
		OpenTimeout:            10 * time.Second,
		HandshakeTimeout:       10 * time.Second,
		MaxDrainBytes:          1 * miB,
		ConnReadBufferSize:     64 * kiB,
		MaxSessionsPerEndpoint: 64,
		// Confirmed §6.1 defaults (task 7.5; evidence in example/resp/LOAD.md).
		MaxIdleSessions:    8,
		SessionIdleTimeout: 50 * time.Second,
		MaxSessionLifetime: 30 * time.Minute,
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
// WithBinding belong to the Client that named them, not to the configuration.
// Without this, a Config taken from one Client and reused as another's
// WithConfig base would silently make the second Client open calls for the
// first one's service, and the missing-service-name guard would never fire.
func (c *Config) Clone() *Config {
	if c == nil {
		out := Defaults()
		return &out
	}
	out := *c
	out.serviceName = ""
	out.targetOverride = ""
	out.bindingOverride = nil
	if c.Filters != nil {
		out.Filters = append([]filter.Filter(nil), c.Filters...)
	}
	if c.OpenFilters != nil {
		out.OpenFilters = append([]filter.OpenFilter(nil), c.OpenFilters...)
	}
	if c.Services != nil {
		out.Services = make(map[string]ServiceConfig, len(c.Services))
		for k, v := range c.Services {
			out.Services[k] = v
		}
	}
	return &out
}

// ClientConfig builds the Config for one Client: it starts from the Config
// named by WithConfig, or from the process default, layers opts over a copy of
// it, fills zero fields with the built-in defaults, and validates the result.
//
// WithConfig is order-independent — it names the base wherever it appears in
// the list, and every other option applies on top of it. Naming more than one
// base is allowed; the last non-nil one wins.
func ClientConfig(opts ...ClientOption) (*Config, error) {
	base := DefaultConfig()
	for _, o := range opts {
		if o == nil {
			return nil, fmt.Errorf("argos: nil ClientOption")
		}
		if b, ok := o.(configBase); ok && b.baseConfig() != nil {
			base = b.baseConfig()
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

// ServerConfig builds the Config for one Server or one of its bindings. It is
// ClientConfig for the server side; see there for how WithConfig is resolved.
func ServerConfig(opts ...ServerOption) (*Config, error) {
	base := DefaultConfig()
	for _, o := range opts {
		if o == nil {
			return nil, fmt.Errorf("argos: nil ServerOption")
		}
		if b, ok := o.(configBase); ok && b.baseConfig() != nil {
			base = b.baseConfig()
		}
	}
	cfg := base.Clone()
	for _, o := range opts {
		o.applyServer(cfg)
	}
	return finish(cfg)
}

func finish(cfg *Config) (*Config, error) {
	cfg.fillDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// SelectedService reports the service this Config opens calls for and the
// Binding and Target that apply to it. Later layers win:
//
//	WithBinding / WithTarget  >  Services[name]  >  Config.Binding
func (c *Config) SelectedService() (string, ServiceConfig) {
	if c == nil {
		return "", ServiceConfig{}
	}
	sel := ServiceConfig{Binding: c.Binding}
	if sc, ok := c.Services[c.serviceName]; ok {
		if sc.Binding != nil {
			sel.Binding = sc.Binding
		}
		if sc.Target != "" {
			sel.Target = sc.Target
		}
	}
	if c.bindingOverride != nil {
		sel.Binding = c.bindingOverride
	}
	if c.targetOverride != "" {
		sel.Target = c.targetOverride
	}
	return c.serviceName, sel
}

// fillDefaults replaces every zero field with its built-in default and turns
// Disabled into the internal off value. A field that has no off state keeps
// Disabled so validate can name it.
func (c *Config) fillDefaults() {
	d := Defaults()

	c.MaxFrameSize = orDefault(c.MaxFrameSize, d.MaxFrameSize)
	c.MaxMessageSize = orDefault(c.MaxMessageSize, d.MaxMessageSize)
	c.MaxMetadataSize = orDefault(c.MaxMetadataSize, d.MaxMetadataSize)
	c.MaxInboundMetadataSize = orDefault(c.MaxInboundMetadataSize, d.MaxInboundMetadataSize)
	c.MaxHeaderBytes = orDefault(c.MaxHeaderBytes, d.MaxHeaderBytes)
	c.ReadAheadMessages = orDefault(c.ReadAheadMessages, d.ReadAheadMessages)
	c.MaxConcurrentCalls = orDefault(c.MaxConcurrentCalls, d.MaxConcurrentCalls)
	c.MaxBufferedBytes = orDefault(c.MaxBufferedBytes, d.MaxBufferedBytes)
	c.OpenTimeout = orDefault(c.OpenTimeout, d.OpenTimeout)

	c.HandshakeTimeout = orDefault(c.HandshakeTimeout, d.HandshakeTimeout)
	c.MaxDrainBytes = orDefault(c.MaxDrainBytes, d.MaxDrainBytes)
	c.ConnReadBufferSize = orDefault(c.ConnReadBufferSize, d.ConnReadBufferSize)
	c.MaxSessionsPerEndpoint = orDefault(c.MaxSessionsPerEndpoint, d.MaxSessionsPerEndpoint)
	c.MaxInboundConns = orDefault(c.MaxInboundConns, d.MaxInboundConns)
	c.MaxInboundConnIdle = orDefault(c.MaxInboundConnIdle, d.MaxInboundConnIdle)
	c.MaxInboundConnAge = orDefault(c.MaxInboundConnAge, d.MaxInboundConnAge)
	c.HTTPReadHeaderTimeout = orDefault(c.HTTPReadHeaderTimeout, d.HTTPReadHeaderTimeout)
	c.HTTPIdleTimeout = orDefault(c.HTTPIdleTimeout, d.HTTPIdleTimeout)

	c.MaxIdleSessions = orOff(c.MaxIdleSessions, d.MaxIdleSessions)
	c.SessionIdleTimeout = orOff(c.SessionIdleTimeout, d.SessionIdleTimeout)
	c.MaxSessionLifetime = orOff(c.MaxSessionLifetime, d.MaxSessionLifetime)
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

func orOff[T tunable](v, def T) T {
	switch v {
	case 0:
		return def
	case Disabled:
		return 0
	}
	return v
}

// PerCall returns the admission reservation size (§2.6):
//
//	MaxFrameSize + (ReadAheadMessages+1)×MaxMessageSize + MaxMessageSize
func (c *Config) PerCall() (int64, error) {
	if c == nil {
		return 0, fmt.Errorf("argos: nil Config")
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

func (c *Config) validate() error {
	// Disabled on a field with no off state would otherwise be reported as
	// "must be > 0", which does not say what the caller actually did wrong.
	for _, f := range []struct {
		name string
		val  int64
	}{
		{"MaxFrameSize", c.MaxFrameSize},
		{"MaxMessageSize", c.MaxMessageSize},
		{"MaxMetadataSize", c.MaxMetadataSize},
		{"MaxInboundMetadataSize", c.MaxInboundMetadataSize},
		{"MaxHeaderBytes", c.MaxHeaderBytes},
		{"ReadAheadMessages", int64(c.ReadAheadMessages)},
		{"MaxConcurrentCalls", int64(c.MaxConcurrentCalls)},
		{"MaxBufferedBytes", c.MaxBufferedBytes},
		{"OpenTimeout", int64(c.OpenTimeout)},
		{"HandshakeTimeout", int64(c.HandshakeTimeout)},
		{"MaxDrainBytes", c.MaxDrainBytes},
		{"ConnReadBufferSize", c.ConnReadBufferSize},
		{"MaxSessionsPerEndpoint", int64(c.MaxSessionsPerEndpoint)},
		{"MaxInboundConns", int64(c.MaxInboundConns)},
		{"MaxInboundConnIdle", int64(c.MaxInboundConnIdle)},
		{"MaxInboundConnAge", int64(c.MaxInboundConnAge)},
		{"HTTPReadHeaderTimeout", int64(c.HTTPReadHeaderTimeout)},
		{"HTTPIdleTimeout", int64(c.HTTPIdleTimeout)},
	} {
		if f.val == Disabled {
			return fmt.Errorf("argos: %s has no off state; Disabled is not allowed", f.name)
		}
	}

	if c.MaxFrameSize <= 0 {
		return fmt.Errorf("argos: MaxFrameSize must be > 0")
	}
	if c.MaxMessageSize <= 0 {
		return fmt.Errorf("argos: MaxMessageSize must be > 0")
	}
	if c.MaxMetadataSize <= 0 {
		return fmt.Errorf("argos: MaxMetadataSize must be > 0")
	}
	if c.MaxInboundMetadataSize <= 0 {
		return fmt.Errorf("argos: MaxInboundMetadataSize must be > 0")
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
	if c.OpenTimeout <= 0 {
		return fmt.Errorf("argos: OpenTimeout must be > 0")
	}
	if c.HandshakeTimeout <= 0 {
		return fmt.Errorf("argos: HandshakeTimeout must be > 0")
	}
	if c.MaxDrainBytes <= 0 {
		return fmt.Errorf("argos: MaxDrainBytes must be > 0")
	}
	if c.ConnReadBufferSize <= 0 {
		return fmt.Errorf("argos: ConnReadBufferSize must be > 0")
	}
	if c.MaxSessionsPerEndpoint < 1 {
		return fmt.Errorf("argos: MaxSessionsPerEndpoint must be > 0")
	}
	if c.MaxIdleSessions < 0 {
		return fmt.Errorf("argos: MaxIdleSessions must be >= 0 (use argos.Disabled to keep none)")
	}
	if c.SessionIdleTimeout < 0 {
		return fmt.Errorf("argos: SessionIdleTimeout must be >= 0 (use argos.Disabled to turn it off)")
	}
	if c.MaxSessionLifetime < 0 {
		return fmt.Errorf("argos: MaxSessionLifetime must be >= 0 (use argos.Disabled to turn it off)")
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
