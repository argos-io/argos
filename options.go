package argos

import (
	"fmt"
	"time"

	"github.com/argos-io/argos/filter"
)

const (
	kiB = 1024
	miB = 1024 * kiB
)

// Options holds the tunables of a Client or Server. It is a plain struct: write
// the fields that matter and leave the rest zero. Zero means no cap (or no
// timeout where the field is a duration); constructors do not replace zeros.
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
	// Call-dimension limits (composition layer only).
	MaxHeaderBytes     int64
	MaxConcurrentCalls int

	// Connection-dimension limits.
	HandshakeTimeout   time.Duration
	MaxInboundConns    int           // server-only
	MaxInboundConnIdle time.Duration // server-only; 0 = no idle close between calls
	MaxInboundConnAge  time.Duration // server-only; 0 = no max connection age

	// HTTPReadHeaderTimeout and HTTPIdleTimeout are the HTTP-level limits for
	// the HTTP-based transports (ws, http1, http2). They apply before onConn
	// runs, so they are what actually bounds a peer that connects and sends
	// nothing — MaxInboundConns and MaxInboundConnIdle only see connections
	// that completed a request or upgrade. Zero disables each timeout.
	HTTPReadHeaderTimeout time.Duration // server-only
	HTTPIdleTimeout       time.Duration // server-only

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
//	argos.DefaultOptions().MaxConcurrentCalls = 128
//
// Tune it before constructing anything. Constructors copy it, so a change
// never reaches a Client or Server that already exists.
func DefaultOptions() *Options { return &defaultOptions }

// Defaults returns the built-in defaults, unaffected by any write to the
// process default. It is the starting point for an Options that must not
// inherit process-wide tuning.
//
// Every numeric limit defaults to zero, meaning no cap (or no timeout where
// the field is a duration). Set a field > 0 to enforce it.
func Defaults() Options {
	return Options{}
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

// fillDefaults is a no-op: zero fields stay zero (no cap / no timeout).
func (c *Options) fillDefaults() {}

func (c *Options) validate() error {
	for _, f := range []struct {
		name string
		val  int64
	}{
		{"MaxHeaderBytes", c.MaxHeaderBytes},
		{"MaxConcurrentCalls", int64(c.MaxConcurrentCalls)},
		{"HandshakeTimeout", int64(c.HandshakeTimeout)},
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

	return nil
}
