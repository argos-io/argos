package argos

import (
	"context"
	"errors"
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
)

func TestWithFilterAndOpenFilterStored(t *testing.T) {
	t.Parallel()
	// One per side: Filter is server-side, OpenFilter client-side, and the
	// option types now say so.
	server, err := ServerConfig(WithConfig(&Config{}), WithFilter(noopFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Filters) != 1 || len(server.OpenFilters) != 0 {
		t.Fatalf("server: Filters=%d OpenFilters=%d", len(server.Filters), len(server.OpenFilters))
	}
	client, err := ClientConfig(WithConfig(&Config{}), WithOpenFilter(noopOpenFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(client.OpenFilters) != 1 || len(client.Filters) != 0 {
		t.Fatalf("client: Filters=%d OpenFilters=%d", len(client.Filters), len(client.OpenFilters))
	}
}

func TestWithServiceStoresProtocolAndTarget(t *testing.T) {
	t.Parallel()
	cfg, err := ClientConfig(
		WithConfig(&Config{}),
		WithService("echo.v1.EchoService",
			markerCodec(1),
			ServiceTarget("ip://127.0.0.1:7001")),
		WithService("other.Svc",
			ServiceTarget("ip://127.0.0.1:7002")),
	)
	if err != nil {
		t.Fatal(err)
	}
	sc, ok := cfg.Services["echo.v1.EchoService"]
	if !ok || sc.Codec == nil || sc.Target != "ip://127.0.0.1:7001" {
		t.Fatalf("echo service: %+v ok=%v", sc, ok)
	}
	other, ok := cfg.Services["other.Svc"]
	if !ok || other.Target != "ip://127.0.0.1:7002" || other.Codec != nil {
		t.Fatalf("other service: %+v ok=%v", other, ok)
	}
}

// TestWithServiceMergesIntoExistingEntry: a generated stub may name the
// protocol and the caller only the Target, so the second WithService must not
// wipe the half the first one filled in.
func TestWithServiceMergesIntoExistingEntry(t *testing.T) {
	t.Parallel()
	cfg, err := ClientConfig(
		WithConfig(&Config{
			Services: map[string]ServiceConfig{
				"echo.v1.EchoService": {Codec: stubCodecFactory(1)},
			},
		}),
		WithService("echo.v1.EchoService", ServiceTarget("ip://127.0.0.1:7001")),
	)
	if err != nil {
		t.Fatal(err)
	}
	sc := cfg.Services["echo.v1.EchoService"]
	if sc.Target != "ip://127.0.0.1:7001" {
		t.Errorf("Target = %q", sc.Target)
	}
	if got := protocolCodecID(t, sc); got != 1 {
		t.Errorf("protocol id = %d, want the entry already in the base", got)
	}
}

// TestWithConfigOrderIndependent: WithConfig names the base, it does not reset
// the fields listed before it, so a caller can put it wherever it reads best.
func TestWithConfigOrderIndependent(t *testing.T) {
	t.Parallel()
	base := &Config{MaxFrameSize: 2 * miB, MaxSessionsPerEndpoint: 7}
	const msgSize = 1 * miB

	first, err := ClientConfig(WithConfig(base), WithMaxMessageSize(msgSize))
	if err != nil {
		t.Fatal(err)
	}
	last, err := ClientConfig(WithMaxMessageSize(msgSize), WithConfig(base))
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *first, *last)

	if first.MaxMessageSize != msgSize {
		t.Errorf("MaxMessageSize = %d, want %d", first.MaxMessageSize, msgSize)
	}
	if first.MaxFrameSize != 2*miB || first.MaxSessionsPerEndpoint != 7 {
		t.Errorf("base not carried through: MaxFrameSize=%d MaxSessionsPerEndpoint=%d",
			first.MaxFrameSize, first.MaxSessionsPerEndpoint)
	}

	srvFirst, err := ServerConfig(WithConfig(base), WithMaxMessageSize(msgSize))
	if err != nil {
		t.Fatal(err)
	}
	srvLast, err := ServerConfig(WithMaxMessageSize(msgSize), WithConfig(base))
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *srvFirst, *srvLast)
}

func TestWithConfigNilUsesProcessDefault(t *testing.T) {
	t.Parallel()
	got, err := ClientConfig(WithConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	want, err := ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *got, *want)
}

// TestConstructorsCopyTheBaseConfig replaces the old Config.With test: the base
// must survive both an appending Option and a later write to the returned copy.
func TestConstructorsCopyTheBaseConfig(t *testing.T) {
	t.Parallel()
	base := &Config{
		MaxIdleSessions: 3,
		Filters:         []filter.Filter{noopFilter()},
		OpenFilters:     []filter.OpenFilter{noopOpenFilter()},
	}

	server, err := ServerConfig(WithConfig(base), WithFilter(noopFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Filters) != 2 {
		t.Fatalf("server Filters = %d, want 2", len(server.Filters))
	}

	client, err := ClientConfig(
		WithConfig(base),
		WithOpenFilter(noopOpenFilter()),
		WithMaxIdleSessions(Disabled),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.OpenFilters) != 2 {
		t.Fatalf("client OpenFilters = %d, want 2", len(client.OpenFilters))
	}

	if len(base.Filters) != 1 || len(base.OpenFilters) != 1 {
		t.Fatalf("an appending Option reached the base: filters=%d openFilters=%d",
			len(base.Filters), len(base.OpenFilters))
	}
	if base.MaxIdleSessions != 3 {
		t.Fatalf("base mutated: MaxIdleSessions=%d", base.MaxIdleSessions)
	}
	if client.MaxIdleSessions != 0 {
		t.Fatalf("client MaxIdleSessions = %d, want 0", client.MaxIdleSessions)
	}

	base.MaxIdleSessions = 99
	if client.MaxIdleSessions != 0 || server.MaxIdleSessions != 3 {
		t.Fatalf("returned Config still tracks the base: client=%d server=%d",
			client.MaxIdleSessions, server.MaxIdleSessions)
	}
}

func TestNilOptionRejected(t *testing.T) {
	t.Parallel()
	if _, err := ClientConfig(WithMaxMessageSize(1*miB), nil); err == nil {
		t.Error("ClientConfig(nil): want error")
	}
	if _, err := ServerConfig(WithMaxMessageSize(1*miB), nil); err == nil {
		t.Error("ServerConfig(nil): want error")
	}
}

func TestSelectedServiceLayering(t *testing.T) {
	t.Parallel()
	const service = "echo.v1.EchoService"
	entry := WithService(service,
		markerCodec(2),
		ServiceTarget("ip://127.0.0.1:7001"))

	for _, tc := range []struct {
		name       string
		opts       []ClientOption
		wantName   string
		wantID     int
		wantTarget string
	}{
		{
			name:       "services entry supplies protocol and target",
			opts:       []ClientOption{WithServiceName(service), entry},
			wantName:   service,
			wantID:     2,
			wantTarget: "ip://127.0.0.1:7001",
		},
		{
			name: "call site beats the services entry",
			opts: []ClientOption{
				WithServiceName(service), entry,
				WithCodec(stubCodecFactory(3)),
				WithTarget("ip://127.0.0.1:9999"),
			},
			wantName:   service,
			wantID:     3,
			wantTarget: "ip://127.0.0.1:9999",
		},
		{
			// No WithServiceName selects no entry at all, so the entry's Target
			// stays out of the way instead of leaking into an unnamed Client.
			name:     "no service name selects nothing",
			opts:     []ClientOption{entry},
			wantName: "",
			wantID:   0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := ClientConfig(tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			name, sel := cfg.SelectedService()
			if name != tc.wantName {
				t.Errorf("service name = %q, want %q", name, tc.wantName)
			}
			if got := protocolCodecID(t, sel); got != tc.wantID {
				t.Errorf("protocol id = %d, want %d", got, tc.wantID)
			}
			if sel.Target != tc.wantTarget {
				t.Errorf("Target = %q, want %q", sel.Target, tc.wantTarget)
			}
		})
	}
}

// A Config that one Client already built from carries that Client's call-site
// selection. Reusing it as another Client's base used to hand over the service
// name, target and protocol too, so the second Client silently opened calls for
// the first one's service and the missing-service-name guard never fired.
func TestSelectionNotInheritedThroughWithConfig(t *testing.T) {
	t.Parallel()
	first, err := ClientConfig(
		WithServiceName("echo.v1.EchoService"),
		WithTarget("ip://127.0.0.1:7001"),
		WithCodec(stubCodecFactory(1)),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ClientConfig(WithConfig(first))
	if err != nil {
		t.Fatal(err)
	}
	name, sel := second.SelectedService()
	if name != "" {
		t.Errorf("service name = %q, want none inherited", name)
	}
	if sel.Target != "" {
		t.Errorf("Target = %q, want none inherited", sel.Target)
	}
	if sel.Codec != nil {
		t.Errorf("protocol id %d inherited from the other Client's call site", protocolCodecID(t, sel))
	}
}

// TestOptionSideTyping is the runtime shadow of a compile-time guarantee:
// ClientConfig takes ClientOption and WithListenAddress only implements
// ServerOption, so
//
//	ClientConfig(WithListenAddress(":0"))
//
// does not build ("ServerOption does not implement ClientOption"). Side misuse
// can no longer be tested by calling the constructor and expecting an error —
// the asserts below check the interface satisfaction that replaces it.
func TestOptionSideTyping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		opt            any
		client, server bool
	}{
		{"WithListenAddress", WithListenAddress(":0"), false, true},
		{"WithServiceName", WithServiceName("echo.v1.EchoService"), true, false},
		{"WithService", WithService("echo.v1.EchoService", ServiceTarget("ip://127.0.0.1:1")), true, true},
		{"WithMaxMessageSize", WithMaxMessageSize(1 * miB), true, true},
	} {
		if _, ok := tc.opt.(ClientOption); ok != tc.client {
			t.Errorf("%s satisfies ClientOption = %v, want %v", tc.name, ok, tc.client)
		}
		if _, ok := tc.opt.(ServerOption); ok != tc.server {
			t.Errorf("%s satisfies ServerOption = %v, want %v", tc.name, ok, tc.server)
		}
	}
}

func TestCallErrorObserverUnknownPhaseAndRecover(t *testing.T) {
	t.Parallel()
	var got CallInfo
	var gotErr error
	cfg, err := ClientConfig(
		WithConfig(&Config{}),
		WithCallErrorObserver(func(info CallInfo, e error) {
			got = info
			gotErr = e
		}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CallErrorObserver == nil {
		t.Fatal("CallErrorObserver nil")
	}

	info := CallInfo{
		Service:   "svc",
		Method:    "m",
		Peer:      "peer",
		SessionID: "s1",
		Phase:     Phase(99), // unknown; must still invoke
	}
	want := errors.New("boom")
	NotifyCallError(cfg, info, want)
	if got.Phase != Phase(99) || got.SessionID != "s1" || !errors.Is(gotErr, want) {
		t.Fatalf("got=%+v err=%v", got, gotErr)
	}

	panicCfg, err := ClientConfig(
		WithConfig(&Config{}),
		WithCallErrorObserver(func(CallInfo, error) {
			panic("observer panic")
		}))
	if err != nil {
		t.Fatal(err)
	}
	NotifyCallError(panicCfg, CallInfo{Phase: PhaseOpen}, want) // must not panic

	// "Nil clears the observer" reads two ways now that the field is public and
	// a base Config can carry one: pin the clearing reading.
	cleared, err := ClientConfig(WithConfig(cfg), WithCallErrorObserver(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cleared.CallErrorObserver != nil {
		t.Error("WithCallErrorObserver(nil) must clear the inherited observer")
	}
}

func TestConnErrorObserverInvokeOnceAndRecover(t *testing.T) {
	t.Parallel()
	var n int
	var got ConnInfo
	cfg, err := ServerConfig(
		WithConfig(&Config{}),
		WithConnErrorObserver(func(info ConnInfo, e error) {
			n++
			got = info
		}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnErrorObserver == nil {
		t.Fatal("ConnErrorObserver nil")
	}

	info := ConnInfo{
		Side:      SideServer,
		Binding:   "b1",
		Peer:      "p",
		SessionID: "s1",
		Phase:     ConnPhaseHandshake,
	}
	NotifyConnError(cfg, info, errors.New("hs"))
	if n != 1 || got.Phase != ConnPhaseHandshake || got.SessionID != "s1" {
		t.Fatalf("n=%d got=%+v", n, got)
	}

	// Unknown ConnPhase still notifies.
	NotifyConnError(cfg, ConnInfo{Phase: ConnPhase(77)}, errors.New("x"))
	if n != 2 {
		t.Fatalf("unknown phase: n=%d", n)
	}

	panicCfg, err := ServerConfig(
		WithConfig(&Config{}),
		WithConnErrorObserver(func(ConnInfo, error) {
			panic("conn observer panic")
		}))
	if err != nil {
		t.Fatal(err)
	}
	NotifyConnError(panicCfg, ConnInfo{Phase: ConnPhaseDial}, errors.New("d"))
}

func noopFilter() filter.Filter {
	return func(ctx context.Context, m descriptor.Method, st stream.Stream, next filter.Handler) error {
		return next(ctx, m, st)
	}
}

func noopOpenFilter() filter.OpenFilter {
	return func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
		return next(ctx, m)
	}
}

func stubCodecFactory(id int) CodecFunc {
	return func() (codec.Codec, error) { return &stubCodec{id: id}, nil }
}

func markerCodec(id int) ServiceOption {
	return ServiceCodec(stubCodecFactory(id))
}

func protocolCodecID(t *testing.T, sc ServiceConfig) int {
	t.Helper()
	if sc.Codec == nil {
		return 0
	}
	cd, err := sc.Codec()
	if err != nil {
		t.Fatalf("Codec factory: %v", err)
	}
	c, ok := cd.(*stubCodec)
	if !ok {
		t.Fatalf("Codec = %T, want *stubCodec", cd)
	}
	return c.id
}
