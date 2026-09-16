package argos_test

import (
	"context"
	"errors"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
)

func TestWithFilterAndOpenFilterStored(t *testing.T) {
	t.Parallel()
	// One per side: Filter is server-side, OpenFilter client-side, and the
	// option types now say so.
	server, err := argos.ServerConfig(argos.WithConfig(&argos.Config{}), argos.WithFilter(noopFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Filters) != 1 || len(server.OpenFilters) != 0 {
		t.Fatalf("server: Filters=%d OpenFilters=%d", len(server.Filters), len(server.OpenFilters))
	}
	client, err := argos.ClientConfig(argos.WithConfig(&argos.Config{}), argos.WithOpenFilter(noopOpenFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(client.OpenFilters) != 1 || len(client.Filters) != 0 {
		t.Fatalf("client: Filters=%d OpenFilters=%d", len(client.Filters), len(client.OpenFilters))
	}
}

func TestWithServiceStoresBindingAndTarget(t *testing.T) {
	t.Parallel()
	cfg, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithService("echo.v1.EchoService",
			argos.ServiceBinding(markerBinding(1)),
			argos.ServiceTarget("ip://127.0.0.1:7001")),
		argos.WithService("other.Svc",
			argos.ServiceTarget("ip://127.0.0.1:7002")),
	)
	if err != nil {
		t.Fatal(err)
	}
	sc, ok := cfg.Services["echo.v1.EchoService"]
	if !ok || sc.Binding == nil || sc.Target != "ip://127.0.0.1:7001" {
		t.Fatalf("echo service: %+v ok=%v", sc, ok)
	}
	other, ok := cfg.Services["other.Svc"]
	if !ok || other.Target != "ip://127.0.0.1:7002" || other.Binding != nil {
		t.Fatalf("other service: %+v ok=%v", other, ok)
	}
}

// TestWithServiceMergesIntoExistingEntry: a generated stub may name the Binding
// and the caller only the Target, so the second WithService must not wipe the
// half the first one filled in.
func TestWithServiceMergesIntoExistingEntry(t *testing.T) {
	t.Parallel()
	cfg, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{
			Services: map[string]argos.ServiceConfig{
				"echo.v1.EchoService": {Binding: markerBinding(1)},
			},
		}),
		argos.WithService("echo.v1.EchoService", argos.ServiceTarget("ip://127.0.0.1:7001")),
	)
	if err != nil {
		t.Fatal(err)
	}
	sc := cfg.Services["echo.v1.EchoService"]
	if sc.Target != "ip://127.0.0.1:7001" {
		t.Errorf("Target = %q", sc.Target)
	}
	if got := bindingID(t, sc.Binding); got != 1 {
		t.Errorf("Binding id = %d, want the entry already in the base", got)
	}
}

// TestWithConfigOrderIndependent: WithConfig names the base, it does not reset
// the fields listed before it, so a caller can put it wherever it reads best.
func TestWithConfigOrderIndependent(t *testing.T) {
	t.Parallel()
	base := &argos.Config{MaxFrameSize: 2 * miB, MaxSessionsPerEndpoint: 7}
	const msgSize = 1 * miB

	first, err := argos.ClientConfig(argos.WithConfig(base), argos.WithMaxMessageSize(msgSize))
	if err != nil {
		t.Fatal(err)
	}
	last, err := argos.ClientConfig(argos.WithMaxMessageSize(msgSize), argos.WithConfig(base))
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

	srvFirst, err := argos.ServerConfig(argos.WithConfig(base), argos.WithMaxMessageSize(msgSize))
	if err != nil {
		t.Fatal(err)
	}
	srvLast, err := argos.ServerConfig(argos.WithMaxMessageSize(msgSize), argos.WithConfig(base))
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *srvFirst, *srvLast)
}

func TestWithConfigNilUsesProcessDefault(t *testing.T) {
	t.Parallel()
	got, err := argos.ClientConfig(argos.WithConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	want, err := argos.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *got, *want)
}

// TestConstructorsCopyTheBaseConfig replaces the old Config.With test: the base
// must survive both an appending Option and a later write to the returned copy.
func TestConstructorsCopyTheBaseConfig(t *testing.T) {
	t.Parallel()
	base := &argos.Config{
		MaxIdleSessions: 3,
		Filters:         []filter.Filter{noopFilter()},
		OpenFilters:     []filter.OpenFilter{noopOpenFilter()},
	}

	server, err := argos.ServerConfig(argos.WithConfig(base), argos.WithFilter(noopFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Filters) != 2 {
		t.Fatalf("server Filters = %d, want 2", len(server.Filters))
	}

	client, err := argos.ClientConfig(
		argos.WithConfig(base),
		argos.WithOpenFilter(noopOpenFilter()),
		argos.WithMaxIdleSessions(argos.Disabled),
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
	if _, err := argos.ClientConfig(argos.WithMaxMessageSize(1*miB), nil); err == nil {
		t.Error("ClientConfig(nil): want error")
	}
	if _, err := argos.ServerConfig(argos.WithMaxMessageSize(1*miB), nil); err == nil {
		t.Error("ServerConfig(nil): want error")
	}
}

func TestSelectedServiceLayering(t *testing.T) {
	t.Parallel()
	const service = "echo.v1.EchoService"
	withFallback := argos.WithConfig(&argos.Config{Binding: markerBinding(1)})
	entry := argos.WithService(service,
		argos.ServiceBinding(markerBinding(2)),
		argos.ServiceTarget("ip://127.0.0.1:7001"))

	for _, tc := range []struct {
		name       string
		opts       []argos.ClientOption
		wantName   string
		wantID     int
		wantTarget string
	}{
		{
			name:     "config binding is the floor",
			opts:     []argos.ClientOption{withFallback, argos.WithServiceName(service)},
			wantName: service,
			wantID:   1,
		},
		{
			name:       "services entry beats config binding",
			opts:       []argos.ClientOption{withFallback, argos.WithServiceName(service), entry},
			wantName:   service,
			wantID:     2,
			wantTarget: "ip://127.0.0.1:7001",
		},
		{
			name: "call site beats the services entry",
			opts: []argos.ClientOption{
				withFallback, argos.WithServiceName(service), entry,
				argos.WithBinding(markerBinding(3)),
				argos.WithTarget("ip://127.0.0.1:9999"),
			},
			wantName:   service,
			wantID:     3,
			wantTarget: "ip://127.0.0.1:9999",
		},
		{
			// No WithServiceName selects no entry at all, so the entry's Target
			// stays out of the way instead of leaking into an unnamed Client.
			name:     "no service name selects nothing",
			opts:     []argos.ClientOption{withFallback, entry},
			wantName: "",
			wantID:   1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := argos.ClientConfig(tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			name, sel := cfg.SelectedService()
			if name != tc.wantName {
				t.Errorf("service name = %q, want %q", name, tc.wantName)
			}
			if got := bindingID(t, sel.Binding); got != tc.wantID {
				t.Errorf("Binding id = %d, want %d", got, tc.wantID)
			}
			if sel.Target != tc.wantTarget {
				t.Errorf("Target = %q, want %q", sel.Target, tc.wantTarget)
			}
		})
	}
}

// A Config that one Client already built from carries that Client's call-site
// selection. Reusing it as another Client's base used to hand over the service
// name, target and Binding too, so the second Client silently opened calls for
// the first one's service and the missing-service-name guard never fired.
func TestSelectionNotInheritedThroughWithConfig(t *testing.T) {
	t.Parallel()
	first, err := argos.ClientConfig(
		argos.WithServiceName("echo.v1.EchoService"),
		argos.WithTarget("ip://127.0.0.1:7001"),
		argos.WithBinding(markerBinding(1)),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := argos.ClientConfig(argos.WithConfig(first))
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
	if sel.Binding != nil {
		t.Errorf("Binding id %d inherited from the other Client's call site", bindingID(t, sel.Binding))
	}
}

// TestOptionSideTyping is the runtime shadow of a compile-time guarantee:
// ClientConfig takes ClientOption and WithListenAddress only implements
// ServerOption, so
//
//	argos.ClientConfig(argos.WithListenAddress(":0"))
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
		{"WithListenAddress", argos.WithListenAddress(":0"), false, true},
		{"WithServiceName", argos.WithServiceName("echo.v1.EchoService"), true, false},
		{"WithMaxMessageSize", argos.WithMaxMessageSize(1 * miB), true, true},
	} {
		if _, ok := tc.opt.(argos.ClientOption); ok != tc.client {
			t.Errorf("%s satisfies ClientOption = %v, want %v", tc.name, ok, tc.client)
		}
		if _, ok := tc.opt.(argos.ServerOption); ok != tc.server {
			t.Errorf("%s satisfies ServerOption = %v, want %v", tc.name, ok, tc.server)
		}
	}
}

func TestCallErrorObserverUnknownPhaseAndRecover(t *testing.T) {
	t.Parallel()
	var got argos.CallInfo
	var gotErr error
	cfg, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithCallErrorObserver(func(info argos.CallInfo, e error) {
			got = info
			gotErr = e
		}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CallErrorObserver == nil {
		t.Fatal("CallErrorObserver nil")
	}

	info := argos.CallInfo{
		Service:   "svc",
		Method:    "m",
		Peer:      "peer",
		SessionID: "s1",
		Phase:     argos.Phase(99), // unknown; must still invoke
	}
	want := errors.New("boom")
	argos.NotifyCallError(cfg, info, want)
	if got.Phase != argos.Phase(99) || got.SessionID != "s1" || !errors.Is(gotErr, want) {
		t.Fatalf("got=%+v err=%v", got, gotErr)
	}

	panicCfg, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithCallErrorObserver(func(argos.CallInfo, error) {
			panic("observer panic")
		}))
	if err != nil {
		t.Fatal(err)
	}
	argos.NotifyCallError(panicCfg, argos.CallInfo{Phase: argos.PhaseOpen}, want) // must not panic

	// "Nil clears the observer" reads two ways now that the field is public and
	// a base Config can carry one: pin the clearing reading.
	cleared, err := argos.ClientConfig(argos.WithConfig(cfg), argos.WithCallErrorObserver(nil))
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
	var got argos.ConnInfo
	cfg, err := argos.ServerConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithConnErrorObserver(func(info argos.ConnInfo, e error) {
			n++
			got = info
		}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnErrorObserver == nil {
		t.Fatal("ConnErrorObserver nil")
	}

	info := argos.ConnInfo{
		Side:      argos.SideServer,
		Binding:   "b1",
		Peer:      "p",
		SessionID: "s1",
		Phase:     argos.ConnPhaseHandshake,
	}
	argos.NotifyConnError(cfg, info, errors.New("hs"))
	if n != 1 || got.Phase != argos.ConnPhaseHandshake || got.SessionID != "s1" {
		t.Fatalf("n=%d got=%+v", n, got)
	}

	// Unknown ConnPhase still notifies.
	argos.NotifyConnError(cfg, argos.ConnInfo{Phase: argos.ConnPhase(77)}, errors.New("x"))
	if n != 2 {
		t.Fatalf("unknown phase: n=%d", n)
	}

	panicCfg, err := argos.ServerConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithConnErrorObserver(func(argos.ConnInfo, error) {
			panic("conn observer panic")
		}))
	if err != nil {
		t.Fatal(err)
	}
	argos.NotifyConnError(panicCfg, argos.ConnInfo{Phase: argos.ConnPhaseDial}, errors.New("d"))
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

// markerBinding returns a BindingFunc identifiable by the id it stamps on what
// it builds: BindingFunc values themselves cannot be compared, so layering
// tests have to call the winner to find out which one it was.
func markerBinding(id int) argos.BindingFunc {
	return func() (argos.Binding, error) {
		return argos.Binding{Codec: &stubCodec{id: id}}, nil
	}
}

func bindingID(t *testing.T, fn argos.BindingFunc) int {
	t.Helper()
	if fn == nil {
		t.Fatal("no BindingFunc selected")
	}
	b, err := fn()
	if err != nil {
		t.Fatalf("BindingFunc: %v", err)
	}
	c, ok := b.Codec.(*stubCodec)
	if !ok {
		t.Fatalf("Codec = %T, want *stubCodec", b.Codec)
	}
	return c.id
}
