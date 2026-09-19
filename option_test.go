package argos

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
)

func TestWithFilterAndOpenFilterStored(t *testing.T) {
	t.Parallel()
	server, err := ServerOptions(WithServerOptions(&Options{}), WithFilter(noopFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Filters) != 1 || len(server.OpenFilters) != 0 {
		t.Fatalf("server: Filters=%d OpenFilters=%d", len(server.Filters), len(server.OpenFilters))
	}
	client, err := ClientOptions(WithClientOptions(&Options{}), WithOpenFilter(noopOpenFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(client.OpenFilters) != 1 || len(client.Filters) != 0 {
		t.Fatalf("client: Filters=%d OpenFilters=%d", len(client.Filters), len(client.OpenFilters))
	}
}

func TestWithServiceStoresProtocolAndTarget(t *testing.T) {
	t.Parallel()
	cfg, err := ClientOptions(WithClientOptions(&Options{
		Services: map[string]ServiceOptions{
			"echo.v1.EchoService": {Codec: stubCodecName(1), Target: "ip://127.0.0.1:7001"},
			"other.Svc":           {Target: "ip://127.0.0.1:7002"},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	sc, ok := cfg.Services["echo.v1.EchoService"]
	if !ok || sc.Codec != stubCodecName(1) || sc.Target != "ip://127.0.0.1:7001" {
		t.Fatalf("echo service: %+v ok=%v", sc, ok)
	}
	other, ok := cfg.Services["other.Svc"]
	if !ok || other.Target != "ip://127.0.0.1:7002" || other.Codec != "" {
		t.Fatalf("other service: %+v ok=%v", other, ok)
	}
}

func TestServicesEntryCarriesProtocolAndTarget(t *testing.T) {
	t.Parallel()
	cfg, err := ClientOptions(WithClientOptions(&Options{
		Services: map[string]ServiceOptions{
			"echo.v1.EchoService": {Codec: stubCodecName(1), Target: "ip://127.0.0.1:7001"},
		},
	}))
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

func TestWithClientOptionsOrderIndependent(t *testing.T) {
	t.Parallel()
	base := &Options{MaxConcurrentCalls: 7, MaxHeaderBytes: 2 * miB}
	const maxCalls = 9

	first, err := ClientOptions(WithClientOptions(base), WithMaxConcurrentCalls(maxCalls))
	if err != nil {
		t.Fatal(err)
	}
	last, err := ClientOptions(WithMaxConcurrentCalls(maxCalls), WithClientOptions(base))
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *first, *last)

	if first.MaxConcurrentCalls != maxCalls {
		t.Errorf("MaxConcurrentCalls = %d, want %d", first.MaxConcurrentCalls, maxCalls)
	}
	if first.MaxHeaderBytes != 2*miB {
		t.Errorf("base not carried through: MaxHeaderBytes=%d", first.MaxHeaderBytes)
	}

	srvFirst, err := ServerOptions(WithServerOptions(base), WithServerMaxConcurrentCalls(maxCalls))
	if err != nil {
		t.Fatal(err)
	}
	srvLast, err := ServerOptions(WithServerMaxConcurrentCalls(maxCalls), WithServerOptions(base))
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *srvFirst, *srvLast)
}

func TestWithClientOptionsNilUsesProcessDefault(t *testing.T) {
	t.Parallel()
	got, err := ClientOptions(WithClientOptions(nil))
	if err != nil {
		t.Fatal(err)
	}
	want, err := ClientOptions()
	if err != nil {
		t.Fatal(err)
	}
	assertSameFields(t, *got, *want)
}

func TestConstructorsCopyTheBaseOptions(t *testing.T) {
	t.Parallel()
	base := &Options{
		MaxConcurrentCalls: 3,
		Filters:            []filter.Filter{noopFilter()},
		OpenFilters:        []filter.OpenFilter{noopOpenFilter()},
	}

	server, err := ServerOptions(WithServerOptions(base), WithFilter(noopFilter()))
	if err != nil {
		t.Fatal(err)
	}
	if len(server.Filters) != 2 {
		t.Fatalf("server Filters = %d, want 2", len(server.Filters))
	}

	client, err := ClientOptions(
		WithClientOptions(base),
		WithOpenFilter(noopOpenFilter()),
		WithMaxConcurrentCalls(5),
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
	if base.MaxConcurrentCalls != 3 {
		t.Fatalf("base mutated: MaxConcurrentCalls=%d", base.MaxConcurrentCalls)
	}
	if client.MaxConcurrentCalls != 5 {
		t.Fatalf("client MaxConcurrentCalls = %d, want the option's 5", client.MaxConcurrentCalls)
	}

	base.MaxConcurrentCalls = 99
	if client.MaxConcurrentCalls != 5 || server.MaxConcurrentCalls != 3 {
		t.Fatalf("returned Options still tracks the base: client=%d server=%d",
			client.MaxConcurrentCalls, server.MaxConcurrentCalls)
	}
}

func TestNilOptionRejected(t *testing.T) {
	t.Parallel()
	if _, err := ClientOptions(WithMaxConcurrentCalls(2), nil); err == nil {
		t.Error("ClientOptions(nil): want error")
	}
	if _, err := ServerOptions(WithServerMaxConcurrentCalls(2), nil); err == nil {
		t.Error("ServerOptions(nil): want error")
	}
}

func TestSelectedServiceLayering(t *testing.T) {
	t.Parallel()
	const service = "echo.v1.EchoService"
	entry := WithClientOptions(&Options{
		Services: map[string]ServiceOptions{
			service: {Codec: stubCodecName(2), Target: "ip://127.0.0.1:7001"},
		},
	})

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
				WithCodec(stubCodecName(3)),
				WithTarget("ip://127.0.0.1:9999"),
			},
			wantName:   service,
			wantID:     3,
			wantTarget: "ip://127.0.0.1:9999",
		},
		{
			name:     "no service name selects nothing",
			opts:     []ClientOption{entry},
			wantName: "",
			wantID:   0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := ClientOptions(tc.opts...)
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

func TestSelectionNotInheritedThroughWithClientOptions(t *testing.T) {
	t.Parallel()
	first, err := ClientOptions(
		WithServiceName("echo.v1.EchoService"),
		WithTarget("ip://127.0.0.1:7001"),
		WithCodec(stubCodecName(1)),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ClientOptions(WithClientOptions(first))
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
	if sel.Codec != "" {
		t.Errorf("codec %q inherited from the other Client's call site", sel.Codec)
	}
}

func TestOptionSideTyping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		opt            any
		client, server bool
	}{
		{"WithListenAddress", WithListenAddress(":0"), false, true},
		{"WithServiceName", WithServiceName("echo.v1.EchoService"), true, false},
		{"WithMaxConcurrentCalls", WithMaxConcurrentCalls(2), true, false},
		{"WithServerMaxConcurrentCalls", WithServerMaxConcurrentCalls(2), false, true},
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
	cfg, err := ClientOptions(
		WithClientOptions(&Options{}),
		WithClientCallErrorObserver(func(info CallInfo, e error) {
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
		Phase:     Phase(99),
	}
	want := errors.New("boom")
	NotifyCallError(cfg, info, want)
	if got.Phase != Phase(99) || got.SessionID != "s1" || !errors.Is(gotErr, want) {
		t.Fatalf("got=%+v err=%v", got, gotErr)
	}

	panicCfg, err := ClientOptions(
		WithClientOptions(&Options{}),
		WithClientCallErrorObserver(func(CallInfo, error) {
			panic("observer panic")
		}))
	if err != nil {
		t.Fatal(err)
	}
	NotifyCallError(panicCfg, CallInfo{Phase: PhaseOpen}, want)

	cleared, err := ClientOptions(WithClientOptions(cfg), WithClientCallErrorObserver(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cleared.CallErrorObserver != nil {
		t.Error("WithClientCallErrorObserver(nil) must clear the inherited observer")
	}
}

func TestConnErrorObserverInvokeOnceAndRecover(t *testing.T) {
	t.Parallel()
	var n int
	var got ConnInfo
	cfg, err := ServerOptions(
		WithServerOptions(&Options{}),
		WithServerConnErrorObserver(func(info ConnInfo, e error) {
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

	NotifyConnError(cfg, ConnInfo{Phase: ConnPhase(77)}, errors.New("x"))
	if n != 2 {
		t.Fatalf("unknown phase: n=%d", n)
	}

	panicCfg, err := ServerOptions(
		WithServerOptions(&Options{}),
		WithServerConnErrorObserver(func(ConnInfo, error) {
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

func stubCodecName(id int) string {
	return fmt.Sprintf("argos-test-stub-codec-%d", id)
}

func init() {
	for id := 1; id <= 3; id++ {
		n := id
		codec.Register(stubCodecName(n), func() (codec.Codec, error) {
			return &stubCodec{id: n}, nil
		})
	}
}

func protocolCodecID(t *testing.T, sc ServiceOptions) int {
	t.Helper()
	if sc.Codec == "" {
		return 0
	}
	const prefix = "argos-test-stub-codec-"
	if strings.HasPrefix(sc.Codec, prefix) {
		id, err := strconv.Atoi(sc.Codec[len(prefix):])
		if err != nil {
			t.Fatalf("stub codec name: %v", err)
		}
		return id
	}
	cd, err := sc.AssembleCodec()
	if err != nil {
		t.Fatalf("AssembleCodec: %v", err)
	}
	c, ok := cd.(*stubCodec)
	if !ok {
		t.Fatalf("Codec = %T, want *stubCodec", cd)
	}
	return c.id
}
