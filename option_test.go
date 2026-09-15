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
	var sawFilter, sawOpen bool
	f := filter.Filter(func(ctx context.Context, m descriptor.Method, st stream.Stream, next filter.Handler) error {
		sawFilter = true
		return next(ctx, m, st)
	})
	of := filter.OpenFilter(func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
		sawOpen = true
		return next(ctx, m)
	})
	cfg, err := argos.New(argos.WithFilter(f), argos.WithOpenFilter(of))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Filters) != 1 || len(cfg.OpenFilters) != 1 {
		t.Fatalf("Filters=%d OpenFilters=%d", len(cfg.Filters), len(cfg.OpenFilters))
	}
	_ = sawFilter
	_ = sawOpen
}

func TestWithServiceStoresBindingAndTarget(t *testing.T) {
	fn := argos.BindingFunc(func() (argos.Binding, error) {
		return argos.Binding{}, nil
	})
	cfg, err := argos.New(
		argos.WithService("echo.v1.EchoService",
			argos.ServiceBinding(fn),
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

func TestConfigWithDerivesWithoutMutatingOriginal(t *testing.T) {
	base, err := argos.New(argos.WithMaxIdleSessions(3))
	if err != nil {
		t.Fatal(err)
	}
	derived, err := base.With(argos.WithMaxIdleSessions(0), argos.WithFilter(
		func(ctx context.Context, m descriptor.Method, st stream.Stream, next filter.Handler) error {
			return next(ctx, m, st)
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if base.MaxIdleSessions != 3 {
		t.Fatalf("base mutated: MaxIdleSessions=%d", base.MaxIdleSessions)
	}
	if len(base.Filters) != 0 {
		t.Fatalf("base Filters mutated: %d", len(base.Filters))
	}
	if derived.MaxIdleSessions != 0 || len(derived.Filters) != 1 {
		t.Fatalf("derived: idle=%d filters=%d", derived.MaxIdleSessions, len(derived.Filters))
	}
}

func TestCallErrorObserverUnknownPhaseAndRecover(t *testing.T) {
	var got argos.CallInfo
	var gotErr error
	cfg, err := argos.New(argos.WithCallErrorObserver(func(info argos.CallInfo, e error) {
		got = info
		gotErr = e
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CallErrorObserver() == nil {
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

	panicCfg, err := argos.New(argos.WithCallErrorObserver(func(argos.CallInfo, error) {
		panic("observer panic")
	}))
	if err != nil {
		t.Fatal(err)
	}
	argos.NotifyCallError(panicCfg, argos.CallInfo{Phase: argos.PhaseOpen}, want) // must not panic
}

func TestConnErrorObserverInvokeOnceAndRecover(t *testing.T) {
	var n int
	var got argos.ConnInfo
	cfg, err := argos.New(argos.WithConnErrorObserver(func(info argos.ConnInfo, e error) {
		n++
		got = info
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnErrorObserver() == nil {
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

	panicCfg, err := argos.New(argos.WithConnErrorObserver(func(argos.ConnInfo, error) {
		panic("conn observer panic")
	}))
	if err != nil {
		t.Fatal(err)
	}
	argos.NotifyConnError(panicCfg, argos.ConnInfo{Phase: argos.ConnPhaseDial}, errors.New("d"))
}
