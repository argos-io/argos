package argos_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/filter"
)

const (
	kiB = 1024
	miB = 1024 * kiB
	giB = 1024 * miB
)

func TestDefaultsMatchSection61(t *testing.T) {
	t.Parallel()
	// Defaults, not DefaultConfig: §6.1 pins the built-in numbers, which a
	// process-wide tune elsewhere must not be able to move.
	d := argos.Defaults()

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"MaxFrameSize", d.MaxFrameSize, int64(4 * miB)},
		{"MaxMessageSize", d.MaxMessageSize, int64(4 * miB)},
		{"MaxMetadataSize", d.MaxMetadataSize, int64(256 * kiB)},
		{"MaxInboundMetadataSize", d.MaxInboundMetadataSize, int64(4 * miB)},
		{"MaxHeaderBytes", d.MaxHeaderBytes, int64(1 * miB)},
		{"ReadAheadMessages", d.ReadAheadMessages, 1},
		{"MaxConcurrentCalls", d.MaxConcurrentCalls, 64},
		{"MaxBufferedBytes", d.MaxBufferedBytes, int64(1 * giB)},
		{"OpenTimeout", d.OpenTimeout, 10 * time.Second},
		{"HandshakeTimeout", d.HandshakeTimeout, 10 * time.Second},
		{"MaxDrainBytes", d.MaxDrainBytes, int64(1 * miB)},
		{"ConnReadBufferSize", d.ConnReadBufferSize, int64(64 * kiB)},
		{"MaxSessionsPerEndpoint", d.MaxSessionsPerEndpoint, 64},
		// Confirmed §6.1 (task 7.5; example/resp/LOAD.md).
		{"MaxIdleSessions", d.MaxIdleSessions, 8},
		{"SessionIdleTimeout", d.SessionIdleTimeout, 50 * time.Second},
		{"MaxSessionLifetime", d.MaxSessionLifetime, 30 * time.Minute},
		{"MaxInboundConns", d.MaxInboundConns, 1024},
		{"MaxInboundConnIdle", d.MaxInboundConnIdle, 50 * time.Second},
		{"MaxInboundConnAge", d.MaxInboundConnAge, 30 * time.Minute},
		{"HTTPReadHeaderTimeout", d.HTTPReadHeaderTimeout, 10 * time.Second},
		{"HTTPIdleTimeout", d.HTTPIdleTimeout, 50 * time.Second},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if d.Binding != nil {
		t.Errorf("Binding = %v, want nil", d.Binding)
	}

	pc, err := d.PerCall()
	if err != nil {
		t.Fatalf("PerCall: %v", err)
	}
	// 4MiB + (1+1)×4MiB + 4MiB = 16MiB
	if want := int64(16 * miB); pc != want {
		t.Errorf("PerCall = %d, want %d", pc, want)
	}
}

// TestZeroFieldsFilledWithDefaults is the contract that lets a Config literal
// name only what it changes: every field the caller left zero comes back at its
// Defaults value, on both sides.
func TestZeroFieldsFilledWithDefaults(t *testing.T) {
	t.Parallel()
	want := argos.Defaults()
	want.MaxMessageSize = 1 << 20

	for _, tc := range []struct {
		side  string
		build func(*argos.Config) (*argos.Config, error)
	}{
		{"client", func(c *argos.Config) (*argos.Config, error) {
			return argos.ClientConfig(argos.WithConfig(c))
		}},
		{"server", func(c *argos.Config) (*argos.Config, error) {
			return argos.ServerConfig(argos.WithConfig(c))
		}},
	} {
		t.Run(tc.side, func(t *testing.T) {
			t.Parallel()
			got, err := tc.build(&argos.Config{MaxMessageSize: 1 << 20})
			if err != nil {
				t.Fatalf("%sConfig: %v", tc.side, err)
			}
			assertSameFields(t, *got, want)
		})
	}
}

// TestNegativeValuesRejected replaces the old "zero is rejected" cases: zero
// now selects the built-in default, so only a genuinely negative value that is
// not Disabled is still an error, and the error must name the field.
func TestNegativeValuesRejected(t *testing.T) {
	t.Parallel()
	forEachNumericField(t, func(t *testing.T, name string, set func(*argos.Config, int64)) {
		cfg := &argos.Config{}
		set(cfg, -5)
		_, err := argos.ServerConfig(argos.WithConfig(cfg))
		if err == nil {
			t.Fatalf("%s = -5: want error", name)
		}
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q should name %s", err, name)
		}
	})
}

// TestDisabledRejectedOnFieldsWithNoOffState guards the sentinel from spreading:
// Disabled is a real off switch on three fields and a mistake everywhere else,
// so the rest must say so instead of reporting a generic "must be > 0".
func TestDisabledRejectedOnFieldsWithNoOffState(t *testing.T) {
	t.Parallel()
	forEachNumericField(t, func(t *testing.T, name string, set func(*argos.Config, int64)) {
		if offStateFields[name] {
			return
		}
		cfg := &argos.Config{}
		set(cfg, argos.Disabled)
		_, err := argos.ServerConfig(argos.WithConfig(cfg))
		if err == nil {
			t.Fatalf("%s = Disabled: want error", name)
		}
		if !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "no off state") {
			t.Errorf("error %q should name %s and say it has no off state", err, name)
		}
	})
}

// TestDisabledTurnsOffOptionalClientLimits is the old "allowed zeros" test
// re-pointed at Disabled: the three optional limits lost their 0-means-off
// reading when 0 became "use the default".
func TestDisabledTurnsOffOptionalClientLimits(t *testing.T) {
	t.Parallel()
	cfg, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithMaxIdleSessions(argos.Disabled),
		argos.WithSessionIdleTimeout(argos.Disabled),
		argos.WithMaxSessionLifetime(argos.Disabled),
	)
	if err != nil {
		t.Fatalf("ClientConfig with Disabled: %v", err)
	}
	if cfg.MaxIdleSessions != 0 || cfg.SessionIdleTimeout != 0 || cfg.MaxSessionLifetime != 0 {
		t.Fatalf("Disabled not turned off: idle=%d sessionIdle=%v lifetime=%v",
			cfg.MaxIdleSessions, cfg.SessionIdleTimeout, cfg.MaxSessionLifetime)
	}

	zeroed, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithMaxIdleSessions(0),
		argos.WithSessionIdleTimeout(0),
		argos.WithMaxSessionLifetime(0),
	)
	if err != nil {
		t.Fatalf("ClientConfig with zeros: %v", err)
	}
	d := argos.Defaults()
	if zeroed.MaxIdleSessions != d.MaxIdleSessions ||
		zeroed.SessionIdleTimeout != d.SessionIdleTimeout ||
		zeroed.MaxSessionLifetime != d.MaxSessionLifetime {
		t.Fatalf("zero must mean default: idle=%d sessionIdle=%v lifetime=%v",
			zeroed.MaxIdleSessions, zeroed.SessionIdleTimeout, zeroed.MaxSessionLifetime)
	}
}

// sideOwnershipDoc is a test helper documenting §6: client-only and
// server-only fields share one Config. Options are typed per side now, so the
// mix can only be written as a Config literal — and neither constructor
// rejects it.
func sideOwnershipDoc(t *testing.T) *argos.Config {
	t.Helper()
	return &argos.Config{
		// client-only
		MaxSessionsPerEndpoint: 16,
		MaxIdleSessions:        argos.Disabled,
		SessionIdleTimeout:     argos.Disabled,
		MaxSessionLifetime:     argos.Disabled,
		// server-only
		MaxInboundConns:    32,
		MaxInboundConnIdle: time.Minute,
		MaxInboundConnAge:  time.Hour,
		OpenTimeout:        5 * time.Second,
		MaxDrainBytes:      1 << 20,
	}
}

func TestSideOwnershipCoexistsOnOneConfig(t *testing.T) {
	t.Parallel()
	mixed := sideOwnershipDoc(t)

	client, err := argos.ClientConfig(argos.WithConfig(mixed))
	if err != nil {
		t.Fatalf("ClientConfig must accept a mixed Config: %v", err)
	}
	server, err := argos.ServerConfig(argos.WithConfig(mixed))
	if err != nil {
		t.Fatalf("ServerConfig must accept a mixed Config: %v", err)
	}
	for _, c := range []struct {
		side string
		cfg  *argos.Config
	}{{"client", client}, {"server", server}} {
		if c.cfg.MaxSessionsPerEndpoint != 16 {
			t.Errorf("%s: MaxSessionsPerEndpoint = %d", c.side, c.cfg.MaxSessionsPerEndpoint)
		}
		if c.cfg.MaxInboundConns != 32 {
			t.Errorf("%s: MaxInboundConns = %d", c.side, c.cfg.MaxInboundConns)
		}
		if c.cfg.MaxIdleSessions != 0 {
			t.Errorf("%s: MaxIdleSessions = %d, want 0", c.side, c.cfg.MaxIdleSessions)
		}
	}
}

func TestBudgetProductConflict(t *testing.T) {
	t.Parallel()
	_, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithMaxConcurrentCalls(64),
		argos.WithMaxBufferedBytes(1<<20), // 1 MiB, far below 64×16MiB
	)
	if err == nil {
		t.Fatal("want budget product conflict error")
	}
	for _, field := range []string{
		"MaxConcurrentCalls",
		"MaxFrameSize",
		"MaxMessageSize",
		"ReadAheadMessages",
		"MaxBufferedBytes",
	} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("error %q missing field %s", err, field)
		}
	}
}

func TestClientConfigReturnsIndependentSnapshots(t *testing.T) {
	t.Parallel()
	cfg1, err := argos.ClientConfig(argos.WithConfig(&argos.Config{}), argos.WithMaxMessageSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	cfg2, err := argos.ClientConfig(argos.WithConfig(&argos.Config{}), argos.WithMaxMessageSize(2<<20))
	if err != nil {
		t.Fatal(err)
	}
	if cfg1.MaxMessageSize != 1<<20 {
		t.Fatalf("cfg1.MaxMessageSize = %d, want %d", cfg1.MaxMessageSize, 1<<20)
	}
	if cfg2.MaxMessageSize != 2<<20 {
		t.Fatalf("cfg2.MaxMessageSize = %d, want %d", cfg2.MaxMessageSize, 2<<20)
	}
	cfg1.MaxMessageSize = 99
	if cfg2.MaxMessageSize == 99 {
		t.Fatal("configs share mutable state")
	}
}

// TestCloneDoesNotAliasSliceOrMapFields is what makes an appending Option safe:
// without it, WithFilter on a derived Config would reach the shared base.
func TestCloneDoesNotAliasSliceOrMapFields(t *testing.T) {
	t.Parallel()
	base := &argos.Config{
		Filters:     []filter.Filter{noopFilter()},
		OpenFilters: []filter.OpenFilter{noopOpenFilter()},
		Services:    map[string]argos.ServiceConfig{"echo.v1.Echo": {Target: "ip://127.0.0.1:7001"}},
	}

	// Overwrite rather than append: appending to the copy cannot change the
	// base's length even when both slices share one backing array, so only a
	// write through the copy proves they were really separated.
	clone := base.Clone()
	clone.Filters[0] = nil
	clone.OpenFilters[0] = nil
	clone.Services["other.Svc"] = argos.ServiceConfig{Target: "ip://127.0.0.1:7002"}

	if base.Filters[0] == nil || base.OpenFilters[0] == nil {
		t.Error("clone shares the base's slice backing array")
	}
	if len(base.Services) != 1 {
		t.Errorf("clone shares the base's Services map: %d entries", len(base.Services))
	}

	// A nil receiver is the documented shorthand for "start from the defaults".
	var nilCfg *argos.Config
	got := nilCfg.Clone()
	if got == nil {
		t.Fatal("(*Config)(nil).Clone() = nil, want Defaults()")
	}
	assertSameFields(t, *got, argos.Defaults())
}

func TestPerCallRejectsNilConfigAndZeroReadAhead(t *testing.T) {
	t.Parallel()
	var nilCfg *argos.Config
	if _, err := nilCfg.PerCall(); err == nil {
		t.Error("PerCall on nil Config: want error")
	}
	// A raw literal is not run through ClientConfig, so PerCall has to defend
	// itself against the unfilled zero rather than trust fillDefaults.
	raw := &argos.Config{MaxFrameSize: 1 << 20, MaxMessageSize: 1 << 20}
	if _, err := raw.PerCall(); err == nil {
		t.Error("PerCall with ReadAheadMessages=0: want error")
	}
}

// TestNoPackageLevelConfigureOrMutableRegistry keeps §3.1-11 honest. The one
// allowed package-level var is defaultConfig: DefaultConfig hands out a pointer
// to it on purpose, and naming it here means a second piece of hidden state
// still fails instead of quietly widening the exemption.
func TestNoPackageLevelConfigureOrMutableRegistry(t *testing.T) {
	t.Parallel()
	const processDefaultVar = "defaultConfig"

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		if f.Name.Name != "argos" {
			continue
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Name.Name == "Configure" {
					t.Errorf("%s: forbidden process-level Configure", name)
				}
			case *ast.GenDecl:
				if d.Tok != token.VAR {
					continue
				}
				for _, spec := range d.Specs {
					vs := spec.(*ast.ValueSpec)
					for _, id := range vs.Names {
						if id.Name == processDefaultVar {
							continue
						}
						t.Errorf("%s: package-level var %s forbidden (§3.1-11); only %s may hold process state", name, id.Name, processDefaultVar)
					}
				}
			}
		}
	}
}

// offStateFields are the only fields Disabled is valid on (§6.1).
var offStateFields = map[string]bool{
	"MaxIdleSessions":    true,
	"SessionIdleTimeout": true,
	"MaxSessionLifetime": true,
}

// forEachNumericField runs fn as a subtest per exported int-kind Config field.
// Walking the struct rather than listing names means a field added later is
// held to the same validation contract from the day it appears.
func forEachNumericField(t *testing.T, fn func(t *testing.T, name string, set func(*argos.Config, int64))) {
	t.Helper()
	typ := reflect.TypeOf(argos.Config{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		switch f.Type.Kind() {
		case reflect.Int, reflect.Int64: // int, int64 and time.Duration
		default:
			continue
		}
		t.Run(f.Name, func(t *testing.T) {
			t.Parallel()
			fn(t, f.Name, func(c *argos.Config, v int64) {
				reflect.ValueOf(c).Elem().Field(i).SetInt(v)
			})
		})
	}
}

// assertSameFields compares the exported fields of two Configs one by one so a
// mismatch names the field instead of dumping two structs.
func assertSameFields(t *testing.T, got, want argos.Config) {
	t.Helper()
	gv, wv := reflect.ValueOf(got), reflect.ValueOf(want)
	typ := gv.Type()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		g, w := gv.Field(i).Interface(), wv.Field(i).Interface()
		if !reflect.DeepEqual(g, w) {
			t.Errorf("%s = %v, want %v", f.Name, g, w)
		}
	}
}
