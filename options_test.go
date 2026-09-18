package argos

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

	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
)

func TestDefaultsMatchSection61(t *testing.T) {
	t.Parallel()
	// Defaults, not DefaultOptions: §6.1 pins the built-in numbers, which a
	// process-wide tune elsewhere must not be able to move.
	d := Defaults()

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"MaxFrameSize", d.MaxFrameSize, int64(4 * miB)},
		{"MaxMessageSize", d.MaxMessageSize, int64(4 * miB)},
		{"MaxHeaderBytes", d.MaxHeaderBytes, int64(1 * miB)},
		{"ReadAheadMessages", d.ReadAheadMessages, 1},
		{"MaxConcurrentCalls", d.MaxConcurrentCalls, 64},
		{"MaxBufferedBytes", d.MaxBufferedBytes, int64(1 * giB)},
		{"HandshakeTimeout", d.HandshakeTimeout, 10 * time.Second},
		{"ConnReadBufferSize", d.ConnReadBufferSize, int64(64 * kiB)},
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
	pc, err := d.PerCall()
	if err != nil {
		t.Fatalf("PerCall: %v", err)
	}
	// 4MiB + (1+1)×4MiB + 4MiB = 16MiB
	if want := int64(16 * miB); pc != want {
		t.Errorf("PerCall = %d, want %d", pc, want)
	}

	// §6.1 also pins the session and pool numbers. They no longer live on
	// Options — an axis fixes them when it is constructed, so the defaults moved
	// to session.DefaultOptions / sessionpool.DefaultOptions — but they are still
	// §6.1 numbers, and this is still the test that catches a drift in them.
	// Without this the documented defaults would be pinned nowhere: the axis
	// agreement test only proves an axis seeds from DefaultOptions, not that
	// DefaultOptions holds the numbers §6.1 promises.
	sess := session.DefaultOptions()
	for _, c := range []struct {
		name string
		got  any
		want any
	}{
		{"session.MaxFrameSize", sess.MaxFrameSize, int64(4 * miB)},
		{"session.MaxMessageSize", sess.MaxMessageSize, int64(4 * miB)},
		{"session.MaxMetadataSize", sess.MaxMetadataSize, int64(256 * kiB)},
		{"session.MaxInboundMetadataSize", sess.MaxInboundMetadataSize, int64(4 * miB)},
		{"session.ReadAheadMessages", sess.ReadAheadMessages, 1},
		{"session.OpenTimeout", sess.OpenTimeout, 10 * time.Second},
		{"session.MaxDrainBytes", sess.MaxDrainBytes, int64(1 * miB)},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	pool := sessionpool.DefaultOptions()
	for _, c := range []struct {
		name string
		got  any
		want any
	}{
		{"sessionpool.MaxSessionsPerEndpoint", pool.MaxSessionsPerEndpoint, 64},
		{"sessionpool.MaxIdleSessions", pool.MaxIdleSessions, 8},
		{"sessionpool.SessionIdleTimeout", pool.SessionIdleTimeout, 50 * time.Second},
		{"sessionpool.MaxSessionLifetime", pool.MaxSessionLifetime, 30 * time.Minute},
		{"sessionpool.HandshakeTimeout", pool.HandshakeTimeout, 10 * time.Second},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestZeroFieldsFilledWithDefaults is the contract that lets a Options literal
// name only what it changes: every field the caller left zero comes back at its
// Defaults value, on both sides.
func TestZeroFieldsFilledWithDefaults(t *testing.T) {
	t.Parallel()
	want := Defaults()
	want.MaxMessageSize = 1 << 20

	for _, tc := range []struct {
		side  string
		build func(*Options) (*Options, error)
	}{
		{"client", func(c *Options) (*Options, error) {
			return ClientOptions(WithClientOptions(c))
		}},
		{"server", func(c *Options) (*Options, error) {
			return ServerOptions(WithServerOptions(c))
		}},
	} {
		t.Run(tc.side, func(t *testing.T) {
			t.Parallel()
			got, err := tc.build(&Options{MaxMessageSize: 1 << 20})
			if err != nil {
				t.Fatalf("%sOptions: %v", tc.side, err)
			}
			assertSameFields(t, *got, want)
		})
	}
}

// TestNegativeValuesRejected replaces the old "zero is rejected" cases: zero
// now selects the built-in default, so only a genuinely negative value is still
// an error, and the error must name the field.
func TestNegativeValuesRejected(t *testing.T) {
	t.Parallel()
	forEachNumericField(t, func(t *testing.T, name string, set func(*Options, int64)) {
		cfg := &Options{}
		set(cfg, -5)
		_, err := ServerOptions(WithServerOptions(cfg))
		if err == nil {
			t.Fatalf("%s = -5: want error", name)
		}
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q should name %s", err, name)
		}
	})
}

// sideOwnershipDoc is a test helper documenting §6: side-specific fields share
// one Options with the limits both sides read. Options are typed per side, so
// the mix can only be written as a Options literal — and neither constructor
// rejects it or strips the other side's fields.
func sideOwnershipDoc(t *testing.T) *Options {
	t.Helper()
	return &Options{
		// client-side chain entries
		OpenFilters: []filter.OpenFilter{noopOpenFilter()},
		// shared limits
		MaxFrameSize:       2 * miB,
		MaxConcurrentCalls: 16,
		// server-only
		Filters:               []filter.Filter{noopFilter()},
		MaxInboundConns:       32,
		MaxInboundConnIdle:    time.Minute,
		MaxInboundConnAge:     time.Hour,
		HTTPReadHeaderTimeout: 5 * time.Second,
		HTTPIdleTimeout:       time.Minute,
	}
}

func TestSideOwnershipCoexistsOnOneOptions(t *testing.T) {
	t.Parallel()
	mixed := sideOwnershipDoc(t)

	client, err := ClientOptions(WithClientOptions(mixed))
	if err != nil {
		t.Fatalf("ClientOptions must accept a mixed Options: %v", err)
	}
	server, err := ServerOptions(WithServerOptions(mixed))
	if err != nil {
		t.Fatalf("ServerOptions must accept a mixed Options: %v", err)
	}
	for _, c := range []struct {
		side string
		cfg  *Options
	}{{"client", client}, {"server", server}} {
		if c.cfg.MaxFrameSize != 2*miB {
			t.Errorf("%s: MaxFrameSize = %d", c.side, c.cfg.MaxFrameSize)
		}
		if c.cfg.MaxConcurrentCalls != 16 {
			t.Errorf("%s: MaxConcurrentCalls = %d", c.side, c.cfg.MaxConcurrentCalls)
		}
		if c.cfg.MaxInboundConns != 32 {
			t.Errorf("%s: MaxInboundConns = %d", c.side, c.cfg.MaxInboundConns)
		}
		if len(c.cfg.OpenFilters) != 1 {
			t.Errorf("%s: OpenFilters = %d, want the client-side entry kept", c.side, len(c.cfg.OpenFilters))
		}
		if len(c.cfg.Filters) != 1 {
			t.Errorf("%s: Filters = %d, want the server-side entry kept", c.side, len(c.cfg.Filters))
		}
	}
}

func TestBudgetProductConflict(t *testing.T) {
	t.Parallel()
	_, err := ClientOptions(
		WithClientOptions(&Options{}),
		WithMaxConcurrentCalls(64),
		WithMaxBufferedBytes(1<<20), // 1 MiB, far below 64×16MiB
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

func TestClientOptionsReturnsIndependentSnapshots(t *testing.T) {
	t.Parallel()
	cfg1, err := ClientOptions(WithClientOptions(&Options{}), WithMaxMessageSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	cfg2, err := ClientOptions(WithClientOptions(&Options{}), WithMaxMessageSize(2<<20))
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
// without it, WithFilter on a derived Options would reach the shared base.
func TestCloneDoesNotAliasSliceOrMapFields(t *testing.T) {
	t.Parallel()
	base := &Options{
		Filters:     []filter.Filter{noopFilter()},
		OpenFilters: []filter.OpenFilter{noopOpenFilter()},
		Services:    map[string]ServiceOptions{"echo.v1.Echo": {Target: "ip://127.0.0.1:7001"}},
	}

	// Overwrite rather than append: appending to the copy cannot change the
	// base's length even when both slices share one backing array, so only a
	// write through the copy proves they were really separated.
	clone := base.Clone()
	clone.Filters[0] = nil
	clone.OpenFilters[0] = nil
	clone.Services["other.Svc"] = ServiceOptions{Target: "ip://127.0.0.1:7002"}

	if base.Filters[0] == nil || base.OpenFilters[0] == nil {
		t.Error("clone shares the base's slice backing array")
	}
	if len(base.Services) != 1 {
		t.Errorf("clone shares the base's Services map: %d entries", len(base.Services))
	}

	// A nil receiver is the documented shorthand for "start from the defaults".
	var nilCfg *Options
	got := nilCfg.Clone()
	if got == nil {
		t.Fatal("(*Options)(nil).Clone() = nil, want Defaults()")
	}
	assertSameFields(t, *got, Defaults())
}

func TestPerCallRejectsNilOptionsAndZeroReadAhead(t *testing.T) {
	t.Parallel()
	var nilCfg *Options
	if _, err := nilCfg.PerCall(); err == nil {
		t.Error("PerCall on nil Options: want error")
	}
	// A raw literal is not run through ClientOptions, so PerCall has to defend
	// itself against the unfilled zero rather than trust fillDefaults.
	raw := &Options{MaxFrameSize: 1 << 20, MaxMessageSize: 1 << 20}
	if _, err := raw.PerCall(); err == nil {
		t.Error("PerCall with ReadAheadMessages=0: want error")
	}
}

// TestNoPackageLevelConfigureOrMutableRegistry keeps §3.1-11 honest. The one
// allowed package-level var is defaultOptions: DefaultOptions hands out a pointer
// to it on purpose, and naming it here means a second piece of hidden state
// still fails instead of quietly widening the exemption.
func TestNoPackageLevelConfigureOrMutableRegistry(t *testing.T) {
	t.Parallel()
	const processDefaultVar = "defaultOptions"

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

// forEachNumericField runs fn as a subtest per exported int-kind Options field.
// Walking the struct rather than listing names means a field added later is
// held to the same validation contract from the day it appears.
func forEachNumericField(t *testing.T, fn func(t *testing.T, name string, set func(*Options, int64))) {
	t.Helper()
	typ := reflect.TypeOf(Options{})
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
			fn(t, f.Name, func(c *Options, v int64) {
				reflect.ValueOf(c).Elem().Field(i).SetInt(v)
			})
		})
	}
}

// assertSameFields compares the exported fields of two Options snapshots one by one so a
// mismatch names the field instead of dumping two structs.
func assertSameFields(t *testing.T, got, want Options) {
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
