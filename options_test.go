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
	// Defaults, not DefaultOptions: built-in limits are zero (no cap).
	d := Defaults()
	assertSameFields(t, d, Options{})
	sess := session.DefaultOptions()
	if sess != (session.Options{}) {
		t.Fatalf("session.DefaultOptions() = %+v, want zero session.Options", sess)
	}
	pool := sessionpool.DefaultOptions()
	if pool != (sessionpool.Options{}) {
		t.Fatalf("sessionpool.DefaultOptions() = %+v, want zero sessionpool.Options", pool)
	}
}

// TestZeroFieldsFilledWithDefaults is the contract that unset fields stay zero.
func TestZeroFieldsFilledWithDefaults(t *testing.T) {
	t.Parallel()
	want := Options{MaxHeaderBytes: 1 << 20}

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
			got, err := tc.build(&Options{MaxHeaderBytes: 1 << 20})
			if err != nil {
				t.Fatalf("%sOptions: %v", tc.side, err)
			}
			assertSameFields(t, *got, want)
		})
	}
}

// TestNegativeValuesRejected: zero is valid; only negative values error.
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
		MaxHeaderBytes:     2 * miB,
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
		if c.cfg.MaxHeaderBytes != 2*miB {
			t.Errorf("%s: MaxHeaderBytes = %d", c.side, c.cfg.MaxHeaderBytes)
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

func TestClientOptionsReturnsIndependentSnapshots(t *testing.T) {
	t.Parallel()
	cfg1, err := ClientOptions(WithClientOptions(&Options{}), WithMaxHeaderBytes(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	cfg2, err := ClientOptions(WithClientOptions(&Options{}), WithMaxHeaderBytes(2<<20))
	if err != nil {
		t.Fatal(err)
	}
	if cfg1.MaxHeaderBytes != 1<<20 {
		t.Fatalf("cfg1.MaxHeaderBytes = %d, want %d", cfg1.MaxHeaderBytes, 1<<20)
	}
	if cfg2.MaxHeaderBytes != 2<<20 {
		t.Fatalf("cfg2.MaxHeaderBytes = %d, want %d", cfg2.MaxHeaderBytes, 2<<20)
	}
	cfg1.MaxHeaderBytes = 99
	if cfg2.MaxHeaderBytes == 99 {
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
