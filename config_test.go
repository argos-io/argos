package argos_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
)

func TestDefaultsMatchSection61(t *testing.T) {
	cfg, err := argos.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const (
		kiB = 1024
		miB = 1024 * kiB
		giB = 1024 * miB
	)

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"MaxFrameSize", cfg.MaxFrameSize, int64(4 * miB)},
		{"MaxMessageSize", cfg.MaxMessageSize, int64(4 * miB)},
		{"MaxMetadataSize", cfg.MaxMetadataSize, int64(256 * kiB)},
		{"MaxHeaderBytes", cfg.MaxHeaderBytes, int64(1 * miB)},
		{"ReadAheadMessages", cfg.ReadAheadMessages, 1},
		{"MaxConcurrentCalls", cfg.MaxConcurrentCalls, 64},
		{"MaxBufferedBytes", cfg.MaxBufferedBytes, int64(1 * giB)},
		{"OpenTimeout", cfg.OpenTimeout, 10 * time.Second},
		{"HandshakeTimeout", cfg.HandshakeTimeout, 10 * time.Second},
		{"MaxDrainBytes", cfg.MaxDrainBytes, int64(1 * miB)},
		{"ConnReadBufferSize", cfg.ConnReadBufferSize, int64(64 * kiB)},
		{"MaxSessionsPerEndpoint", cfg.MaxSessionsPerEndpoint, 64},
		// Confirmed §6.1 (task 7.5; example/resp/LOAD.md).
		{"MaxIdleSessions", cfg.MaxIdleSessions, 8},
		{"SessionIdleTimeout", cfg.SessionIdleTimeout, 50 * time.Second},
		{"MaxSessionLifetime", cfg.MaxSessionLifetime, 30 * time.Minute},
		{"MaxInboundConns", cfg.MaxInboundConns, 1024},
		{"MaxInboundConnIdle", cfg.MaxInboundConnIdle, 50 * time.Second},
		{"MaxInboundConnAge", cfg.MaxInboundConnAge, 30 * time.Minute},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if cfg.Binding != nil {
		t.Errorf("Binding = %v, want nil", cfg.Binding)
	}

	pc, err := cfg.PerCall()
	if err != nil {
		t.Fatalf("PerCall: %v", err)
	}
	// 4MiB + (1+1)×4MiB + 4MiB = 16MiB
	if want := int64(16 * miB); pc != want {
		t.Errorf("PerCall = %d, want %d", pc, want)
	}
}

func TestZeroMaxMessageSizeRejected(t *testing.T) {
	_, err := argos.New(argos.WithMaxMessageSize(0))
	if err == nil {
		t.Fatal("New(WithMaxMessageSize(0)): want error")
	}
	if !strings.Contains(err.Error(), "MaxMessageSize") {
		t.Fatalf("error %q should mention MaxMessageSize", err)
	}
}

func TestZeroMaxConcurrentCallsRejected(t *testing.T) {
	_, err := argos.New(argos.WithMaxConcurrentCalls(0))
	if err == nil {
		t.Fatal("want error for MaxConcurrentCalls=0")
	}
}

func TestReadAheadMessagesMustBeAtLeastOne(t *testing.T) {
	_, err := argos.New(argos.WithReadAheadMessages(0))
	if err == nil {
		t.Fatal("want error for ReadAheadMessages=0")
	}
}

func TestZeroMaxInboundConnIdleRejected(t *testing.T) {
	_, err := argos.New(argos.WithMaxInboundConnIdle(0))
	if err == nil {
		t.Fatal("want error for MaxInboundConnIdle=0")
	}
	if !strings.Contains(err.Error(), "MaxInboundConnIdle") {
		t.Fatalf("error %q should mention MaxInboundConnIdle", err)
	}
}

func TestZeroMaxInboundConnAgeRejected(t *testing.T) {
	_, err := argos.New(argos.WithMaxInboundConnAge(0))
	if err == nil {
		t.Fatal("want error for MaxInboundConnAge=0")
	}
	if !strings.Contains(err.Error(), "MaxInboundConnAge") {
		t.Fatalf("error %q should mention MaxInboundConnAge", err)
	}
}

func TestAllowedZeroClientSessionLimits(t *testing.T) {
	cfg, err := argos.New(
		argos.WithMaxIdleSessions(0),
		argos.WithSessionIdleTimeout(0),
		argos.WithMaxSessionLifetime(0),
	)
	if err != nil {
		t.Fatalf("New with allowed zeros: %v", err)
	}
	if cfg.MaxIdleSessions != 0 || cfg.SessionIdleTimeout != 0 || cfg.MaxSessionLifetime != 0 {
		t.Fatalf("zeros not stored: idle=%d sessionIdle=%v lifetime=%v",
			cfg.MaxIdleSessions, cfg.SessionIdleTimeout, cfg.MaxSessionLifetime)
	}
}

// sideOwnershipDoc is a test helper documenting §6: client-only and
// server-only fields share one Config; New does not reject the mix.
func sideOwnershipDoc(t *testing.T) *argos.Config {
	t.Helper()
	cfg, err := argos.New(
		// client-only
		argos.WithMaxSessionsPerEndpoint(16),
		argos.WithMaxIdleSessions(0),
		argos.WithSessionIdleTimeout(0),
		argos.WithMaxSessionLifetime(0),
		// server-only
		argos.WithMaxInboundConns(32),
		argos.WithMaxInboundConnIdle(time.Minute),
		argos.WithMaxInboundConnAge(time.Hour),
		argos.WithOpenTimeout(5*time.Second),
		argos.WithMaxDrainBytes(1<<20),
	)
	if err != nil {
		t.Fatalf("mixed side options must be accepted by New: %v", err)
	}
	return cfg
}

func TestSideOwnershipCoexistsOnOneConfig(t *testing.T) {
	cfg := sideOwnershipDoc(t)
	if cfg.MaxSessionsPerEndpoint != 16 {
		t.Errorf("MaxSessionsPerEndpoint = %d", cfg.MaxSessionsPerEndpoint)
	}
	if cfg.MaxInboundConns != 32 {
		t.Errorf("MaxInboundConns = %d", cfg.MaxInboundConns)
	}
}

func TestBudgetProductConflict(t *testing.T) {
	_, err := argos.New(
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

func TestNewReturnsIndependentSnapshots(t *testing.T) {
	cfg1, err := argos.New(argos.WithMaxMessageSize(1 << 20))
	if err != nil {
		t.Fatal(err)
	}
	cfg2, err := argos.New(argos.WithMaxMessageSize(2 << 20))
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

func TestNoPackageLevelConfigureOrMutableRegistry(t *testing.T) {
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
						t.Errorf("%s: package-level var %s forbidden (§3.1-11); Config must be explicit values", name, id.Name)
					}
				}
			}
		}
	}
}
