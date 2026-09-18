package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func chdirRepoRoot(t *testing.T) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if err := os.Chdir(dir); err != nil {
				t.Fatal(err)
			}
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func captureStdout(t *testing.T, fn func() error) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = old })

	runErr := fn()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = old

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatal(err)
	}
	if runErr != nil {
		t.Fatalf("command: %v", runErr)
	}
	return buf.String()
}

func TestFrontendList(t *testing.T) {
	out := captureStdout(t, func() error {
		return App().Run(context.Background(), []string{"argos", "frontend", "list"})
	})
	for _, want := range []string{"Built-in frontends:", "proto", "ir", "emit-ir"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestGenerateStubEchoProto(t *testing.T) {
	chdirRepoRoot(t)
	dir := t.TempDir()
	err := App().Run(context.Background(), []string{
		"argos", "generate", "stub",
		"--from", "proto",
		"--proto-path", "example/echo",
		"--out", dir,
		"example/echo/echo.proto",
	})
	if err != nil {
		t.Fatalf("generate stub: %v", err)
	}
	stubPath := filepath.Join(dir, "echo.argos.go")
	data, err := os.ReadFile(stubPath)
	if err != nil {
		t.Fatalf("read stub: %v", err)
	}
	source := string(data)
	for _, want := range []string{
		"EchoServiceDesc",
		"EchoService_Echo",
		"EchoServiceHandlers",
		"descriptor.MustMethod",
		"status.ErrCardinality",
		"map[string]filter.Handler",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("stub missing %q\n%s", want, source)
		}
	}
	if strings.Contains(source, "switch method") {
		t.Fatal("stub still contains method switch")
	}
}

// TestGenerateStubCheckIsAGate mirrors the Makefile's test-generate rule: a
// --check run must fail on drift, and must compare the file generation writes
// rather than whichever path it was handed.
func TestGenerateStubCheckIsAGate(t *testing.T) {
	chdirRepoRoot(t)
	dir := t.TempDir()
	args := []string{
		"argos", "generate", "stub",
		"--from", "proto",
		"--proto-path", "example/echo",
		"--out", dir,
		"example/echo/echo.proto",
	}
	if err := App().Run(t.Context(), args); err != nil {
		t.Fatalf("generate stub: %v", err)
	}
	stubPath := filepath.Join(dir, "echo.argos.go")
	checkArgs := append([]string{"argos", "generate", "stub", "--check", stubPath}, args[3:]...)
	if err := App().Run(t.Context(), checkArgs); err != nil {
		t.Fatalf("check of freshly generated stub: %v", err)
	}

	data, err := os.ReadFile(stubPath)
	if err != nil {
		t.Fatalf("read stub: %v", err)
	}
	if err := os.WriteFile(stubPath, append(data, []byte("\n// drift\n")...), 0o644); err != nil {
		t.Fatalf("drift stub: %v", err)
	}
	err = App().Run(t.Context(), checkArgs)
	if err == nil || !strings.Contains(err.Error(), stubPath) {
		t.Fatalf("check error = %v, want a diff naming %s", err, stubPath)
	}

	// The shipped artifact stays stale: another directory holding a fresh copy
	// must not stand in for it.
	decoy := t.TempDir()
	if err := os.WriteFile(filepath.Join(decoy, "echo.argos.go"), data, 0o644); err != nil {
		t.Fatalf("write decoy: %v", err)
	}
	decoyArgs := append([]string{"argos", "generate", "stub", "--check", filepath.Join(decoy, "echo.argos.go")}, args[3:]...)
	if err := App().Run(t.Context(), decoyArgs); err == nil {
		t.Fatal("check passed against a copy while the generated stub is stale")
	}
}

func TestGenerateStubRejectsPluginAndFrom(t *testing.T) {
	chdirRepoRoot(t)
	err := App().Run(context.Background(), []string{
		"argos", "generate", "stub",
		"--plugin", "fake-plugin",
		"--from", "proto",
		"example/echo/echo.proto",
	})
	if err == nil {
		t.Fatal("expected error when --plugin and --from are both set")
	}
	if !strings.Contains(err.Error(), "use either --plugin or --from") {
		t.Fatalf("error = %v", err)
	}
}
