package execplugin_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/internal/codegen/frontend/execplugin"
	"github.com/argos-io/argos/internal/codegen/gen"
	"github.com/argos-io/argos/internal/codegen/gen/message"
)

func TestPluginEmitIR(t *testing.T) {
	script, err := filepath.Abs(filepath.Join("testdata", "fake-emit-ir.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	files, err := execplugin.Frontend{Command: script}.Parse(
		t.Context(),
		[]string{"ignored.thrift"},
	)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if len(files[0].Services[0].Methods) != 2 {
		t.Fatalf("methods = %d, want 2", len(files[0].Services[0].Methods))
	}
	stubOut, err := gen.Generate(files[0])
	if err != nil {
		t.Fatalf("Generate stub: %v", err)
	}
	if len(stubOut) == 0 {
		t.Fatal("Generate stub: empty output")
	}
	msgOut, err := message.Generate(files[0])
	if err != nil {
		t.Fatalf("Generate message: %v", err)
	}
	if len(msgOut) == 0 {
		t.Fatal("Generate message: empty output")
	}
}

func TestPluginEmptyCommand(t *testing.T) {
	_, err := execplugin.Frontend{}.Parse(t.Context(), []string{"x"})
	if err == nil {
		t.Fatal("empty command: want error")
	}
}

func TestPluginBadExit(t *testing.T) {
	_, err := execplugin.Frontend{Command: "/bin/false"}.Parse(
		t.Context(),
		[]string{"x"},
	)
	if err == nil {
		t.Fatal("bad plugin exit: want error")
	}
}
