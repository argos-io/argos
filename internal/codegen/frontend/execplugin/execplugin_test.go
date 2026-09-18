package execplugin

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/argos-io/argos/internal/codegen/gen"
	"github.com/argos-io/argos/internal/codegen/gen/message"
	"github.com/argos-io/argos/internal/codegen/ir"
)

func TestPluginEmitIR(t *testing.T) {
	name := "fake-emit-ir.sh"
	if runtime.GOOS == "windows" {
		name = "fake-emit-ir.cmd"
	}
	script, err := filepath.Abs(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	files, err := Frontend{Command: script}.Parse(
		t.Context(),
		[]string{"ignored.thrift"},
	)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	svc := files[0].Services[0]
	if got, want := svc.FullName, "echo.v1.EchoService"; got != want {
		t.Fatalf("Service.FullName = %q, want %q", got, want)
	}
	if len(svc.Methods) != 2 {
		t.Fatalf("methods = %d, want 2", len(svc.Methods))
	}
	if got, want := svc.Methods[0].FullName, "echo.v1.EchoService.Echo"; got != want {
		t.Fatalf("Echo.FullName = %q, want %q", got, want)
	}
	if got, want := svc.Methods[0].Shape, ir.Unary; got != want {
		t.Fatalf("Echo.Shape = %v, want %v", got, want)
	}
	if got, want := svc.Methods[1].FullName, "echo.v1.EchoService.Watch"; got != want {
		t.Fatalf("Watch.FullName = %q, want %q", got, want)
	}
	if got, want := svc.Methods[1].Shape, ir.ServerStreaming; got != want {
		t.Fatalf("Watch.Shape = %v, want %v", got, want)
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
	_, err := Frontend{}.Parse(t.Context(), []string{"x"})
	if err == nil {
		t.Fatal("empty command: want error")
	}
}

func TestPluginBadExit(t *testing.T) {
	_, err := Frontend{Command: "/bin/false"}.Parse(
		t.Context(),
		[]string{"x"},
	)
	if err == nil {
		t.Fatal("bad plugin exit: want error")
	}
}
