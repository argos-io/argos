package irjson_test

import (
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/internal/codegen/frontend/irjson"
	"github.com/argos-io/argos/internal/codegen/ir"
)

func TestParseSingleFile(t *testing.T) {
	path := filepath.Join("testdata", "echo.ir.json")
	files, err := irjson.Frontend{}.Parse(t.Context(), []string{path})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if got, want := files[0].GoPackage, "github.com/argos-io/argos/example/echo"; got != want {
		t.Fatalf("GoPackage = %q, want %q", got, want)
	}
	svc := files[0].Services[0]
	if svc.GoName != "EchoService" {
		t.Fatalf("unexpected services: %+v", files[0].Services)
	}
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
}

func TestParseArrayFile(t *testing.T) {
	path := filepath.Join("testdata", "files.ir.json")
	files, err := irjson.Frontend{}.Parse(t.Context(), []string{path})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("files = %d, want 2", len(files))
	}
}

func TestParseInvalidJSON(t *testing.T) {
	path := filepath.Join("testdata", "invalid.ir.json")
	_, err := irjson.Frontend{}.Parse(t.Context(), []string{path})
	if err == nil {
		t.Fatal("Parse invalid JSON: want error")
	}
}

func TestParseNoInputs(t *testing.T) {
	_, err := irjson.Frontend{}.Parse(t.Context(), nil)
	if err == nil {
		t.Fatal("Parse empty inputs: want error")
	}
}
