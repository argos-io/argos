package irjson_test

import (
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/internal/codegen/frontend/irjson"
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
	if len(files[0].Services) != 1 || files[0].Services[0].GoName != "EchoService" {
		t.Fatalf("unexpected services: %+v", files[0].Services)
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
