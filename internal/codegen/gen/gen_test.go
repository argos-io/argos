package gen_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/internal/codegen/frontend/proto"
	"github.com/argos-io/argos/internal/codegen/gen"
)

func TestGenerateMatchesHandwrittenEcho(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	echoDir := filepath.Join(root, "example", "echo")
	handwritten := filepath.Join(echoDir, "echo.argos.go")

	files, err := proto.Frontend{ImportPaths: []string{echoDir}}.Parse(
		t.Context(),
		[]string{"echo.proto"},
	)
	if err != nil {
		t.Fatalf("parse proto: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	got, err := gen.Generate(files[0])
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	want, err := os.ReadFile(handwritten)
	if err != nil {
		t.Fatalf("read handwritten: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("generated output differs from %s", handwritten)
	}
}
