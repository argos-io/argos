package message_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/internal/codegen/check"
	"github.com/argos-io/argos/internal/codegen/frontend/proto"
	"github.com/argos-io/argos/internal/codegen/gen/message"
)

func TestGenerateEchoMessages(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	protoPath := filepath.Join(root, "example", "echo", "echo.proto")
	files, err := proto.Frontend{ImportPaths: []string{root}}.Parse(
		t.Context(),
		[]string{protoPath},
	)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := message.Generate(files[0])
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(root, "example", "echo", "echo.pb.go"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(check.Normalize(got), check.Normalize(want)) {
		t.Fatal("generated echo.pb.go differs from checked-in file")
	}
}
