package message_test

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
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

func TestGenerateComplexMessages(t *testing.T) {
	dir := filepath.Join("testdata")
	files, err := proto.Frontend{ImportPaths: []string{dir}}.Parse(
		t.Context(),
		[]string{"complex.proto"},
	)
	if err != nil {
		t.Fatalf("parse complex proto: %v", err)
	}
	got, err := message.Generate(files[0])
	if err != nil {
		t.Fatalf("generate complex messages: %v", err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "complex.pb.go", got, parser.AllErrors); err != nil {
		t.Fatalf("generated source is not valid Go: %v", err)
	}
	for _, want := range []string{
		"type TopState int32",
		"type Outer_LocalState int32",
		"type Outer_Inner struct",
		"map[string]int64",
		"protobuf_key:",
		"json:\"snakeName,omitempty\"",
		"TopState_TOP_STATE_UNSPECIFIED",
		"TopState_live",
		"return TopState_TOP_STATE_UNSPECIFIED",
		"Nickname      *string",
		"GetNickname() string",
		"return *x.Nickname",
		"GetCount() int32",
	} {
		if !strings.Contains(string(got), want) {
			t.Fatalf("generated complex source missing %q", want)
		}
	}
}
