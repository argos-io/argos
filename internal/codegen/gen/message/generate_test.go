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
		"TopState_TOP_STATE_UNSPECIFIED",
		"TopState_live",
		"return TopState_TOP_STATE_UNSPECIFIED",
		"Nickname      *string",
		"GetNickname() string",
		"return *x.Nickname",
		"GetCount() int32",
		// Struct tags below are protoc-gen-go's byte for byte: the json element
		// of the json tag carries the proto field name (not the JSON name), the
		// oneof element follows proto3, there is no msg element, and an enum
		// field records its enum type.
		"`protobuf:\"bytes,1,opt,name=snake_name,json=snakeName,proto3\" json:\"snake_name,omitempty\"`",
		"`protobuf:\"varint,2,rep,packed,name=numbers,proto3\" json:\"numbers,omitempty\"`",
		"`protobuf:\"bytes,3,rep,name=labels,proto3\" json:\"labels,omitempty\" protobuf_key:\"bytes,1,opt,name=key,proto3\" protobuf_val:\"varint,2,opt,name=value,proto3\"`",
		"`protobuf:\"bytes,4,opt,name=child,proto3\" json:\"child,omitempty\"`",
		"`protobuf:\"varint,5,opt,name=state,proto3,enum=complex.v1.TopState\" json:\"state,omitempty\"`",
		"`protobuf:\"bytes,6,opt,name=nickname,proto3,oneof\" json:\"nickname,omitempty\"`",
		"`protobuf:\"varint,7,opt,name=count,proto3,oneof\" json:\"count,omitempty\"`",
		"`protobuf:\"varint,8,rep,name=unpacked,proto3\" json:\"unpacked,omitempty\"`",
	} {
		if !strings.Contains(string(got), want) {
			t.Fatalf("generated complex source missing %q", want)
		}
	}
	if strings.Contains(string(got), "msg=") {
		t.Fatal("generated complex source still emits the msg struct tag element")
	}
}
