package stub

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/argos-io/argos/internal/codegen/ir"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestRunRejectsOutputPathCollisionBeforeWriting(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "files.ir.json")
	const irJSON = `[
  {
    "ir_version": 1,
    "go_package": "example.com/test",
    "outputs": {"stub": "same.argos.go"},
    "services": [{"go_name": "FirstService", "methods": []}]
  },
  {
    "ir_version": 1,
    "go_package": "example.com/test",
    "outputs": {"stub": "same.argos.go"},
    "services": [{"go_name": "SecondService", "methods": []}]
  }
]`
	if err := os.WriteFile(input, []byte(irJSON), 0o644); err != nil {
		t.Fatalf("write IR: %v", err)
	}

	err := Run(context.Background(), Options{From: "ir", OutDir: dir}, []string{input})
	if err == nil || !strings.Contains(err.Error(), "generated more than once") {
		t.Fatalf("Run error = %v, want output collision", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "same.argos.go")); !os.IsNotExist(statErr) {
		t.Fatalf("collision output exists: %v", statErr)
	}
}

func TestSafeOutputPathRejectsTraversal(t *testing.T) {
	base := t.TempDir()
	for _, name := range []string{"..", "../outside.go", `..\outside.go`} {
		if _, err := safeOutputPath(base, name); err == nil {
			t.Fatalf("safeOutputPath(%q) succeeded", name)
		}
	}
	path, err := safeOutputPath(base, filepath.Join("nested", "service.argos.go"))
	if err != nil {
		t.Fatalf("safeOutputPath nested: %v", err)
	}
	if !strings.HasPrefix(path, base+string(filepath.Separator)) {
		t.Fatalf("path %q escaped base %q", path, base)
	}
}

func TestWriteFileReplacesOutputAfterCompleteWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "generated.go")
	if err := writeFile(path, []byte("first")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := writeFile(path, []byte("second")); err != nil {
		t.Fatalf("second write: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("output = %q, want second", got)
	}
}

func TestGenerateFileOutputsBeforeWriting(t *testing.T) {
	descriptor, err := proto.Marshal(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("sample.proto"),
		Package: proto.String("sample"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Request"),
		}},
	})
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}

	dir := t.TempDir()
	err = writeFileOutputs(Options{OutDir: dir}, []string{"sample.proto"}, ir.File{
		Source:         ir.SourceProto,
		GoPackage:      "sample",
		InputBase:      "sample",
		FileDescriptor: descriptor,
	})
	if err == nil {
		t.Fatal("writeFileOutputs succeeded without services")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "sample.pb.go")); !os.IsNotExist(statErr) {
		t.Fatalf("message output exists after generation failure: %v", statErr)
	}
}
