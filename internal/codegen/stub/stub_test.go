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

// checkFixture writes a proto input that generates both a stub and a message
// file, and returns the options and inputs that generate it.
func checkFixture(t *testing.T) (Options, []string) {
	t.Helper()
	dir := t.TempDir()
	const source = `syntax = "proto3";

package check.v1;

option go_package = "example.com/check;checkv1";

message Request {
  string id = 1;
}

message Response {
  string id = 1;
}

service CheckService {
  rpc Do(Request) returns (Response);
}
`
	protoPath := filepath.Join(dir, "check.proto")
	if err := os.WriteFile(protoPath, []byte(source), 0o644); err != nil {
		t.Fatalf("write proto: %v", err)
	}
	return Options{From: "proto", ImportPaths: []string{dir}}, []string{protoPath}
}

// outputPathFor returns where generation writes name for these inputs when no
// --out is given: next to the input file.
func outputPathFor(t *testing.T, inputs []string, name string) string {
	t.Helper()
	return filepath.Join(filepath.Dir(inputs[0]), name)
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	data, err := os.ReadFile(from)
	if err != nil {
		t.Fatalf("read %s: %v", from, err)
	}
	if err := os.WriteFile(to, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", to, err)
	}
}

// TestCheckVerifiesEveryGeneratedFile pins that --check compares the stub and
// the message file that generation would write, whichever of the two the caller
// names.
func TestCheckVerifiesEveryGeneratedFile(t *testing.T) {
	generated := []string{"check.argos.go", "check.pb.go"}
	for _, checked := range generated {
		for _, stale := range generated {
			t.Run("check "+checked+" with stale "+stale, func(t *testing.T) {
				opts, inputs := checkFixture(t)
				if err := Run(t.Context(), opts, inputs); err != nil {
					t.Fatalf("generate: %v", err)
				}

				checkOpts := opts
				checkOpts.Check = outputPathFor(t, inputs, checked)
				if err := Run(t.Context(), checkOpts, inputs); err != nil {
					t.Fatalf("check of freshly generated output: %v", err)
				}

				stalePath := outputPathFor(t, inputs, stale)
				if err := os.WriteFile(stalePath, []byte("// stale\n"), 0o644); err != nil {
					t.Fatalf("stale %s: %v", stale, err)
				}
				err := Run(t.Context(), checkOpts, inputs)
				if err == nil || !strings.Contains(err.Error(), stalePath) {
					t.Fatalf("check error = %v, want a diff naming %s", err, stalePath)
				}
			})
		}
	}
}

// TestCheckRejectsPathGenerationWouldNotWrite is the loophole test: taking the
// --check path verbatim let a pristine copy of the output stand in for the
// shipped artifact, so the check passed while the real file was stale.
func TestCheckRejectsPathGenerationWouldNotWrite(t *testing.T) {
	opts, inputs := checkFixture(t)
	if err := Run(t.Context(), opts, inputs); err != nil {
		t.Fatalf("generate: %v", err)
	}

	decoy := t.TempDir()
	for _, name := range []string{"check.argos.go", "check.pb.go"} {
		copyFile(t, outputPathFor(t, inputs, name), filepath.Join(decoy, name))
	}
	stalePath := outputPathFor(t, inputs, "check.argos.go")
	if err := os.WriteFile(stalePath, []byte("// stale\n"), 0o644); err != nil {
		t.Fatalf("stale stub: %v", err)
	}

	checkOpts := opts
	checkOpts.Check = filepath.Join(decoy, "check.argos.go")
	err := Run(t.Context(), checkOpts, inputs)
	if err == nil {
		t.Fatal("check passed against a copy generation would not write while the shipped artifact is stale")
	}
	if !strings.Contains(err.Error(), stalePath) {
		t.Fatalf("check error = %v, want it to name the generated path %s", err, stalePath)
	}
}

// TestCheckComparesUnderOutDir pins that the comparison target follows --out,
// so a path outside --out cannot redirect it and a stale artifact inside --out
// cannot hide.
func TestCheckComparesUnderOutDir(t *testing.T) {
	opts, inputs := checkFixture(t)
	outDir := t.TempDir()
	genOpts := opts
	genOpts.OutDir = outDir
	if err := Run(t.Context(), genOpts, inputs); err != nil {
		t.Fatalf("generate: %v", err)
	}

	// The default location stays fresh; the artifact under --out goes stale.
	for _, name := range []string{"check.argos.go", "check.pb.go"} {
		copyFile(t, filepath.Join(outDir, name), outputPathFor(t, inputs, name))
	}
	stalePath := filepath.Join(outDir, "check.pb.go")
	if err := os.WriteFile(stalePath, []byte("// stale\n"), 0o644); err != nil {
		t.Fatalf("stale companion: %v", err)
	}

	checkOpts := genOpts
	checkOpts.Check = outputPathFor(t, inputs, "check.argos.go")
	err := Run(t.Context(), checkOpts, inputs)
	if err == nil {
		t.Fatal("--check verified a path outside --out while the artifact under --out is stale")
	}
	if !strings.Contains(err.Error(), filepath.Join(outDir, "check.argos.go")) {
		t.Fatalf("check error = %v, want it to name the generated path under --out", err)
	}

	// Naming the generated path does compare the stale artifact under --out.
	checkOpts.Check = filepath.Join(outDir, "check.argos.go")
	err = Run(t.Context(), checkOpts, inputs)
	if err == nil || !strings.Contains(err.Error(), stalePath) {
		t.Fatalf("check error = %v, want a diff naming %s", err, stalePath)
	}

	if err := os.WriteFile(stalePath, []byte("// fresh\n"), 0o644); err != nil {
		t.Fatalf("restore companion: %v", err)
	}
	if err := Run(t.Context(), genOpts, inputs); err != nil {
		t.Fatalf("regenerate: %v", err)
	}
	if err := Run(t.Context(), checkOpts, inputs); err != nil {
		t.Fatalf("check after regenerate: %v", err)
	}
}

// TestCheckSkipsMessageOutputWhenGenerationDoes covers legacy IR, where the
// messages are produced by another generator: generation writes no message
// file, so the .pb.go on disk is not ours to compare.
func TestCheckSkipsMessageOutputWhenGenerationDoes(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "legacy.ir.json")
	const irJSON = `{
  "ir_version": 1,
  "go_package": "example.com/legacy",
  "outputs": {"stub": "legacy.argos.go"},
  "services": [{"go_name": "LegacyService", "methods": []}]
}`
	if err := os.WriteFile(input, []byte(irJSON), 0o644); err != nil {
		t.Fatalf("write IR: %v", err)
	}
	inputs := []string{input}
	opts := Options{From: "ir"}
	if err := Run(t.Context(), opts, inputs); err != nil {
		t.Fatalf("generate: %v", err)
	}

	stubPath := filepath.Join(dir, "legacy.argos.go")
	msgPath := filepath.Join(dir, "legacy.pb.go")
	if err := os.WriteFile(msgPath, []byte("// generated elsewhere\n"), 0o644); err != nil {
		t.Fatalf("write unrelated message file: %v", err)
	}

	checkOpts := opts
	checkOpts.Check = stubPath
	if err := Run(t.Context(), checkOpts, inputs); err != nil {
		t.Fatalf("check compared the message file generation does not write: %v", err)
	}

	checkOpts.Check = msgPath
	if err := Run(t.Context(), checkOpts, inputs); err == nil {
		t.Fatal("check accepted a path generation does not write")
	}
}
