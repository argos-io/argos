package stubgen

import (
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/internal/codegen/gogenrun"
	"github.com/argos-io/argos/internal/codegen/ir"
)

func stubRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// compileStub generates file, drops it into a scratch package inside the
// repository module together with the message types it names, and compiles and
// runs it. A syntax parse is not enough: the generated file can reference an
// import it never uses, which only the type checker rejects.
func compileStub(t *testing.T, file ir.File, refs string) {
	t.Helper()
	source, err := Generate(file)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	root := stubRepoRoot(t)
	dir := gogenrun.DirName("stub-compile")
	if err := gogenrun.WriteFiles(root, dir, map[string]string{
		"sample/sample.argos.go": string(source),
		"sample/types.go":        "package sample\n\ntype Request struct{}\n\ntype Response struct{}\n\ntype Event struct{}\n",
		"main.go":                "package main\n\nimport (\n\t\"fmt\"\n\n\tsample \"github.com/argos-io/argos/" + dir + "/sample\"\n)\n\nfunc main() {\n" + refs + "}\n",
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gogenrun.Remove(root, dir) })

	out, err := gogenrun.Run(root, dir)
	if err != nil {
		t.Fatalf("generated stub does not compile/run: %v\n%s\n--- source ---\n%s", err, out, source)
	}
	if got := string(out); got != "ok\n" {
		t.Fatalf("checker output = %q, want %q", got, "ok\n")
	}
}

// A service whose methods are all unary is the most common shape there is. It
// references neither metadata (streaming clients only) nor errors/fmt/io/status
// (single-response wrappers only), so an unconditional import block produced a
// file that does not compile.
func TestGeneratedUnaryOnlyStubCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles generated code")
	}
	compileStub(t, ir.File{
		GoPackage:    "sample",
		ProtoPackage: "sample.v1",
		InputBase:    "sample",
		Services: []ir.Service{{
			GoName:   "OnlyUnaryService",
			FullName: "sample.v1.OnlyUnaryService",
			Methods: []ir.Method{{
				GoName: "Do", FullName: "sample.v1.OnlyUnaryService.Do",
				InputType: "Request", OutputType: "Response",
			}},
		}},
	}, "\t_ = sample.OnlyUnaryService_Do.FullName()\n\tfmt.Println(\"ok\")\n")
}

// A server-streaming-only service references metadata but none of the
// single-response wrapper imports.
func TestGeneratedServerStreamingOnlyStubCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles generated code")
	}
	compileStub(t, ir.File{
		GoPackage:    "sample",
		ProtoPackage: "sample.v1",
		InputBase:    "sample",
		Services: []ir.Service{{
			GoName:   "WatchService",
			FullName: "sample.v1.WatchService",
			Methods: []ir.Method{{
				GoName: "Watch", FullName: "sample.v1.WatchService.Watch",
				InputType: "Request", OutputType: "Event", ServerStream: true,
			}},
		}},
	}, "\t_ = sample.WatchService_Watch.FullName()\n\tfmt.Println(\"ok\")\n")
}
