package transport_test

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/argos-io/argos/transport"
)

// Compile-time assertions that interface embeddings match §2.1.
var (
	_ transport.Carrier = (transport.ByteStreamCarrier)(nil)
	_ transport.Carrier = (transport.MessageCarrier)(nil)
	_ transport.Carrier = (transport.DatagramCarrier)(nil)
	_ transport.Carrier = (transport.SendCloser)(nil)
	_ transport.Carrier = (transport.RequestHeaderReader)(nil)
	_ transport.Carrier = (transport.ResponseHeaderReader)(nil)
	_ transport.Carrier = (transport.ResponseTrailerReader)(nil)
	_ transport.Carrier = (transport.ResponseWriter)(nil)
	_ transport.Carrier = (transport.UnaryResponseWriter)(nil)
	_ transport.Conn    = (transport.CarrierConn)(nil)
	_ transport.Conn    = (transport.StreamConn)(nil)
)

func TestNoForbiddenImports(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.Imports}}", "github.com/argos-io/argos/transport").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	imports := string(out)
	// The axis owns frames, so descriptor / metadata / budget are its own
	// vocabulary. codec stays forbidden: message bodies are the other axis,
	// and that import is what would make "Axis × Codec" a single axis.
	forbidden := []string{
		"github.com/argos-io/argos/internal/session",
		"github.com/argos-io/argos/codec",
		"google.golang.org/grpc",
	}
	for _, f := range forbidden {
		if strings.Contains(imports, f) {
			t.Fatalf("transport must not import %q; go list Imports = %s", f, imports)
		}
	}

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if f.Name.Name != "transport" {
			t.Fatalf("%s: package name = %q, want transport", path, f.Name.Name)
		}
		for _, imp := range f.Imports {
			ipath := strings.Trim(imp.Path.Value, `"`)
			for _, forb := range forbidden {
				if ipath == forb || strings.HasPrefix(ipath, forb+"/") {
					t.Fatalf("%s imports forbidden %q", name, ipath)
				}
			}
		}
	}
}
