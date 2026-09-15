package argos_test

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
)

const modulePath = "github.com/argos-io/argos"

// productionImports runs `go list` for ImportPath → production Imports only
// (TestImports are excluded — §9-2).
func productionImports(t *testing.T, patterns ...string) map[string][]string {
	t.Helper()
	args := append([]string{"list", "-f", "{{.ImportPath}}\t{{range .Imports}}{{.}},{{end}}"}, patterns...)
	cmd := exec.Command("go", args...)
	cmd.Dir = moduleRoot(t)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %v: %v\n%s", patterns, err, stderr.String())
	}
	result := make(map[string][]string)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		pkg, imports, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("unexpected go list line: %q", line)
		}
		var list []string
		for _, imp := range strings.Split(imports, ",") {
			if imp == "" {
				continue
			}
			list = append(list, imp)
		}
		result[pkg] = list
	}
	return result
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("go", "env", "GOMOD")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" || mod == "/dev/null" {
		t.Fatal("not in a Go module")
	}
	return filepath.Dir(mod)
}

func hasImport(imports []string, substr string) bool {
	for _, imp := range imports {
		if strings.Contains(imp, substr) {
			return true
		}
	}
	return false
}

func hasExactImport(imports []string, want string) bool {
	for _, imp := range imports {
		if imp == want {
			return true
		}
	}
	return false
}

func repoPackages(t *testing.T) map[string][]string {
	t.Helper()
	return productionImports(t, "./...")
}

func isCorePackage(importPath string) bool {
	if !strings.HasPrefix(importPath, modulePath) {
		return false
	}
	rest := strings.TrimPrefix(importPath, modulePath)
	if rest == "" {
		return true
	}
	if !strings.HasPrefix(rest, "/") {
		return false
	}
	first := strings.Split(strings.TrimPrefix(rest, "/"), "/")[0]
	switch first {
	case "probe", "example", "internal":
		// internal/fake is a test helper; still core enough to forbid probe/example.
		// probe and example themselves are excluded from "core" subject set.
		if first == "probe" || first == "example" {
			return false
		}
		return true
	default:
		return true
	}
}

// TestInvariantDependencyTable covers the practical §3.1 dependency edges
// required by Task 1.16.
func TestInvariantDependencyTable(t *testing.T) {
	t.Parallel()
	pkgs := repoPackages(t)

	t.Run("transport_no_descriptor_framing_codec_grpc", func(t *testing.T) {
		t.Parallel()
		for pkg, imports := range pkgs {
			if pkg != modulePath+"/transport" && !strings.HasPrefix(pkg, modulePath+"/transport/") {
				continue
			}
			for _, forbid := range []string{
				modulePath + "/descriptor",
				modulePath + "/framing",
				modulePath + "/codec",
				"google.golang.org/grpc",
				"google.golang.org/genproto",
			} {
				if hasImport(imports, forbid) {
					t.Errorf("%s imports %q; §3.1-2 forbids it", pkg, forbid)
				}
			}
		}
	})

	t.Run("framing_no_stream_client_server_compressor", func(t *testing.T) {
		t.Parallel()
		imports := pkgs[modulePath+"/framing"]
		for _, forbid := range []string{
			modulePath + "/stream",
			modulePath + "/client",
			modulePath + "/server",
			modulePath + "/compressor",
		} {
			if hasExactImport(imports, forbid) || hasImport(imports, forbid+"/") {
				t.Errorf("framing imports %q; forbidden", forbid)
			}
		}
	})

	t.Run("status_no_protobuf_genproto", func(t *testing.T) {
		t.Parallel()
		imports := pkgs[modulePath+"/status"]
		for _, forbid := range []string{
			"google.golang.org/protobuf",
			"google.golang.org/genproto",
			"github.com/golang/protobuf",
		} {
			if hasImport(imports, forbid) {
				t.Errorf("status imports %q; §3.1-7 forbids protobuf/genproto", forbid)
			}
		}
	})

	t.Run("codec_no_transport_framing", func(t *testing.T) {
		t.Parallel()
		for pkg, imports := range pkgs {
			if pkg != modulePath+"/codec" && !strings.HasPrefix(pkg, modulePath+"/codec/") {
				continue
			}
			for _, forbid := range []string{
				modulePath + "/transport",
				modulePath + "/framing",
			} {
				if hasExactImport(imports, forbid) || hasImport(imports, forbid+"/") {
					t.Errorf("%s imports %q; §3.1-2 forbids it", pkg, forbid)
				}
			}
		}
	})

	t.Run("only_client_imports_sessionpool", func(t *testing.T) {
		t.Parallel()
		const pool = modulePath + "/internal/sessionpool"
		var importers []string
		for pkg, imports := range pkgs {
			if hasExactImport(imports, pool) {
				importers = append(importers, pkg)
			}
		}
		if len(importers) != 1 || importers[0] != modulePath+"/client" {
			t.Errorf("sessionpool importers = %v; want only %s/client (§3.1-16)", importers, modulePath)
		}
		if hasExactImport(pkgs[modulePath+"/server"], pool) {
			t.Error("server must not import internal/sessionpool")
		}
	})

	t.Run("core_no_example_or_probe", func(t *testing.T) {
		t.Parallel()
		for pkg, imports := range pkgs {
			if !isCorePackage(pkg) {
				continue
			}
			for _, imp := range imports {
				if strings.HasPrefix(imp, modulePath+"/example") || strings.HasPrefix(imp, modulePath+"/probe") {
					t.Errorf("%s imports %q; core packages must not depend on example/ or probe/ (§3.1-19)", pkg, imp)
				}
			}
		}
	})

	t.Run("argos_root_no_transport_implementations", func(t *testing.T) {
		t.Parallel()
		imports := pkgs[modulePath]
		if !hasExactImport(imports, modulePath+"/transport") {
			t.Errorf("argos root should import %s/transport (interface package)", modulePath)
		}
		for _, imp := range imports {
			if strings.HasPrefix(imp, modulePath+"/transport/") {
				t.Errorf("argos root imports transport implementation %q; only the interface package is allowed", imp)
			}
		}
	})

	t.Run("argos_root_no_compressor", func(t *testing.T) {
		t.Parallel()
		imports := pkgs[modulePath]
		if hasImport(imports, modulePath+"/compressor") {
			t.Error("argos root must not import compressor (§3.1-10)")
		}
	})

	t.Run("filter_only_descriptor_stream", func(t *testing.T) {
		t.Parallel()
		imports := pkgs[modulePath+"/filter"]
		for _, imp := range imports {
			if !strings.HasPrefix(imp, modulePath+"/") {
				continue
			}
			switch imp {
			case modulePath + "/descriptor", modulePath + "/stream":
			default:
				t.Errorf("filter imports repo package %q; §3.1-4 allows only descriptor and stream", imp)
			}
		}
	})
}

// TestInvariantClientDepsNoProbe ensures client production deps still exclude probe/.
func TestInvariantClientDepsNoProbe(t *testing.T) {
	t.Parallel()
	cmd := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./client")
	cmd.Dir = moduleRoot(t)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps ./client: %v\n%s", err, stderr.String())
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, modulePath+"/probe") {
			t.Errorf("client transitive deps include %q", line)
		}
	}
}

// TestInvariantTransitiveEnvelopeTCPNoGRPC is Task 2.7 / §3.1-15:
// a program that only imports framing/envelope + transport/tcp must not
// transitively depend on gRPC framing, binding, compressor, or genproto.
func TestInvariantTransitiveEnvelopeTCPNoGRPC(t *testing.T) {
	t.Parallel()
	cmd := exec.Command(
		"go", "list", "-deps",
		"-f", "{{if not .Standard}}{{.ImportPath}}{{end}}",
		"./framing/envelope", "./transport/tcp",
	)
	cmd.Dir = moduleRoot(t)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps envelope+tcp: %v\n%s", err, stderr.String())
	}
	forbidden := []string{
		modulePath + "/framing/grpc",
		modulePath + "/binding/grpc",
		modulePath + "/compressor",
		"google.golang.org/genproto",
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		for _, bad := range forbidden {
			if line == bad || strings.HasPrefix(line, bad+"/") {
				t.Errorf("envelope+tcp transitive deps include %q (§3.1-15)", line)
			}
		}
	}
}

// TestInvariantTransitiveEnvelopeUDPNoGenproto is Task 4.2:
// framing/envelope + transport/udp must not pull genproto (same gate as tcp).
func TestInvariantTransitiveEnvelopeUDPNoGenproto(t *testing.T) {
	t.Parallel()
	cmd := exec.Command(
		"go", "list", "-deps",
		"-f", "{{if not .Standard}}{{.ImportPath}}{{end}}",
		"./framing/envelope", "./transport/udp",
	)
	cmd.Dir = moduleRoot(t)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps envelope+udp: %v\n%s", err, stderr.String())
	}
	forbidden := []string{
		modulePath + "/framing/grpc",
		modulePath + "/binding/grpc",
		modulePath + "/compressor",
		"google.golang.org/genproto",
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		for _, bad := range forbidden {
			if line == bad || strings.HasPrefix(line, bad+"/") {
				t.Errorf("envelope+udp transitive deps include %q (§3.1-15)", line)
			}
		}
	}
}

func TestInvariantFramingConfigNoCompression(t *testing.T) {
	t.Parallel()
	compressField := regexp.MustCompile(`(?i)compress`)
	typ := reflect.TypeOf(framing.Config{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if compressField.MatchString(name) {
			t.Fatalf("Config field %q matches (?i)compress; §3.1-10 forbids compression in framing.Config", name)
		}
	}
}

func TestInvariantDescriptorFieldsUnexported(t *testing.T) {
	t.Parallel()
	for _, v := range []any{descriptor.Method{}, descriptor.Service{}} {
		typ := reflect.TypeOf(v)
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.PkgPath == "" {
				t.Errorf("%s.%s is exported; §3.1-14 requires unexported fields", typ.Name(), f.Name)
			}
		}
	}
}

func TestInvariantSessionpoolBucketKey(t *testing.T) {
	t.Parallel()
	pkgs := productionImports(t, "./internal/sessionpool")
	imports := pkgs[modulePath+"/internal/sessionpool"]
	if hasExactImport(imports, modulePath+"/metadata") || hasImport(imports, modulePath+"/metadata/") {
		t.Fatal("sessionpool imports metadata; §3.1-20 bucket keys must not incorporate call-scoped metadata")
	}

	// Source heuristic: bucket map must be keyed by string (endpoint), and
	// production sources must not mention metadata as a map-key ingredient.
	root := moduleRoot(t)
	dir := filepath.Join(root, "internal", "sessionpool")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(string(src))
		if strings.Contains(lower, "metadata") {
			t.Errorf("%s mentions metadata; session identity must not use call-scoped values (§3.1-20)", path)
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || ts.Name == nil || ts.Name.Name != "Pool" {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				for _, name := range field.Names {
					if name.Name != "buckets" {
						continue
					}
					mt, ok := field.Type.(*ast.MapType)
					if !ok {
						t.Errorf("Pool.buckets is %T, want map", field.Type)
						continue
					}
					ident, ok := mt.Key.(*ast.Ident)
					if !ok || ident.Name != "string" {
						t.Errorf("Pool.buckets key type = %v; want string (endpoint-only §3.1-20)", mt.Key)
					}
				}
			}
			return true
		})
	}
}

func TestInvariantClientOpenNoSessionAffinity(t *testing.T) {
	t.Parallel()
	m, ok := reflect.TypeOf(&client.Client{}).MethodByName("Open")
	if !ok {
		t.Fatal("client.Client.Open not found")
	}
	ft := m.Type
	// recv, ctx, Method
	if ft.NumIn() != 3 {
		t.Fatalf("Open arity = %d; want 3 (recv, ctx, Method) — no session-affinity params (§3.1-21)", ft.NumIn())
	}
	if ft.In(1).String() != "context.Context" {
		t.Fatalf("Open 1st param = %s; want context.Context", ft.In(1))
	}
	if ft.In(2) != reflect.TypeOf(descriptor.Method{}) {
		t.Fatalf("Open 2nd param = %s; want descriptor.Method", ft.In(2))
	}

	// No other exported Client method may accept an affinity / session-handle hint.
	affinity := regexp.MustCompile(`(?i)(affinity|sessionid|sessionkey|sticky|pin)`)
	ct := reflect.TypeOf(&client.Client{})
	for i := 0; i < ct.NumMethod(); i++ {
		meth := ct.Method(i)
		if affinity.MatchString(meth.Name) {
			t.Errorf("exported Client method %q looks like session affinity (§3.1-21)", meth.Name)
		}
		for j := 1; j < meth.Type.NumIn(); j++ { // skip receiver
			p := meth.Type.In(j)
			name := p.String()
			if affinity.MatchString(name) {
				t.Errorf("Client.%s param %s looks like session affinity (§3.1-21)", meth.Name, name)
			}
		}
	}
}

func TestInvariantNotifyConnError(t *testing.T) {
	t.Parallel()

	// API exists and is callable (light contract for §3.1-22).
	argos.NotifyConnError(nil, argos.ConnInfo{}, fmt.Errorf("noop"))

	fnType := reflect.TypeOf(argos.NotifyConnError)
	if fnType.Kind() != reflect.Func {
		t.Fatal("NotifyConnError is not a function")
	}

	root := moduleRoot(t)
	serverDir := filepath.Join(root, "server")
	entries, err := os.ReadDir(serverDir)
	if err != nil {
		t.Fatal(err)
	}
	var hits int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(serverDir, name))
		if err != nil {
			t.Fatal(err)
		}
		hits += strings.Count(string(src), "NotifyConnError")
	}
	if hits == 0 {
		t.Fatal("server production sources never call NotifyConnError; §3.1-22 requires the server path to report conn-level errors")
	}
}
