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
	"github.com/argos-io/argos/internal/session"
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
			// A package with no production imports prints no separator; it
			// still belongs in the map, or the transport classification gate
			// could not report it by name.
			result[line] = nil
			continue
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

// transportPkgClass is how a package under transport/ is classified for the
// dependency gate. Classification is explicit and total: a package is its own
// entry or its nearest classified ancestor's, and anything unclassified fails
// the test rather than falling into a default bucket.
type transportPkgClass uint8

const (
	// interfaceTransport is the transport root: the interfaces and their
	// vocabulary. descriptor / metadata belong to the contract
	// (descriptor.Method, metadata.CallMetadata), so they are
	// allowed here and nowhere else in transport/ except the wire stacks.
	interfaceTransport transportPkgClass = iota
	// pipeTransport is a byte pipe (tcp, ws, udp, http1, http2): bytes and
	// frames only. It has no method and no metadata, so needing descriptor /
	// metadata means the pipe grew a second
	// responsibility; a session is a wire stack's business, not its own.
	pipeTransport
	// wireStackTransport is a finished wire stack (grpc, httpunary): bytes,
	// framing and session in one axis, so descriptor / metadata are
	// its vocabulary and internal/session is legal.
	wireStackTransport
)

// interfaceTransportPkg is the transport root package. It is not an axis, so it
// is classified on its own and deliberately does not act as an ancestor: were
// it in the table below, every unclassified axis would inherit its class and
// the "must be classified" gate could never fire.
const interfaceTransportPkg = modulePath + "/transport"

// classifiedTransportPkgs names every axis under transport/. A subpackage
// (transport/grpc/health) inherits its nearest classified ancestor.
var classifiedTransportPkgs = map[string]transportPkgClass{
	modulePath + "/transport/tcp":       pipeTransport,
	modulePath + "/transport/ws":        pipeTransport,
	modulePath + "/transport/udp":       pipeTransport,
	modulePath + "/transport/http1":     pipeTransport,
	modulePath + "/transport/http2":     pipeTransport,
	modulePath + "/transport/grpc":      wireStackTransport,
	modulePath + "/transport/httpunary": wireStackTransport,
}

// transportForbiddenImports is the dependency gate per class. codec is
// forbidden in all of them: an axis is Axis × Codec, and importing codec is
// what would collapse the two into one.
var transportForbiddenImports = map[transportPkgClass][]string{
	interfaceTransport: {
		modulePath + "/codec",
		modulePath + "/internal/session",
		"google.golang.org/grpc",
		"google.golang.org/genproto",
	},
	pipeTransport: {
		modulePath + "/descriptor",
		modulePath + "/metadata",
		modulePath + "/budget",
		modulePath + "/codec",
		modulePath + "/internal/session",
		"google.golang.org/grpc",
		"google.golang.org/genproto",
	},
	wireStackTransport: {
		modulePath + "/codec",
	},
}

// classifyTransportPkg returns the class of a package under transport/:
// interfaceTransport for the root, otherwise the nearest classified axis
// ancestor. ok is false for every other package — an axis nobody classified
// included — and for packages outside transport/, which the caller skips.
func classifyTransportPkg(importPath string) (transportPkgClass, bool) {
	if importPath == interfaceTransportPkg {
		return interfaceTransport, true
	}
	for p := importPath; strings.HasPrefix(p, modulePath+"/transport/"); p = p[:strings.LastIndex(p, "/")] {
		if c, ok := classifiedTransportPkgs[p]; ok {
			return c, true
		}
	}
	return 0, false
}

func (c transportPkgClass) String() string {
	switch c {
	case interfaceTransport:
		return "interfaceTransport"
	case pipeTransport:
		return "pipeTransport"
	case wireStackTransport:
		return "wireStackTransport"
	default:
		return fmt.Sprintf("transportPkgClass(%d)", uint8(c))
	}
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

	// The axis owns bytes and frames. Which vocabulary it may use depends on
	// what it is: a byte pipe moves bytes, a wire stack also carries sessions
	// and so deals in descriptors, metadata and budgets. codec stays forbidden
	// everywhere: a protocol is Axis × Codec, and this import is what would
	// collapse the two into one.
	t.Run("transport_packages_classified", func(t *testing.T) {
		t.Parallel()
		// A new axis must be named in classifiedTransportPkgs. Falling back to
		// a default bucket would let it inherit the wrong contract silently —
		// and the wrong contract here is a weaker dependency gate.
		for pkg := range pkgs {
			if pkg == interfaceTransportPkg || !strings.HasPrefix(pkg, modulePath+"/transport/") {
				continue
			}
			if _, ok := classifyTransportPkg(pkg); !ok {
				t.Errorf("transport package %s is unclassified; add it to classifiedTransportPkgs "+
					"as a byte pipe (pipeTransport) or a full wire stack (wireStackTransport)", pkg)
			}
		}
		// The table must not rot either: a named axis that no longer exists
		// means the gate is checking a package nobody ships.
		for pkg := range classifiedTransportPkgs {
			if _, ok := pkgs[pkg]; !ok {
				t.Errorf("classifiedTransportPkgs names %s, which no longer exists; drop the entry", pkg)
			}
		}
	})

	t.Run("transport_no_session_codec_grpc", func(t *testing.T) {
		t.Parallel()
		for pkg, imports := range pkgs {
			class, ok := classifyTransportPkg(pkg)
			if !ok {
				continue // not a transport package; transport_packages_classified reports the rest
			}
			for _, forbid := range transportForbiddenImports[class] {
				if hasImport(imports, forbid) {
					t.Errorf("%s imports %q; §3.1-2 forbids it for class %s", pkg, forbid, class)
				}
			}
		}
	})

	t.Run("session_no_stream_client_server_compressor", func(t *testing.T) {
		t.Parallel()
		imports := pkgs[modulePath+"/internal/session"]
		for _, forbid := range []string{
			modulePath + "/stream",
			modulePath + "/client",
			modulePath + "/server",
			modulePath + "/compressor",
		} {
			if hasExactImport(imports, forbid) || hasImport(imports, forbid+"/") {
				t.Errorf("internal/session imports %q; forbidden", forbid)
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

	t.Run("codec_no_transport_session", func(t *testing.T) {
		t.Parallel()
		for pkg, imports := range pkgs {
			if pkg != modulePath+"/codec" && !strings.HasPrefix(pkg, modulePath+"/codec/") {
				continue
			}
			for _, forbid := range []string{
				modulePath + "/transport",
				modulePath + "/internal/session",
			} {
				if hasExactImport(imports, forbid) || hasImport(imports, forbid+"/") {
					t.Errorf("%s imports %q; §3.1-2 forbids it", pkg, forbid)
				}
			}
		}
	})

	// Pooling is a client-side concern and under the current architecture it
	// lives inside the axes: a byte pipe has nothing to pool above one
	// connection, so the full wire stacks (grpc, httpunary) and the examples'
	// own stacks hold the helper, client owns instance-level admission and
	// leaves the pool to the axis it is handed, internal/transportbind maps
	// pool settings onto the public type, and internal/fake is an axis test
	// double. The server never pools — it accepts.
	t.Run("only_links_import_sessionpool", func(t *testing.T) {
		t.Parallel()
		const pool = modulePath + "/internal/sessionpool"
		allowed := map[string]bool{
			modulePath + "/example/resp":           true,
			modulePath + "/example/synth":          true,
			modulePath + "/internal/fake":          true, // in-process axis test double
			modulePath + "/internal/transportbind": true, // maps pool settings onto the public type
			modulePath + "/transport/httpunary":    true,
			modulePath + "/transport/grpc":         true,
		}
		imports := make(map[string]bool, len(allowed))
		for pkg, pkgImports := range pkgs {
			if !hasExactImport(pkgImports, pool) {
				continue
			}
			imports[pkg] = true
			if !allowed[pkg] {
				t.Errorf("%s imports internal/sessionpool; only a client-side axis may pool (§3.1-16)", pkg)
			}
		}
		// An allow-list entry that stopped importing the helper is a stale
		// exception: drop it, so the list keeps naming who really pools.
		for pkg := range allowed {
			if !imports[pkg] {
				t.Errorf("%s no longer imports internal/sessionpool; drop it from the allow-list", pkg)
			}
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

// TestInvariantTransitiveRespTCPNoGRPC is Task 2.7 / §3.1-15:
// example/resp + transport/tcp must not transitively depend on gRPC framing,
// binding, compressor, or genproto.
func TestInvariantTransitiveRespTCPNoGRPC(t *testing.T) {
	t.Parallel()
	assertTransitiveNoGRPC(t, "resp+tcp", "./example/resp", "./transport/tcp")
}

// TestInvariantTransitiveTransportUDPNoGenproto is Task 4.2:
// transport/udp alone must not pull genproto (same neutrality gate as tcp stacks).
func TestInvariantTransitiveTransportUDPNoGenproto(t *testing.T) {
	t.Parallel()
	assertTransitiveNoGRPC(t, "transport/udp", "./transport/udp")
}

// TestInvariantTransitiveHTTPUnaryHTTP1NoGRPC is Task 6.3 / §9-2:
// httpunary + http1 must not transitively depend on gRPC framing, binding,
// compressor, or genproto (same neutrality gate as resp+tcp).
func TestInvariantTransitiveHTTPUnaryHTTP1NoGRPC(t *testing.T) {
	t.Parallel()
	assertTransitiveNoGRPC(t, "httpunary+http1", "./transport/httpunary", "./transport/http1")
}

func assertTransitiveNoGRPC(t *testing.T, label string, patterns ...string) {
	t.Helper()
	args := append([]string{"list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}"}, patterns...)
	cmd := exec.Command("go", args...)
	cmd.Dir = moduleRoot(t)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps %s: %v\n%s", label, err, stderr.String())
	}
	forbidden := []string{
		modulePath + "/transport/grpc",
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
				t.Errorf("%s transitive deps include %q (§3.1-15 / §9-2)", label, line)
			}
		}
	}
}

func TestInvariantFramingOptionsNoCompression(t *testing.T) {
	t.Parallel()
	compressField := regexp.MustCompile(`(?i)compress`)
	typ := reflect.TypeOf(session.Options{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if compressField.MatchString(name) {
			t.Fatalf("Options field %q matches (?i)compress; §3.1-10 forbids compression in session.Options", name)
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

// TestInvariantCompositionNoConcreteProtocolNames is task 7.6 / §3.1-12 / §3.1-19:
// client, server, and internal/sessionpool production sources must not name
// concrete Framing/Transport protocols or example gate packages.
func TestInvariantCompositionNoConcreteProtocolNames(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	dirs := []string{
		filepath.Join(root, "client"),
		filepath.Join(root, "server"),
		filepath.Join(root, "internal", "sessionpool"),
	}
	// Word-boundary tokens: avoid matching "Response" for "resp", etc.
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`(?i)\bgrpc\b`),
		regexp.MustCompile(`(?i)\bhttpunary\b`),
		regexp.MustCompile(`(?i)\bresp\b`),
		regexp.MustCompile(`(?i)\bsynth\b`),
		regexp.MustCompile(`(?i)\bhttp2\b`),
		regexp.MustCompile(`example/resp`),
		regexp.MustCompile(`example/synth`),
	}
	fset := token.NewFileSet()
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// Scan identifiers and string literals via AST (not comments).
			file, err := parser.ParseFile(fset, path, src, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				var text string
				switch x := n.(type) {
				case *ast.Ident:
					text = x.Name
				case *ast.BasicLit:
					if x.Kind == token.STRING {
						text = x.Value
					}
				case *ast.ImportSpec:
					if x.Path != nil {
						text = x.Path.Value
					}
				default:
					return true
				}
				if text == "" {
					return true
				}
				for _, re := range forbidden {
					if re.MatchString(text) {
						pos := fset.Position(n.Pos())
						t.Errorf("%s: composition layer must not reference %q (matched %s)", pos, text, re.String())
					}
				}
				return true
			})
		}
	}
}

// TestInvariantCorePackagesNoExampleImport restates §3.1-19 via go list
// (also covered inside TestInvariantDependencyTable; kept explicit for 7.6).
func TestInvariantCorePackagesNoExampleImport(t *testing.T) {
	t.Parallel()
	pkgs := repoPackages(t)
	for pkg, imports := range pkgs {
		if !isCorePackage(pkg) {
			continue
		}
		for _, imp := range imports {
			if strings.HasPrefix(imp, modulePath+"/example") {
				t.Errorf("%s imports %q; core packages must not import example/* (§3.1-19 / 7.6)", pkg, imp)
			}
		}
	}
}

// TestZeroCoreAPIChangesEvidence ensures the assembly-gate packages keep a
// committed 7.2b evidence note (zero core public-interface changes).
func TestZeroCoreAPIChangesEvidence(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	for _, rel := range []string{
		"example/resp/ASSEMBLY.md",
		"example/synth/ASSEMBLY.md",
	} {
		path := filepath.Join(root, rel)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("missing 7.2b evidence file %s: %v", rel, err)
		}
		body := string(data)
		for _, need := range []string{"Zero core API", "transport/", "internal/session", "client/", "server/"} {
			if !strings.Contains(body, need) {
				t.Errorf("%s: missing required evidence phrase %q", rel, need)
			}
		}
	}
}
