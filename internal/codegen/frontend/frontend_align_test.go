package frontend_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/argos-io/argos/internal/codegen/frontend/execplugin"
	"github.com/argos-io/argos/internal/codegen/frontend/irjson"
	"github.com/argos-io/argos/internal/codegen/frontend/proto"
	"github.com/argos-io/argos/internal/codegen/ir"
)

// echoDesc is the golden FullName + Shape contract shared by all three frontends.
type echoDesc struct {
	serviceFullName string
	methods         []struct {
		goName   string
		fullName string
		shape    ir.Shape
	}
}

func wantEcho() echoDesc {
	return echoDesc{
		serviceFullName: "echo.v1.EchoService",
		methods: []struct {
			goName   string
			fullName string
			shape    ir.Shape
		}{
			{"Echo", "echo.v1.EchoService.Echo", ir.Unary},
			{"Watch", "echo.v1.EchoService.Watch", ir.ServerStreaming},
		},
	}
}

func assertEchoDesc(t *testing.T, frontendName string, file ir.File) {
	t.Helper()
	want := wantEcho()
	if len(file.Services) != 1 {
		t.Fatalf("%s: services = %d, want 1", frontendName, len(file.Services))
	}
	svc := file.Services[0]
	if svc.GoName != "EchoService" {
		t.Fatalf("%s: GoName = %q, want EchoService", frontendName, svc.GoName)
	}
	if svc.FullName != want.serviceFullName {
		t.Fatalf("%s: Service.FullName = %q, want %q", frontendName, svc.FullName, want.serviceFullName)
	}
	if len(svc.Methods) < len(want.methods) {
		t.Fatalf("%s: methods = %d, want at least %d", frontendName, len(svc.Methods), len(want.methods))
	}
	byName := make(map[string]ir.Method, len(svc.Methods))
	for _, m := range svc.Methods {
		byName[m.GoName] = m
	}
	for _, wm := range want.methods {
		m, ok := byName[wm.goName]
		if !ok {
			t.Fatalf("%s: missing method %q", frontendName, wm.goName)
		}
		if m.FullName != wm.fullName {
			t.Errorf("%s: %s.FullName = %q, want %q", frontendName, wm.goName, m.FullName, wm.fullName)
		}
		if m.Shape != wm.shape {
			t.Errorf("%s: %s.Shape = %v, want %v", frontendName, wm.goName, m.Shape, wm.shape)
		}
	}
}

func TestFrontendsAgreeEchoFullNameAndShape(t *testing.T) {
	dir := t.TempDir()
	protoPath := filepath.Join(dir, "echo.proto")
	const protoSource = `syntax = "proto3";
package echo.v1;
option go_package = "echov1";

message EchoRequest { string msg = 1; }
message EchoResponse { string msg = 1; }
message WatchRequest { string msg = 1; }
message Event { string msg = 1; }

service EchoService {
  rpc Echo(EchoRequest) returns (EchoResponse);
  rpc Watch(WatchRequest) returns (stream Event);
}
`
	if err := os.WriteFile(protoPath, []byte(protoSource), 0o600); err != nil {
		t.Fatalf("WriteFile proto: %v", err)
	}

	protoFiles, err := (proto.Frontend{ImportPaths: []string{dir}}).Parse(context.Background(), []string{protoPath})
	if err != nil {
		t.Fatalf("proto Parse: %v", err)
	}
	protoFiles[0].Normalize(false)
	assertEchoDesc(t, "proto", protoFiles[0])

	irPath := filepath.Join("irjson", "testdata", "echo.ir.json")
	irFiles, err := irjson.Frontend{}.Parse(context.Background(), []string{irPath})
	if err != nil {
		t.Fatalf("irjson Parse: %v", err)
	}
	assertEchoDesc(t, "irjson", irFiles[0])

	scriptName := "fake-emit-ir.sh"
	if runtime.GOOS == "windows" {
		scriptName = "fake-emit-ir.cmd"
	}
	script, err := filepath.Abs(filepath.Join("execplugin", "testdata", scriptName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	pluginFiles, err := execplugin.Frontend{Command: script}.Parse(context.Background(), []string{"ignored.thrift"})
	if err != nil {
		t.Fatalf("execplugin Parse: %v", err)
	}
	assertEchoDesc(t, "execplugin", pluginFiles[0])

	// Identical FullName + Shape across frontends for Echo and Watch.
	type key struct{ fullName string; shape ir.Shape }
	collect := func(file ir.File) map[string]key {
		out := make(map[string]key)
		for _, m := range file.Services[0].Methods {
			if m.GoName == "Echo" || m.GoName == "Watch" {
				out[m.GoName] = key{m.FullName, m.Shape}
			}
		}
		return out
	}
	p, j, e := collect(protoFiles[0]), collect(irFiles[0]), collect(pluginFiles[0])
	for _, name := range []string{"Echo", "Watch"} {
		if p[name] != j[name] || j[name] != e[name] {
			t.Fatalf("frontends disagree on %s: proto=%+v irjson=%+v execplugin=%+v",
				name, p[name], j[name], e[name])
		}
	}
}
