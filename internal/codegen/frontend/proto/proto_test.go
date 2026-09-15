package proto

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/codegen/ir"
)

func TestParseMethodFullNameWithoutProtoPackage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "echo.proto")
	const source = `syntax = "proto3";
option go_package = "example";

message Request { string msg = 1; }
message Response { string msg = 1; }
service Echo {
  rpc Say(Request) returns (Response);
}
`
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	files, err := (Frontend{ImportPaths: []string{dir}}).Parse(context.Background(), []string{path})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	svc := files[0].Services[0]
	if got, want := svc.FullName, "Echo"; got != want {
		t.Fatalf("Service.FullName = %q, want %q", got, want)
	}
	got := svc.Methods[0].FullName
	if got != "Echo.Say" {
		t.Fatalf("Method.FullName = %q, want Echo.Say", got)
	}
	if got, want := svc.Methods[0].Shape, ir.Unary; got != want {
		t.Fatalf("Shape = %v, want %v", got, want)
	}
}

func TestParseEchoFullNameAndShape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "echo.proto")
	const source = `syntax = "proto3";
package echo.v1;
option go_package = "echov1";

message EchoRequest { string msg = 1; }
message EchoResponse { string msg = 1; }
message WatchRequest { string msg = 1; }
message Event { string msg = 1; }
message CollectRequest { string msg = 1; }
message ChatRequest { string msg = 1; }
message ChatResponse { string msg = 1; }

service EchoService {
  rpc Echo(EchoRequest) returns (EchoResponse);
  rpc Watch(WatchRequest) returns (stream Event);
  rpc Collect(stream CollectRequest) returns (EchoResponse);
  rpc Chat(stream ChatRequest) returns (stream ChatResponse);
}
`
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	files, err := (Frontend{ImportPaths: []string{dir}}).Parse(context.Background(), []string{path})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	svc := files[0].Services[0]
	if got, want := svc.FullName, "echo.v1.EchoService"; got != want {
		t.Fatalf("Service.FullName = %q, want %q", got, want)
	}

	wantMethods := []struct {
		goName   string
		fullName string
		shape    ir.Shape
	}{
		{"Echo", "echo.v1.EchoService.Echo", ir.Unary},
		{"Watch", "echo.v1.EchoService.Watch", ir.ServerStreaming},
		{"Collect", "echo.v1.EchoService.Collect", ir.ClientStreaming},
		{"Chat", "echo.v1.EchoService.Chat", ir.BidiStreaming},
	}
	if len(svc.Methods) != len(wantMethods) {
		t.Fatalf("methods = %d, want %d", len(svc.Methods), len(wantMethods))
	}
	for i, want := range wantMethods {
		m := svc.Methods[i]
		if m.GoName != want.goName {
			t.Errorf("method[%d].GoName = %q, want %q", i, m.GoName, want.goName)
		}
		if m.FullName != want.fullName {
			t.Errorf("method[%d].FullName = %q, want %q", i, m.FullName, want.fullName)
		}
		if m.Shape != want.shape {
			t.Errorf("method[%d].Shape = %v, want %v", i, m.Shape, want.shape)
		}
		if m.Shape != descriptor.Shape(want.shape) {
			t.Errorf("method[%d].Shape not aligned with descriptor.Shape", i)
		}
		client, server := ir.StreamsFromShape(m.Shape)
		if m.ClientStream != client || m.ServerStream != server {
			t.Errorf("method[%d] stream flags = client=%v server=%v, want %v %v",
				i, m.ClientStream, m.ServerStream, client, server)
		}
	}
}
