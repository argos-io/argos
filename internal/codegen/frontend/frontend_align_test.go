package frontend_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/argos-io/argos/internal/codegen/frontend/execplugin"
	"github.com/argos-io/argos/internal/codegen/frontend/irjson"
	"github.com/argos-io/argos/internal/codegen/frontend/proto"
	"github.com/argos-io/argos/internal/codegen/ir"
)

// surface is the descriptor-relevant projection of an ir.File: every field the
// stub generator turns into descriptor.Service / descriptor.Method, plus the
// message set the generated message code is derived from. Frontend-specific
// metadata (output file names, source kind, raw descriptors) is deliberately
// left out — frontends are not required to agree on it.
type surface struct {
	ProtoPackage string
	Service      serviceSurface
	Messages     []messageSurface
}

type serviceSurface struct {
	GoName   string
	FullName string
	Methods  []methodSurface
}

type methodSurface struct {
	GoName       string
	FullName     string
	InputType    string
	OutputType   string
	Shape        ir.Shape
	ClientStream bool
	ServerStream bool
}

type messageSurface struct {
	GoName string
	Fields []fieldSurface
}

type fieldSurface struct {
	GoName      string
	Number      int32
	Kind        string
	MessageType string
	EnumType    string
}

// wantEcho is the golden descriptor surface shared by all three frontends.
func wantEcho() surface {
	return surface{
		ProtoPackage: "echo.v1",
		Service: serviceSurface{
			GoName:   "EchoService",
			FullName: "echo.v1.EchoService",
			Methods: []methodSurface{
				{
					GoName:     "Echo",
					FullName:   "echo.v1.EchoService.Echo",
					InputType:  "EchoRequest",
					OutputType: "EchoResponse",
					Shape:      ir.Unary,
				},
				{
					GoName:       "Watch",
					FullName:     "echo.v1.EchoService.Watch",
					InputType:    "WatchRequest",
					OutputType:   "Event",
					Shape:        ir.ServerStreaming,
					ServerStream: true,
				},
			},
		},
		Messages: []messageSurface{
			{GoName: "EchoRequest", Fields: []fieldSurface{{GoName: "Msg", Number: 1, Kind: "string"}}},
			{GoName: "EchoResponse", Fields: []fieldSurface{{GoName: "Msg", Number: 1, Kind: "string"}}},
			{GoName: "WatchRequest", Fields: []fieldSurface{{GoName: "Msg", Number: 1, Kind: "string"}}},
			{GoName: "Event", Fields: []fieldSurface{{GoName: "Msg", Number: 1, Kind: "string"}}},
		},
	}
}

// surfaceOf projects file onto the comparable surface. Callers must have
// checked that file holds exactly one service. Slices are always non-nil so
// that reflect.DeepEqual does not distinguish "absent" from "empty".
func surfaceOf(file ir.File) surface {
	out := surface{
		ProtoPackage: file.ProtoPackage,
		Messages:     make([]messageSurface, 0, len(file.Messages)),
	}
	svc := file.Services[0]
	service := serviceSurface{
		GoName:   svc.GoName,
		FullName: svc.FullName,
		Methods:  make([]methodSurface, 0, len(svc.Methods)),
	}
	for _, m := range svc.Methods {
		service.Methods = append(service.Methods, methodSurface{
			GoName:       m.GoName,
			FullName:     m.FullName,
			InputType:    m.InputType,
			OutputType:   m.OutputType,
			Shape:        m.Shape,
			ClientStream: m.ClientStream,
			ServerStream: m.ServerStream,
		})
	}
	out.Service = service
	for _, msg := range file.Messages {
		projected := messageSurface{GoName: msg.GoName, Fields: make([]fieldSurface, 0, len(msg.Fields))}
		for _, f := range msg.Fields {
			projected.Fields = append(projected.Fields, fieldSurface{
				GoName:      f.GoName,
				Number:      f.Number,
				Kind:        f.Kind,
				MessageType: f.MessageType,
				EnumType:    f.EnumType,
			})
		}
		out.Messages = append(out.Messages, projected)
	}
	return out
}

// assertEchoDesc checks the whole descriptor surface of one frontend's output
// against the golden expectation — method count and order included, so a
// frontend that invents, drops, duplicates or reorders a method fails here.
func assertEchoDesc(t *testing.T, frontendName string, file ir.File) surface {
	t.Helper()
	if len(file.Services) != 1 {
		t.Fatalf("%s: services = %d, want 1", frontendName, len(file.Services))
	}
	got, want := surfaceOf(file), wantEcho()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: descriptor surface mismatch\n got: %+v\nwant: %+v", frontendName, got, want)
	}
	return got
}

const echoProtoSource = `syntax = "proto3";
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

// echoIRJSON describes the same IDL as echoProtoSource and as the plugin
// fixture, messages included, so that all three inputs can be compared over
// the full surface. The shared testdata/echo.ir.json is a message-less IR and
// stays covered by irjson_test.go.
const echoIRJSON = `{
  "go_package": "echov1",
  "proto_package": "echo.v1",
  "input_base": "echo",
  "outputs": { "messages": "echo.msg.go", "stub": "echo.argos.go" },
  "messages": [
    { "go_name": "EchoRequest", "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }] },
    { "go_name": "EchoResponse", "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }] },
    { "go_name": "WatchRequest", "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }] },
    { "go_name": "Event", "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }] }
  ],
  "services": [
    {
      "go_name": "EchoService",
      "full_name": "echo.v1.EchoService",
      "methods": [
        {
          "go_name": "Echo",
          "full_name": "echo.v1.EchoService.Echo",
          "input_type": "EchoRequest",
          "output_type": "EchoResponse",
          "shape": 0
        },
        {
          "go_name": "Watch",
          "full_name": "echo.v1.EchoService.Watch",
          "input_type": "WatchRequest",
          "output_type": "Event",
          "shape": 1,
          "server_stream": true
        }
      ]
    }
  ]
}
`

func TestFrontendsAgreeEchoDescriptorSurface(t *testing.T) {
	dir := t.TempDir()

	protoPath := filepath.Join(dir, "echo.proto")
	if err := os.WriteFile(protoPath, []byte(echoProtoSource), 0o600); err != nil {
		t.Fatalf("WriteFile proto: %v", err)
	}
	protoFiles, err := (proto.Frontend{ImportPaths: []string{dir}}).Parse(context.Background(), []string{protoPath})
	if err != nil {
		t.Fatalf("proto Parse: %v", err)
	}
	protoFiles[0].Normalize(false)
	protoSurface := assertEchoDesc(t, "proto", protoFiles[0])

	irPath := filepath.Join(dir, "echo.ir.json")
	if err := os.WriteFile(irPath, []byte(echoIRJSON), 0o600); err != nil {
		t.Fatalf("WriteFile ir json: %v", err)
	}
	irFiles, err := irjson.Frontend{}.Parse(context.Background(), []string{irPath})
	if err != nil {
		t.Fatalf("irjson Parse: %v", err)
	}
	irSurface := assertEchoDesc(t, "irjson", irFiles[0])

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
	pluginSurface := assertEchoDesc(t, "execplugin", pluginFiles[0])

	// Beyond each matching the golden expectation, the three frontends must
	// agree with each other on every method and message field.
	if !reflect.DeepEqual(protoSurface, irSurface) || !reflect.DeepEqual(irSurface, pluginSurface) {
		t.Fatalf("frontends disagree on the echo descriptor surface:\n%s\n%s\n%s",
			fmt.Sprintf("proto      = %+v", protoSurface),
			fmt.Sprintf("irjson     = %+v", irSurface),
			fmt.Sprintf("execplugin = %+v", pluginSurface))
	}
}
