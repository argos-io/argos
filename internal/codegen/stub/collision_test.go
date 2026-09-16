package stub

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/argos-io/argos/internal/codegen/gen/stubgen"
	"github.com/argos-io/argos/internal/codegen/ir"
)

func TestCheckStubMessageCollisionRejectsServiceDescName(t *testing.T) {
	file := ir.File{
		GoPackage: "service",
		Services: []ir.Service{{
			GoName: "FooService",
			Methods: []ir.Method{{
				GoName: "Bar", InputType: "Request", OutputType: "Response",
			}},
		}},
	}
	stubSyms := stubgen.Symbols(file)
	if err := ir.CheckStubMessageCollision(stubSyms, []string{"FooServiceDesc"}); err == nil {
		t.Fatal("CheckStubMessageCollision succeeded for service descriptor name")
	}
}

func TestRunRejectsEnumStubSymbolCollision(t *testing.T) {
	dir := t.TempDir()
	protoPath := filepath.Join(dir, "coll.proto")
	const src = `syntax = "proto3";

package coll.v1;
option go_package = "collv1";

enum EchoServiceDesc {
  ECHO_SERVICE_DESC_UNSPECIFIED = 0;
}

message Req { string msg = 1; }
message Rsp { string msg = 1; }

service EchoService {
  rpc Echo(Req) returns (Rsp);
}
`
	if err := os.WriteFile(protoPath, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(t.Context(), Options{From: "proto", ImportPaths: []string{dir}, OutDir: dir}, []string{protoPath})
	if err == nil || !strings.Contains(err.Error(), "EchoServiceDesc") {
		t.Fatalf("Run error = %v, want symbol collision involving EchoServiceDesc", err)
	}
}
