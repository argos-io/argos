package proto

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
	got := files[0].Services[0].Methods[0].FullName
	if got != "Echo/Say" {
		t.Fatalf("FullName = %q, want Echo/Say", got)
	}
}
