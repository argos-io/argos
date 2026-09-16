package proto

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An input outside every --proto-path falls back to its basename, so two
// distinct files could claim the same compile name and the compiler would
// resolve one of them twice. Generating from the wrong descriptor silently is
// worse than refusing.
func TestDistinctInputsWithSameBasenameRejected(t *testing.T) {
	root := t.TempDir()
	var inputs []string
	for _, sub := range []string{"a", "b"} {
		dir := filepath.Join(root, sub)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "svc.proto")
		src := "syntax = \"proto3\";\npackage p" + sub + ";\n"
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, path)
	}

	// Default import path is ".", so neither input is under it.
	_, err := Frontend{}.Parse(context.Background(), inputs)
	if err == nil {
		t.Fatal("two inputs sharing a basename outside --proto-path were accepted")
	}
	if !strings.Contains(err.Error(), "both compile as") {
		t.Fatalf("err = %v, want it to name the compile-name collision", err)
	}
	// Both offending inputs must be named so the message is actionable.
	for _, in := range inputs {
		if !strings.Contains(err.Error(), in) {
			t.Fatalf("err = %v, want it to mention %s", err, in)
		}
	}
}
