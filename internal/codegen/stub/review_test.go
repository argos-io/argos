package stub

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A run emits a message file and a stub file together. Writing them one after
// another left a mixed revision on disk when a later write failed - new stub
// next to stale messages. Either every output is replaced or none is.
func TestWriteAllLeavesOriginalsWhenOneOutputFails(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "a.pb.go")
	if err := os.WriteFile(first, []byte("old-a"), 0o644); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "b.argos.go")
	if err := os.WriteFile(second, []byte("old-b"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The second output cannot be staged: its parent is a file, not a dir.
	blocker := filepath.Join(dir, "blocked")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(blocker, "c.argos.go")

	err := writeAll([]generatedOutput{
		{path: first, source: []byte("new-a")},
		{path: second, source: []byte("new-b")},
		{path: bad, source: []byte("new-c")},
	})
	if err == nil {
		t.Fatal("writeAll succeeded with an unwritable output")
	}

	for _, tc := range []struct{ path, want string }{
		{first, "old-a"},
		{second, "old-b"},
	} {
		got, readErr := os.ReadFile(tc.path)
		if readErr != nil {
			t.Fatalf("read %s: %v", tc.path, readErr)
		}
		if string(got) != tc.want {
			t.Fatalf("%s = %q, want %q: a failed run replaced part of the output set",
				filepath.Base(tc.path), got, tc.want)
		}
	}

	// No temporaries left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".argos-") {
			t.Fatalf("temporary file %s left behind after a failed run", e.Name())
		}
	}
}

// The happy path still replaces every output.
func TestWriteAllReplacesEveryOutput(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.pb.go")
	b := filepath.Join(dir, "nested", "b.argos.go")
	if err := writeAll([]generatedOutput{
		{path: a, source: []byte("A")},
		{path: b, source: []byte("B")},
	}); err != nil {
		t.Fatalf("writeAll: %v", err)
	}
	for path, want := range map[string]string{a: "A", b: "B"} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want %q", path, got, want)
		}
	}
}
