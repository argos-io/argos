package message

import "testing"

func TestFileIdentIsInjectiveForCommonPathCharacters(t *testing.T) {
	paths := []string{"a-b.proto", "a_b.proto", "a/b.proto", "a.b.proto"}
	seen := make(map[string]string, len(paths))
	for _, path := range paths {
		ident := fileIdent(path)
		if previous, exists := seen[ident]; exists {
			t.Fatalf("fileIdent(%q) = %q, already used by %q", path, ident, previous)
		}
		seen[ident] = path
	}
}
