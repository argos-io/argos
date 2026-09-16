package message

import (
	"strings"
	"testing"
)

// The source path lands in a // line comment. It comes from the .proto file
// name or a plugin-supplied descriptor, so a newline in it would end the
// comment and turn whatever follows into real Go source. go/format only
// catches an injection that is not itself valid Go.
func TestCommentSafeNeutralisesLineBreaks(t *testing.T) {
	cases := map[string]string{
		"a/b.proto":                   "a/b.proto",
		"x\n\nvar Injected = 1\n// y": "x  var Injected = 1 // y",
		"with\rcarriage":              "with carriage",
		"nul\x00and\x1bescape":        "nulandescape",
		"unicode/\u00e9\u4e2d.proto":  "unicode/\u00e9\u4e2d.proto",
	}
	for in, want := range cases {
		if got := commentSafe(in); got != want {
			t.Fatalf("commentSafe(%q) = %q, want %q", in, got, want)
		}
	}
	// Whatever the input, the result must stay on one line.
	for in := range cases {
		if strings.ContainsAny(commentSafe(in), "\n\r") {
			t.Fatalf("commentSafe(%q) still breaks the comment", in)
		}
	}
}
