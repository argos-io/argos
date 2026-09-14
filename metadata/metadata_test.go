package metadata

import (
	"context"
	"testing"
)

func TestWithMetadataMergeSameMap(t *testing.T) {
	ctx := context.Background()
	ctx = With(ctx, Metadata{"a": {"1"}})
	ctx = With(ctx, Metadata{"b": {"2"}})
	md := FromContext(ctx)
	if md["a"][0] != "1" || md["b"][0] != "2" {
		t.Fatalf("merge failed: %v", md)
	}
}

func TestMetadataFromContextWritable(t *testing.T) {
	ctx := With(context.Background(), Metadata{})
	md := FromContext(ctx)
	md["k"] = []string{"v"}
	if FromContext(ctx)["k"][0] != "v" {
		t.Fatal("map must be writable through context")
	}
}

func TestWithDoesNotAliasParentOrInput(t *testing.T) {
	parentValues := []string{"parent"}
	parent := With(context.Background(), Metadata{"k": parentValues})
	inputValues := []string{"input"}
	child := With(parent, Metadata{"k": inputValues})

	inputValues[0] = "changed input"
	FromContext(child)["k"][0] = "changed child"

	if got := FromContext(parent)["k"][0]; got != "parent" {
		t.Fatalf("parent metadata changed to %q", got)
	}
	if got := FromContext(child)["k"][0]; got != "changed child" {
		t.Fatalf("child metadata = %q, want changed child", got)
	}
}
