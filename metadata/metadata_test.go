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
