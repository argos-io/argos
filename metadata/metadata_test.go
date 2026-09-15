package metadata_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
)

func TestCloneIndependent(t *testing.T) {
	src := metadata.Metadata{"k": {"v"}}
	cp := metadata.Clone(src)
	cp["k"][0] = "mutated"
	cp["k"] = append(cp["k"], "x")
	cp["n"] = []string{"y"}
	if src["k"][0] != "v" || len(src["k"]) != 1 {
		t.Fatalf("Clone leaked mutation into source: %v", src)
	}
	if _, ok := src["n"]; ok {
		t.Fatal("Clone leaked new key into source")
	}
	if metadata.Clone(nil) != nil {
		t.Fatal("Clone(nil) must be nil")
	}
}

func TestGettersReturnDefensiveCopies(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error { return nil })
	if err := md.AddOutgoingHeader("oh", "1"); err != nil {
		t.Fatal(err)
	}
	if err := md.AddOutgoingTrailer("ot", "2"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.SetIncomingHeaders(md, metadata.Metadata{"ih": {"3"}}); err != nil {
		t.Fatal(err)
	}
	if err := metadata.SetIncomingTrailers(md, metadata.Metadata{"it": {"4"}}); err != nil {
		t.Fatal(err)
	}

	mutate := func(got metadata.Metadata, key string) {
		got[key][0] = "mutated"
		got[key] = append(got[key], "extra")
		got["injected"] = []string{"x"}
	}
	mutate(md.OutgoingHeaders(), "oh")
	mutate(md.OutgoingTrailers(), "ot")
	mutate(md.IncomingHeaders(), "ih")
	mutate(md.IncomingTrailers(), "it")

	assertOne := func(name string, got metadata.Metadata, key, want string) {
		t.Helper()
		if got[key][0] != want || len(got[key]) != 1 {
			t.Fatalf("%s: internal leaked; got %v", name, got)
		}
		if _, ok := got["injected"]; ok {
			t.Fatalf("%s: injected key visible on re-get", name)
		}
	}
	assertOne("OutgoingHeaders", md.OutgoingHeaders(), "oh", "1")
	assertOne("OutgoingTrailers", md.OutgoingTrailers(), "ot", "2")
	assertOne("IncomingHeaders", md.IncomingHeaders(), "ih", "3")
	assertOne("IncomingTrailers", md.IncomingTrailers(), "it", "4")
}

func TestAddAfterFreezeClearError(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error { return nil })
	if err := md.AddOutgoingHeader("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	err := md.AddOutgoingHeader("a", "2")
	if !errors.Is(err, metadata.ErrOutgoingHeadersFrozen) {
		t.Fatalf("AddOutgoingHeader after freeze: %v", err)
	}
	got := md.OutgoingHeaders()
	if len(got["a"]) != 1 || got["a"][0] != "1" {
		t.Fatalf("frozen add must not mutate; got %v", got)
	}
}

func TestSendHeadersFreezesAndSecondIsAlreadySent(t *testing.T) {
	var submitted metadata.Metadata
	md := metadata.New(metadata.RoleResponder, func(h metadata.Metadata) error {
		submitted = metadata.Clone(h)
		return nil
	})
	if err := md.AddOutgoingHeader("h", "v"); err != nil {
		t.Fatal(err)
	}
	if err := md.SendHeaders(); err != nil {
		t.Fatal(err)
	}
	if submitted["h"][0] != "v" {
		t.Fatalf("submitted = %v", submitted)
	}
	if err := md.AddOutgoingHeader("h", "again"); !errors.Is(err, metadata.ErrOutgoingHeadersFrozen) {
		t.Fatalf("Add after SendHeaders: %v", err)
	}
	err := md.SendHeaders()
	if !errors.Is(err, metadata.ErrHeadersAlreadySent) {
		t.Fatalf("second SendHeaders: %v", err)
	}
}

func TestSendHeadersNilCallbackUnimplementedNoFreeze(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, nil)
	if err := md.AddOutgoingHeader("h", "v"); err != nil {
		t.Fatal(err)
	}
	err := md.SendHeaders()
	if status.CodeOf(err) != status.Unimplemented {
		t.Fatalf("CodeOf = %v, want Unimplemented; err=%v", status.CodeOf(err), err)
	}
	if err := md.AddOutgoingHeader("h", "2"); err != nil {
		t.Fatalf("must not freeze on unsupported SendHeaders: %v", err)
	}
	got := md.OutgoingHeaders()
	if len(got["h"]) != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestSendHeadersReturnsUnimplementedNoFreeze(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error {
		return status.Error(status.Unimplemented, "carrier: no headers")
	})
	if err := md.AddOutgoingHeader("h", "v"); err != nil {
		t.Fatal(err)
	}
	err := md.SendHeaders()
	if status.CodeOf(err) != status.Unimplemented {
		t.Fatalf("CodeOf = %v, err=%v", status.CodeOf(err), err)
	}
	if err := md.AddOutgoingHeader("h", "2"); err != nil {
		t.Fatalf("must not freeze: %v", err)
	}
}

func TestRoleInitiator(t *testing.T) {
	md := metadata.New(metadata.RoleInitiator, func(metadata.Metadata) error { return nil })
	if err := md.AddOutgoingHeader("h", "v"); err != nil {
		t.Fatal(err)
	}
	err := md.AddOutgoingTrailer("t", "v")
	if status.CodeOf(err) != status.Unimplemented {
		t.Fatalf("AddOutgoingTrailer initiator: CodeOf=%v err=%v", status.CodeOf(err), err)
	}
	err = md.SendHeaders()
	if status.CodeOf(err) != status.Unimplemented {
		t.Fatalf("SendHeaders initiator: CodeOf=%v err=%v", status.CodeOf(err), err)
	}
	// Initiator freeze still works for OpenCall path.
	if err := metadata.FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	if err := md.AddOutgoingHeader("h", "x"); !errors.Is(err, metadata.ErrOutgoingHeadersFrozen) {
		t.Fatalf("after freeze: %v", err)
	}
}

func TestRoleResponderTrailersFreeze(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, nil)
	if err := md.AddOutgoingTrailer("t", "1"); err != nil {
		t.Fatal(err)
	}
	if err := metadata.FreezeOutgoingTrailers(md); err != nil {
		t.Fatal(err)
	}
	err := md.AddOutgoingTrailer("t", "2")
	if !errors.Is(err, metadata.ErrOutgoingTrailersFrozen) {
		t.Fatalf("AddOutgoingTrailer after freeze: %v", err)
	}
}

func TestContextRoundTrip(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, nil)
	ctx := metadata.ContextWith(context.Background(), md)
	got, ok := metadata.FromContext(ctx)
	if !ok || got != md {
		t.Fatalf("FromContext: ok=%v got=%v", ok, got)
	}
	if _, ok := metadata.FromContext(context.Background()); ok {
		t.Fatal("empty ctx must miss")
	}
}

func TestSetIncomingCopiesInput(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, nil)
	in := metadata.Metadata{"k": {"v"}}
	if err := metadata.SetIncomingHeaders(md, in); err != nil {
		t.Fatal(err)
	}
	in["k"][0] = "mut"
	in["n"] = []string{"x"}
	got := md.IncomingHeaders()
	if got["k"][0] != "v" || len(got["k"]) != 1 {
		t.Fatalf("SetIncomingHeaders aliased input: %v", got)
	}
}

func TestFramingHelpersRejectForeign(t *testing.T) {
	var foreign metadata.CallMetadata = foreignMD{}
	if err := metadata.SetIncomingHeaders(foreign, nil); err == nil {
		t.Fatal("expected error for foreign CallMetadata")
	}
	if err := metadata.FreezeOutgoingHeaders(foreign); err == nil {
		t.Fatal("expected error for foreign CallMetadata")
	}
}

type foreignMD struct{}

func (foreignMD) IncomingHeaders() metadata.Metadata  { return nil }
func (foreignMD) IncomingTrailers() metadata.Metadata { return nil }
func (foreignMD) OutgoingHeaders() metadata.Metadata  { return nil }
func (foreignMD) OutgoingTrailers() metadata.Metadata { return nil }
func (foreignMD) AddOutgoingHeader(string, ...string) error {
	return nil
}
func (foreignMD) AddOutgoingTrailer(string, ...string) error {
	return nil
}
func (foreignMD) SendHeaders() error { return nil }

func TestConcurrentAddAndGettersRace(t *testing.T) {
	md := metadata.New(metadata.RoleResponder, nil)
	const n = 64
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_ = md.AddOutgoingHeader("k", "v")
			_ = md.AddOutgoingTrailer("t", "v")
		}(i)
		go func() {
			defer wg.Done()
			_ = md.OutgoingHeaders()
			_ = md.OutgoingTrailers()
			_ = md.IncomingHeaders()
			_ = md.IncomingTrailers()
		}()
	}
	wg.Wait()
	oh := md.OutgoingHeaders()
	ot := md.OutgoingTrailers()
	if len(oh["k"]) != n {
		t.Fatalf("outgoing headers count = %d, want %d", len(oh["k"]), n)
	}
	if len(ot["t"]) != n {
		t.Fatalf("outgoing trailers count = %d, want %d", len(ot["t"]), n)
	}
}

func TestFreezeOutgoingHeadersIdempotent(t *testing.T) {
	md := metadata.New(metadata.RoleInitiator, nil)
	if err := metadata.FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	if err := metadata.FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
}

func TestSendHeadersAfterFreezeAlreadySent(t *testing.T) {
	calls := 0
	md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error {
		calls++
		return nil
	})
	if err := metadata.FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	err := md.SendHeaders()
	if !errors.Is(err, metadata.ErrHeadersAlreadySent) {
		t.Fatalf("SendHeaders after freeze: %v", err)
	}
	if calls != 0 {
		t.Fatalf("must not call sendHeaders after freeze; calls=%d", calls)
	}
}
