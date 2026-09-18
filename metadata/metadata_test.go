package metadata

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/argos-io/argos/status"
)

func TestCloneIndependent(t *testing.T) {
	src := Metadata{"k": {"v"}}
	cp := Clone(src)
	cp["k"][0] = "mutated"
	cp["k"] = append(cp["k"], "x")
	cp["n"] = []string{"y"}
	if src["k"][0] != "v" || len(src["k"]) != 1 {
		t.Fatalf("Clone leaked mutation into source: %v", src)
	}
	if _, ok := src["n"]; ok {
		t.Fatal("Clone leaked new key into source")
	}
	if Clone(nil) != nil {
		t.Fatal("Clone(nil) must be nil")
	}
}

func TestGettersReturnDefensiveCopies(t *testing.T) {
	md := New(RoleResponder, func(Metadata) error { return nil })
	if err := md.AddOutgoingHeader("oh", "1"); err != nil {
		t.Fatal(err)
	}
	if err := md.AddOutgoingTrailer("ot", "2"); err != nil {
		t.Fatal(err)
	}
	if err := SetIncomingHeaders(md, Metadata{"ih": {"3"}}); err != nil {
		t.Fatal(err)
	}
	if err := SetIncomingTrailers(md, Metadata{"it": {"4"}}); err != nil {
		t.Fatal(err)
	}

	mutate := func(got Metadata, key string) {
		got[key][0] = "mutated"
		got[key] = append(got[key], "extra")
		got["injected"] = []string{"x"}
	}
	mutate(md.OutgoingHeaders(), "oh")
	mutate(md.OutgoingTrailers(), "ot")
	mutate(md.IncomingHeaders(), "ih")
	mutate(md.IncomingTrailers(), "it")

	assertOne := func(name string, got Metadata, key, want string) {
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
	md := New(RoleResponder, func(Metadata) error { return nil })
	if err := md.AddOutgoingHeader("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	err := md.AddOutgoingHeader("a", "2")
	if !errors.Is(err, ErrOutgoingHeadersFrozen) {
		t.Fatalf("AddOutgoingHeader after freeze: %v", err)
	}
	got := md.OutgoingHeaders()
	if len(got["a"]) != 1 || got["a"][0] != "1" {
		t.Fatalf("frozen add must not mutate; got %v", got)
	}
}

func TestSendHeadersFreezesAndSecondIsAlreadySent(t *testing.T) {
	var submitted Metadata
	md := New(RoleResponder, func(h Metadata) error {
		submitted = Clone(h)
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
	if err := md.AddOutgoingHeader("h", "again"); !errors.Is(err, ErrOutgoingHeadersFrozen) {
		t.Fatalf("Add after SendHeaders: %v", err)
	}
	err := md.SendHeaders()
	if !errors.Is(err, ErrHeadersAlreadySent) {
		t.Fatalf("second SendHeaders: %v", err)
	}
}

func TestSendHeadersNilCallbackUnimplementedNoFreeze(t *testing.T) {
	md := New(RoleResponder, nil)
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
	md := New(RoleResponder, func(Metadata) error {
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
	md := New(RoleInitiator, func(Metadata) error { return nil })
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
	if err := FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	if err := md.AddOutgoingHeader("h", "x"); !errors.Is(err, ErrOutgoingHeadersFrozen) {
		t.Fatalf("after freeze: %v", err)
	}
}

func TestRoleResponderTrailersFreeze(t *testing.T) {
	md := New(RoleResponder, nil)
	if err := md.AddOutgoingTrailer("t", "1"); err != nil {
		t.Fatal(err)
	}
	if err := FreezeOutgoingTrailers(md); err != nil {
		t.Fatal(err)
	}
	err := md.AddOutgoingTrailer("t", "2")
	if !errors.Is(err, ErrOutgoingTrailersFrozen) {
		t.Fatalf("AddOutgoingTrailer after freeze: %v", err)
	}
}

func TestContextRoundTrip(t *testing.T) {
	md := New(RoleResponder, nil)
	ctx := ContextWith(context.Background(), md)
	got, ok := FromContext(ctx)
	if !ok || got != md {
		t.Fatalf("FromContext: ok=%v got=%v", ok, got)
	}
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("empty ctx must miss")
	}
}

func TestSetIncomingCopiesInput(t *testing.T) {
	md := New(RoleResponder, nil)
	in := Metadata{"k": {"v"}}
	if err := SetIncomingHeaders(md, in); err != nil {
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
	var foreign CallMetadata = foreignMD{}
	if err := SetIncomingHeaders(foreign, nil); err == nil {
		t.Fatal("expected error for foreign CallMetadata")
	}
	if err := FreezeOutgoingHeaders(foreign); err == nil {
		t.Fatal("expected error for foreign CallMetadata")
	}
}

type foreignMD struct{}

func (foreignMD) IncomingHeaders() Metadata  { return nil }
func (foreignMD) IncomingTrailers() Metadata { return nil }
func (foreignMD) OutgoingHeaders() Metadata  { return nil }
func (foreignMD) OutgoingTrailers() Metadata { return nil }
func (foreignMD) AddOutgoingHeader(string, ...string) error {
	return nil
}
func (foreignMD) AddOutgoingTrailer(string, ...string) error {
	return nil
}
func (foreignMD) SendHeaders() error { return nil }

func TestConcurrentAddAndGettersRace(t *testing.T) {
	md := New(RoleResponder, nil)
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
	md := New(RoleInitiator, nil)
	if err := FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	if err := FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
}

func TestSendHeadersAfterFreezeAlreadySent(t *testing.T) {
	calls := 0
	md := New(RoleResponder, func(Metadata) error {
		calls++
		return nil
	})
	if err := FreezeOutgoingHeaders(md); err != nil {
		t.Fatal(err)
	}
	err := md.SendHeaders()
	if !errors.Is(err, ErrHeadersAlreadySent) {
		t.Fatalf("SendHeaders after freeze: %v", err)
	}
	if calls != 0 {
		t.Fatalf("must not call sendHeaders after freeze; calls=%d", calls)
	}
}
