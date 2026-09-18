package httpunary

import (
	"testing"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
)

func TestRESTRouterResolveAccept(t *testing.T) {
	getUser := descriptor.MustMethod("demo.v1.Users.GetUser", descriptor.Unary)
	r, err := newRESTRouter([]Binding{
		{Method: getUser, Verb: "GET", Pattern: "/v1/users/{id}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	in := metadata.Metadata{}
	name, err := r.ResolveAccept("GET", "/v1/users/42", in)
	if err != nil {
		t.Fatal(err)
	}
	if name != getUser.FullName() {
		t.Fatalf("fullName = %q", name)
	}
	if got := in[PathVarMetadataPrefix+"id"]; len(got) != 1 || got[0] != "42" {
		t.Fatalf("path var = %v", got)
	}
}

func TestRESTRouterBuildPreface(t *testing.T) {
	getUser := descriptor.MustMethod("demo.v1.Users.GetUser", descriptor.Unary)
	r, err := newRESTRouter([]Binding{
		{Method: getUser, Verb: "GET", Pattern: "/v1/users/{id}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	out := metadata.Metadata{PathVarMetadataPrefix + "id": []string{"7"}}
	p, err := r.BuildPreface(getUser, "json", out)
	if err != nil {
		t.Fatal(err)
	}
	if p.Method != "GET" {
		t.Fatalf("method = %q", p.Method)
	}
	if p.RequestTarget != "/v1/users/7" {
		t.Fatalf("target = %q", p.RequestTarget)
	}
	for _, h := range p.Headers {
		if h.Name == "content-type" {
			t.Fatal("GET should not set content-type by default")
		}
	}
}
