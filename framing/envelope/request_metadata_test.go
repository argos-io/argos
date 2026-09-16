package envelope_test

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/metadata"
)

// A responder's incoming headers are the request's OPEN headers. Arriving DATA
// used to run the initiator-only "implied headers" path on the responder as
// well, overwriting the parsed request metadata with an empty map — so every
// server filter and handler that read request metadata after the first Recv
// saw nothing. See framing/envelope/call.go ensureImpliedHeaders.
func TestServerRequestMetadataSurvivesFirstData(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := tcpBytePair(t)
	fr := envelope.New()

	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cliSess.Close() }()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvSess.Close() }()

	method := descriptor.MustMethod("svc.Meta", descriptor.Unary)

	type observed struct {
		beforeBody int
		afterBody  int
		err        error
	}
	got := make(chan observed, 1)

	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			got <- observed{err: err}
			return
		}
		before := len(md.IncomingHeaders()["authorization"])
		if _, _, err := sc.Recv(); err != nil {
			got <- observed{err: err}
			return
		}
		after := len(md.IncomingHeaders()["authorization"])
		got <- observed{beforeBody: before, afterBody: after}

		_, _, _ = sc.Recv() // END
		_ = sc.Send([]byte("ok"))
		_ = sc.Finish(nil)
		_ = sc.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cliMD := metadata.New(metadata.RoleInitiator, nil)
	if err := cliMD.AddOutgoingHeader("authorization", "Bearer x"); err != nil {
		t.Fatal(err)
	}
	c, err := cliSess.OpenCall(ctx, method, framing.CallSpec{Metadata: cliMD})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Send([]byte("q")); err != nil {
		t.Fatal(err)
	}
	if err := c.HalfClose(); err != nil {
		t.Fatal(err)
	}

	select {
	case o := <-got:
		if o.err != nil {
			t.Fatalf("server: %v", o.err)
		}
		if o.beforeBody != 1 {
			t.Fatalf("request metadata missing before the request body: authorization = %d entries", o.beforeBody)
		}
		if o.afterBody != 1 {
			t.Fatalf("request metadata wiped by the request body: authorization = %d entries, want 1", o.afterBody)
		}
	case <-ctx.Done():
		t.Fatal("server never observed the call")
	}

	if _, _, err := c.Recv(); err != nil {
		t.Fatalf("client Recv: %v", err)
	}
}
