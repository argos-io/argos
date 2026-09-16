package envelope_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
)

// MaxMetadataSize only constrained what we send. A peer could attach metadata
// up to MaxFrameSize to every frame and have it parsed and retained; the
// inbound limit is independent and must reject it.
func TestInboundMetadataLimitRejectsOversizedOpen(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	fr := envelope.New()
	const limit = 512

	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{
		Config: framing.Config{MaxInboundMetadataSize: limit, MaxFrameSize: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvSess.Close() }()

	raw, err := envelope.MarshalFrame(envelope.Frame{
		Type:   envelope.TypeOpen,
		CallID: 1,
		Method: "svc.Meta",
		Headers: []envelope.Header{
			{Name: "big", Value: strings.Repeat("x", limit*2)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// net.Pipe is unbuffered, so the accept must be waiting before we write.
	done := make(chan error, 1)
	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		_, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		done <- err
	}()
	if _, err := cliConn.Write(raw); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("oversized inbound metadata accepted")
		}
		if !errors.Is(err, envelope.ErrMetaTooLarge) {
			t.Fatalf("err = %v, want ErrMetaTooLarge", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AcceptCall blocked")
	}
}

// Metadata within the limit is still accepted.
func TestInboundMetadataLimitAcceptsWithinLimit(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	fr := envelope.New()

	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{
		Config: framing.Config{MaxInboundMetadataSize: 4096, MaxFrameSize: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvSess.Close() }()

	raw, err := envelope.MarshalFrame(envelope.Frame{
		Type:    envelope.TypeOpen,
		CallID:  1,
		Method:  "svc.Meta",
		Headers: []envelope.Header{{Name: "authorization", Value: fmt.Sprintf("Bearer %d", 42)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// net.Pipe is unbuffered: accept concurrently with the write.
	type accepted struct {
		call framing.ServerCall
		md   metadata.CallMetadata
		err  error
	}
	got := make(chan accepted, 1)
	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		got <- accepted{call: sc, md: md, err: err}
	}()
	if _, err := cliConn.Write(raw); err != nil {
		t.Fatal(err)
	}
	var res accepted
	select {
	case res = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("AcceptCall blocked")
	}
	sc, md := res.call, res.md
	if res.err != nil {
		t.Fatalf("AcceptCall: %v", res.err)
	}
	defer func() { _ = sc.Close() }()
	if got := md.IncomingHeaders()["authorization"]; len(got) != 1 {
		t.Fatalf("authorization = %v, want one entry", got)
	}
}
