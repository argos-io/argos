package envelope_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
)

// TestServerSerialAcceptAfterClose verifies AcceptCall for call N+1 does not
// begin until call N's Close completes (Sequential server model).
func TestServerSerialAcceptAfterClose(t *testing.T) {
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	defer cliSess.Close()
	srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	defer srvSess.Close()

	method := descriptor.MustMethod("envelope.v1.Echo.Echo", descriptor.Unary)
	hold := make(chan struct{})
	accept2Started := make(chan struct{})
	var order []string
	var mu sync.Mutex

	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc1, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall1: %v", err)
			return
		}
		mu.Lock()
		order = append(order, "accept1")
		mu.Unlock()
		_, _, _ = sc1.Recv()
		_ = sc1.Finish(nil)
		<-hold
		mu.Lock()
		order = append(order, "close1")
		mu.Unlock()
		_ = sc1.Close()

		close(accept2Started)
		sc2, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			t.Errorf("AcceptCall2: %v", err)
			return
		}
		mu.Lock()
		order = append(order, "accept2")
		mu.Unlock()
		_, _, _ = sc2.Recv()
		_ = sc2.Finish(nil)
		_ = sc2.Close()
	}()

	c1, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c1.HalfClose()
	_, _, _ = c1.Recv()
	_ = c1.Close()

	select {
	case <-accept2Started:
		t.Fatal("AcceptCall2 started before call1 Close")
	case <-time.After(100 * time.Millisecond):
	}

	close(hold)

	select {
	case <-accept2Started:
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptCall2 did not start after call1 Close")
	}

	c2, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c2.HalfClose()
	_, _, _ = c2.Recv()
	_ = c2.Close()

	mu.Lock()
	defer mu.Unlock()
	want := []string{"accept1", "close1", "accept2"}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("order = %v, want %v", order, want)
	}
}
