package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

func TestAcceptCallRejectReturnsFinishableCall(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	f := newTestFraming(t, nil, "")
	spec := session.SessionSpec{CodecName: "proto"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srvErr := make(chan error, 1)
	go func() {
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			srvErr <- err
			return
		}
		ss, err := f.NewServerSession(ctx, srvConn, spec)
		if err != nil {
			srvErr <- err
			return
		}
		defer func() { _ = ss.Close() }()

		md := metadata.New(metadata.RoleResponder, nil)
		call, err := ss.AcceptCall(ctx, session.CallSpec{Metadata: md})
		if !errors.Is(err, session.ErrCallRejected) {
			srvErr <- err
			return
		}
		if call == nil {
			srvErr <- errors.New("AcceptCall returned nil call on reject")
			return
		}
		defer func() { _ = call.Close() }()

		finishErr := status.Error(status.InvalidArgument, err.Error())
		var se *status.StatusError
		if errors.As(err, &se) {
			finishErr = se
		}
		if err := call.Finish(finishErr); err != nil {
			srvErr <- err
			return
		}
		srvErr <- nil
	}()

	car, err := cli.OpenStream(ctx, transport.RequestPreface{
		RequestTarget: "/echo.v1.Echo/Echo",
		// Missing content-type → ParseRequestHeaders rejects.
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer func() { _ = car.Abort() }()
	if err := car.(transport.SendCloser).CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, ok := car.(transport.ResponseHeaderReader); !ok {
		t.Fatal("carrier missing ResponseHeaderReader")
	}

	select {
	case err := <-srvErr:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for server reject")
	}
}
