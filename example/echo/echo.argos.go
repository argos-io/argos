package echov1

import (
	"context"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"
)

const (
	echoEcho  = "echo.v1.EchoService/Echo"
	echoWatch = "echo.v1.EchoService/Watch"
)

// EchoServiceServer implements the Echo service.
type EchoServiceServer interface {
	Echo(context.Context, *EchoRequest) (*EchoResponse, error)
	Watch(context.Context, *WatchRequest, EchoService_WatchServer) error
}

// EchoService_WatchServer sends and receives messages for a Watch call.
type EchoService_WatchServer interface {
	Send(*Event) error
}

// RegisterEchoService registers impl's method dispatcher with svc.
func RegisterEchoService(svc *server.Service, impl EchoServiceServer) {
	svc.RegisterWithMethods(
		func(ctx context.Context, method string, st stream.Stream) error {
			switch method {
			case echoEcho:
				in := new(EchoRequest)
				if err := st.Recv(in); err != nil {
					return err
				}
				resp, err := impl.Echo(ctx, in)
				if err != nil {
					return err
				}
				return st.Send(resp)
			case echoWatch:
				in := new(WatchRequest)
				if err := st.Recv(in); err != nil {
					return err
				}
				return impl.Watch(ctx, in, &echoServiceWatchServer{Stream: st})
			default:
				return errs.Error(errs.Unimplemented, "unknown method")
			}
		},
		server.MethodInfo{Method: echoEcho, Kind: stream.CallUnary},
		server.MethodInfo{Method: echoWatch, Kind: stream.CallServerStreaming},
	)
}

type echoServiceWatchServer struct {
	stream.Stream
}

func (s *echoServiceWatchServer) Send(event *Event) error {
	return s.Stream.Send(event)
}

// EchoServiceClient calls the Echo service.
type EchoServiceClient interface {
	Echo(context.Context, *EchoRequest) (*EchoResponse, error)
	Watch(context.Context, *WatchRequest) EchoService_WatchClient
}

// EchoService_WatchClient sends and receives messages for a Watch call.
type EchoService_WatchClient interface {
	Recv() (*Event, error)
	Close() error
}

// NewEchoServiceClient creates an Echo service client.
func NewEchoServiceClient(opts ...argos.Option) EchoServiceClient {
	return &echoClient{c: client.New(opts...)}
}

type echoClient struct {
	c *client.Client
}

func (c *echoClient) Echo(ctx context.Context, req *EchoRequest) (*EchoResponse, error) {
	var resp *EchoResponse
	err := c.c.Open(ctx, echoEcho, func(st stream.Stream) error {
		if err := st.Send(req); err != nil {
			return err
		}
		if err := st.CloseSend(); err != nil {
			return err
		}
		resp = new(EchoResponse)
		return st.Recv(resp)
	})
	return resp, err
}

func (c *echoClient) Watch(ctx context.Context, req *WatchRequest) EchoService_WatchClient {
	wc := &echoServiceWatchClient{call: c.c.OpenStream(ctx, echoWatch, stream.CallServerStreaming), initialDone: make(chan struct{})}
	go func() {
		err := wc.call.Send(req)
		if err == nil {
			err = wc.call.CloseSend()
		}
		wc.initialErr = err
		close(wc.initialDone)
	}()
	return wc
}

type echoServiceWatchClient struct {
	call        *client.CallStream
	initialDone chan struct{}
	initialErr  error
}

func (c *echoServiceWatchClient) Recv() (*Event, error) {
	<-c.initialDone
	if c.initialErr != nil {
		return nil, c.initialErr
	}
	event := new(Event)
	if err := c.call.Recv(event); err != nil {
		return nil, err
	}
	return event, nil
}

func (c *echoServiceWatchClient) Close() error {
	return c.call.Close()
}
