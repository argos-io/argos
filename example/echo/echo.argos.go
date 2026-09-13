package echov1

import (
	"context"
	"io"

	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/option"
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

// EchoService_WatchServer sends events from a Watch call.
type EchoService_WatchServer interface {
	Send(*Event) error
}

// RegisterEchoService registers impl's method dispatcher with svc.
func RegisterEchoService(svc *server.Service, impl EchoServiceServer) {
	svc.Register(func(ctx context.Context, method string, st stream.Stream) error {
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
			return impl.Watch(ctx, in, &watchServer{Stream: st})
		default:
			return errs.Error(errs.Unimplemented, "unknown method")
		}
	})
}

type watchServer struct {
	stream.Stream
}

func (s *watchServer) Send(event *Event) error {
	return s.Stream.Send(event)
}

// EchoServiceClient calls the Echo service.
type EchoServiceClient interface {
	Echo(context.Context, *EchoRequest) (*EchoResponse, error)
	Watch(context.Context, *WatchRequest) EchoService_WatchClient
}

// EchoService_WatchClient receives events from a Watch call.
type EchoService_WatchClient interface {
	Recv() (*Event, error)
}

// NewEchoServiceClient creates an Echo service client.
func NewEchoServiceClient(opts ...option.Option) EchoServiceClient {
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
	wc := &watchClient{ch: make(chan *Event)}
	go func() {
		defer close(wc.ch)
		wc.err = c.c.Open(ctx, echoWatch, func(st stream.Stream) error {
			if err := st.Send(req); err != nil {
				return err
			}
			if err := st.CloseSend(); err != nil {
				return err
			}
			for {
				event := new(Event)
				switch err := st.Recv(event); err {
				case nil:
					wc.ch <- event
				case io.EOF:
					return nil
				default:
					return err
				}
			}
		})
	}()
	return wc
}

type watchClient struct {
	ch  chan *Event
	err error
}

func (c *watchClient) Recv() (*Event, error) {
	event, ok := <-c.ch
	if !ok {
		if c.err != nil {
			return nil, c.err
		}
		return nil, io.EOF
	}
	return event, nil
}
