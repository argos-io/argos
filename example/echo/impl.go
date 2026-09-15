package echov1

import "context"

type echoImpl struct{}

// NewEchoImpl returns the example Echo service implementation.
func NewEchoImpl() EchoServiceServer {
	return &echoImpl{}
}

func (s *echoImpl) Echo(
	_ context.Context,
	req *EchoRequest,
) (*EchoResponse, error) {
	return &EchoResponse{Msg: "hello " + req.GetMsg()}, nil
}

func (s *echoImpl) Watch(
	_ context.Context,
	req *WatchRequest,
	stream EchoService_WatchServer,
) error {
	for _, suffix := range []string{"one", "two", "three"} {
		if err := stream.Send(&Event{Msg: req.GetMsg() + " " + suffix}); err != nil {
			return err
		}
	}
	return nil
}
