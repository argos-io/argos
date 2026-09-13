package telnet

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/transport"
)

type echoServer struct{}

func (*echoServer) Echo(
	_ context.Context,
	request *echov1.EchoRequest,
) (*echov1.EchoResponse, error) {
	return &echov1.EchoResponse{Msg: "hello " + request.GetMsg()}, nil
}

func (*echoServer) Watch(
	_ context.Context,
	request *echov1.WatchRequest,
	stream echov1.EchoService_WatchServer,
) error {
	return stream.Send(&echov1.Event{Msg: "hello " + request.GetMsg()})
}

func startEcho(t *testing.T, addr string) *channel {
	t.Helper()
	tr := New().(*channel)
	server := argos.NewServer()
	service := server.NewService(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	echov1.RegisterEchoService(service, &echoServer{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("server.Run: %v", err)
		}
	})
	return tr
}

func TestEchoRoundTrip(t *testing.T) {
	tr := startEcho(t, "127.0.0.1:0")
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	response, err := client.Echo(
		context.Background(),
		&echov1.EchoRequest{Msg: "telnet"},
	)
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "hello telnet"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestRawConnErrorPath(t *testing.T) {
	tr := New().(*channel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(
			_ context.Context,
			method string,
			f transport.Framer,
		) error {
			if method != "echo.v1.EchoService/Echo" {
				t.Errorf("method = %q, want echo.v1.EchoService/Echo", method)
			}
			reader, err := f.Recv()
			if err != nil {
				return err
			}
			if _, err := io.ReadAll(reader); err != nil {
				return err
			}
			writer, err := f.Send()
			if err != nil {
				return err
			}
			if _, err := writer.Write([]byte(`{"msg":"denied"}`)); err != nil {
				return err
			}
			if err := writer.Close(); err != nil {
				return err
			}
			return errs.Error(errs.Unauthenticated, "no token")
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("ListenAndServe: %v", err)
		}
	})

	conn, err := net.Dial("tcp", transport.DialableAddress(tr.Addr()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if _, err := io.WriteString(conn, "echo.v1.EchoService/Echo\n"); err != nil {
		t.Fatalf("Write method: %v", err)
	}
	if _, err := io.WriteString(conn, `{"msg":"telnet"}`+"\n"); err != nil {
		t.Fatalf("Write payload: %v", err)
	}

	reader := bufio.NewReader(conn)
	line, err := readLine(reader)
	if err != nil {
		t.Fatalf("Read response: %v", err)
	}
	if strings.HasPrefix(line, errPrefix) {
		t.Fatalf("first line = %q, want JSON response before ERR", line)
	}
	var response echov1.EchoResponse
	if err := json.Unmarshal([]byte(line), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	statusLine, err := readLine(reader)
	if err != nil {
		t.Fatalf("Read ERR line: %v", err)
	}
	if got, want := statusLine, "ERR 2 no token"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}

func TestSecondRecvFails(t *testing.T) {
	tr := New().(*channel)
	secondRecv := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(
			_ context.Context,
			_ string,
			f transport.Framer,
		) error {
			if _, err := f.Recv(); err != nil {
				return err
			}
			_, err := f.Recv()
			secondRecv <- err
			return err
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("ListenAndServe: %v", err)
		}
	})

	conn, err := net.Dial("tcp", transport.DialableAddress(tr.Addr()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "raw/Echo\n"); err != nil {
		t.Fatalf("Write method: %v", err)
	}
	if _, err := io.WriteString(conn, "{}\n"); err != nil {
		t.Fatalf("Write payload: %v", err)
	}
	if err := <-secondRecv; err == nil {
		t.Fatal("second server Recv succeeded, want error")
	}
}

func waitBound(tr *channel) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tr.Addr() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("transport did not start listening")
}
