package telnet

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/server"

	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/metadata"
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
	server := server.New()
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

func TestContextCancellationClosesBlockedRecv(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &framer{
		conn:           conn,
		ctx:            ctx,
		initiator:      true,
		reader:         bufio.NewReader(conn),
		frames:         make(chan []byte, 1),
		done:           make(chan struct{}),
		maxMessageSize: transport.DefaultMaxMessageSize,
	}
	f.watchContext()
	go f.readAhead()
	defer f.Close()

	result := make(chan error, 1)
	go func() {
		_, err := f.Recv()
		result <- err
	}()
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Recv error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Recv was not interrupted by context cancellation")
	}
}

func TestMissingStatusIsProtocolError(t *testing.T) {
	conn, peer := net.Pipe()
	f := &framer{
		conn:           conn,
		ctx:            context.Background(),
		reader:         bufio.NewReader(conn),
		frames:         make(chan []byte, 1),
		done:           make(chan struct{}),
		maxMessageSize: transport.DefaultMaxMessageSize,
		initiator:      true,
	}
	go f.readAhead()
	if err := peer.Close(); err != nil {
		t.Fatalf("peer.Close: %v", err)
	}
	defer f.Close()

	_, err := f.Recv()
	if err == nil || err.Error() != "telnet: response ended before status line" {
		t.Fatalf("Recv = %v, want missing-status protocol error", err)
	}
}

func TestStatusLineEscapesControlCharacters(t *testing.T) {
	const message = "bad\\line\nnext\rline"
	var buf bytes.Buffer
	if err := writeErrLine(&buf, errs.InvalidArgument, message, transport.DefaultMaxMessageSize); err != nil {
		t.Fatalf("writeErrLine: %v", err)
	}
	line := strings.TrimSuffix(buf.String(), "\n")
	if strings.ContainsAny(line, "\r\n") {
		t.Fatalf("status line contains an unescaped line break: %q", line)
	}
	if got := parseErrLine(line); errs.CodeOf(got) != errs.InvalidArgument || got.Error() != message {
		t.Fatalf("parsed status = (%v, %q), want (%v, %q)", errs.CodeOf(got), got, errs.InvalidArgument, message)
	}
}

func TestStatusLineIsReadableWithSmallMessageLimit(t *testing.T) {
	var buf bytes.Buffer
	if err := writeErrLine(&buf, errs.Unauthenticated, "denied", 1); err != nil {
		t.Fatalf("writeErrLine: %v", err)
	}
	line, err := readLine(bufio.NewReader(&buf), lineLimit(1))
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	if got := errs.CodeOf(parseErrLine(line)); got != errs.Unauthenticated {
		t.Fatalf("status code = %d, want %d", got, errs.Unauthenticated)
	}
}

func TestReadLineLimitExcludesDelimiter(t *testing.T) {
	line, err := readLine(bufio.NewReader(strings.NewReader("abc\n")), 3)
	if err != nil {
		t.Fatalf("readLine exact limit: %v", err)
	}
	if line != "abc" {
		t.Fatalf("line = %q, want abc", line)
	}
	if _, err := readLine(bufio.NewReader(strings.NewReader("abcd\n")), 3); err == nil {
		t.Fatal("readLine accepted a payload beyond the limit")
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

func TestRequestMetadataAvailableToHandler(t *testing.T) {
	tr := New().(*channel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	gotMetadata := make(chan metadata.Metadata, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(callCtx context.Context, _ string, _ transport.Framer) error {
			gotMetadata <- metadata.Clone(metadata.FromContext(callCtx))
			return nil
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
	if _, err := io.WriteString(conn, "m authorization Bearer x\n{}\n"); err != nil {
		t.Fatalf("Write request: %v", err)
	}

	select {
	case md := <-gotMetadata:
		if got := md["authorization"]; len(got) != 1 || got[0] != "Bearer x" {
			t.Fatalf("metadata = %v, want authorization=Bearer x", md)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not receive request metadata")
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
