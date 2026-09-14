package http1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"

	jsoncodec "github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/transport"
)

type echoServer struct {
}

func (*echoServer) Echo(
	ctx context.Context,
	request *echov1.EchoRequest,
) (*echov1.EchoResponse, error) {
	prefix := ""
	if values := metadata.FromContext(ctx)["authorization"]; len(values) != 0 {
		prefix = values[0] + " "
	}
	return &echov1.EchoResponse{Msg: prefix + "hello " + request.GetMsg()}, nil
}

func (s *echoServer) Watch(
	_ context.Context,
	request *echov1.WatchRequest,
	stream echov1.EchoService_WatchServer,
) error {
	if err := stream.Send(&echov1.Event{Msg: "hello " + request.GetMsg()}); err != nil {
		return err
	}
	return stream.Send(&echov1.Event{Msg: "second"})
}

func startEcho(
	t *testing.T,
	opts ...argos.Option,
) (*channel, *echoServer) {
	t.Helper()
	tr := New().(*channel)
	impl := &echoServer{}
	server := server.New()
	service := server.NewService(append([]argos.Option{
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	}, opts...)...)
	echov1.RegisterEchoService(service, impl)

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
	return tr, impl
}

func TestEchoRoundTripWithMetadata(t *testing.T) {
	tr, _ := startEcho(t)
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	ctx := metadata.With(context.Background(), metadata.Metadata{
		"authorization": {"Bearer token"},
	})
	response, err := client.Echo(ctx, &echov1.EchoRequest{Msg: "http"})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "Bearer token hello http"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"msg":"followed"}`)
			return
		}
		http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	f, err := New().(*channel).Open(context.Background(), "svc/Method", transport.WithDialAddress(server.Listener.Addr().String()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	writer, err := f.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := writer.Write([]byte(`{"msg":"request"}`)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	if _, err := f.Recv(); err == nil {
		t.Fatal("Recv succeeded after an HTTP redirect")
	}
}

func TestRejectsInvalidContentType(t *testing.T) {
	tr, _ := startEcho(t)
	response, err := http.Post(
		"http://"+transport.DialableAddress(tr.Addr())+"/echo.v1.EchoService/Echo",
		"text/plain",
		bytes.NewBufferString(`{"msg":"http"}`),
	)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer response.Body.Close()
	if got, want := response.StatusCode, http.StatusBadRequest; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
}

func TestExactMessageLimitIsAccepted(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/raw/Echo", strings.NewReader("abc"))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	var got []byte
	serveCall(recorder, req, func(_ context.Context, _ string, f transport.Framer) error {
		r, err := f.Recv()
		if err != nil {
			return err
		}
		got, err = io.ReadAll(r)
		return err
	}, 3)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if string(got) != "abc" {
		t.Fatalf("payload = %q, want abc", got)
	}
}

func TestSendAfterCloseSendFails(t *testing.T) {
	tr := New().(*channel)
	f, err := tr.Open(context.Background(), "svc/Method", transport.WithDialAddress("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if _, err := f.Send(); err == nil {
		t.Fatal("Send succeeded after CloseSend")
	}
}

func TestNonOKResponseRejectsOKErrorCode(t *testing.T) {
	f := &clientFramer{
		response:       &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(strings.NewReader(`{}`))},
		maxMessageSize: transport.DefaultMaxMessageSize,
	}
	if _, err := f.Recv(); err == nil || errs.CodeOf(err) == errs.OK {
		t.Fatalf("Recv error = %v, want protocol error rather than OK", err)
	}
}

func TestNonOKResponseRejectsOversizedControlBody(t *testing.T) {
	body := `{"code":2,"message":"denied"}` + strings.Repeat(" ", int(maxErrorBodyBytes))
	f := &clientFramer{
		response:       &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader(body))},
		maxMessageSize: transport.DefaultMaxMessageSize,
	}
	if _, err := f.Recv(); err == nil || !strings.Contains(err.Error(), "exceeds configured limit") {
		t.Fatalf("Recv error = %v, want oversized trailing body error", err)
	}
}

func TestNonOKResponseRejectsTrailingJSON(t *testing.T) {
	f := &clientFramer{
		response:       &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader(`{"code":2} {"code":2}`))},
		maxMessageSize: transport.DefaultMaxMessageSize,
	}
	if _, err := f.Recv(); err == nil || !strings.Contains(err.Error(), "invalid error body") {
		t.Fatalf("Recv error = %v, want invalid trailing JSON error", err)
	}
}

func TestNonOKResponseIsReadableWithSmallMessageLimit(t *testing.T) {
	f := &clientFramer{
		response: &http.Response{
			StatusCode: http.StatusUnauthorized,
			Body:       io.NopCloser(strings.NewReader(`{"code":2,"message":"denied"}`)),
		},
		maxMessageSize: 1,
	}
	if _, err := f.Recv(); errs.CodeOf(err) != errs.Unauthenticated {
		t.Fatalf("Recv error = %v, want Unauthenticated", err)
	}
}

func TestMetadataLimit(t *testing.T) {
	if err := validateMetadata(metadata.Metadata{"x": {strings.Repeat("v", int(transport.DefaultMaxMetadataSize))}}); err == nil {
		t.Fatal("validateMetadata accepted oversized metadata")
	}
	values := make([]string, 2)
	for i := range values {
		values[i] = strings.Repeat("v", int(transport.DefaultMaxMetadataSize/2))
	}
	if err := validateMetadata(metadata.Metadata{"repeated": values}); err == nil {
		t.Fatal("validateMetadata accepted oversized repeated metadata")
	}

	req := httptest.NewRequest(http.MethodPost, "/raw/Echo", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Argos", strings.Repeat("v", int(transport.DefaultMaxMetadataSize)))
	recorder := httptest.NewRecorder()
	serveCall(recorder, req, func(context.Context, string, transport.Framer) error {
		t.Fatal("onCall should not run for oversized metadata")
		return nil
	}, transport.DefaultMaxMessageSize)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestServerResponseLimit(t *testing.T) {
	recorder := httptest.NewRecorder()
	framer := &serverFramer{response: recorder, maxMessageSize: 3}
	writer, err := framer.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := writer.Write([]byte("abcd")); err == nil {
		t.Fatal("response writer accepted payload over the configured limit")
	}
	if err := writer.Close(); err == nil {
		t.Fatal("Close accepted a response after an oversized write")
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("response body = %q, want empty after rejected write", recorder.Body.Bytes())
	}
}

func TestWatchIsRejectedOnUnaryTransport(t *testing.T) {
	tr, _ := startEcho(t)
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	stream := client.Watch(context.Background(), &echov1.WatchRequest{Msg: "http"})
	defer stream.Close()
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "does not support streaming calls") {
		t.Fatalf("Recv error = %v, want unsupported streaming error", err)
	}
}

func TestSecondServerRecvFails(t *testing.T) {
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

	response, err := http.Post(
		"http://"+transport.DialableAddress(tr.Addr())+"/raw/Echo",
		"application/json",
		bytes.NewBufferString("{}"),
	)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer response.Body.Close()
	if err := <-secondRecv; err == nil {
		t.Fatal("second server Recv succeeded, want error")
	}
}

func TestFilterUnauthenticated(t *testing.T) {
	deny := func(
		_ context.Context,
		_ string,
		_ stream.Stream,
		_ filter.Handler,
	) error {
		return errs.Error(errs.Unauthenticated, "no token")
	}
	tr, _ := startEcho(t, argos.WithFilter(deny))

	response, err := http.Post(
		"http://"+transport.DialableAddress(tr.Addr())+"/echo.v1.EchoService/Echo",
		"application/json",
		bytes.NewBufferString(`{"msg":"http"}`),
	)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer response.Body.Close()
	if got, want := response.StatusCode, http.StatusUnauthorized; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
	var body errorBody
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Code != errs.Unauthenticated || body.Message != "no token" {
		t.Fatalf("error body = %+v, want code 2 and message %q", body, "no token")
	}

	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	_, err = client.Echo(context.Background(), &echov1.EchoRequest{Msg: "http"})
	if got := errs.CodeOf(err); got != errs.Unauthenticated {
		t.Fatalf("client code = %d (err %v), want Unauthenticated", got, err)
	}
}

func TestClientSecondSendFails(t *testing.T) {
	tr, _ := startEcho(t)
	f, err := tr.Open(context.Background(), "echo.v1.EchoService/Echo")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writer, err := f.Send()
	if err != nil {
		t.Fatalf("first Send: %v", err)
	}
	if _, err := writer.Write([]byte(`{"msg":"http"}`)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.Send(); err == nil {
		t.Fatal("second client Send succeeded, want error")
	}
	reader, err := f.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
}

func TestServerWriterStopsAfterClose(t *testing.T) {
	response := httptest.NewRecorder()
	framer := &serverFramer{response: response}
	writer, err := framer.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := framer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := writer.Write([]byte("late")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late Write error = %v, want net.ErrClosed", err)
	}
	if err := writer.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late Close error = %v, want net.ErrClosed", err)
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
