package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/compressor/gzip"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing/grpc/internal/testpb"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	grpcmd "google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	_ "github.com/argos-io/argos/resolver/ip"
)

const (
	interopService = "echo.v1.Echo"

	methodUnary        = interopService + ".Unary"
	methodClientStream = interopService + ".ClientStream"
	methodServerStream = interopService + ".ServerStream"
	methodBidi         = interopService + ".Bidi"
)

func interopDesc() descriptor.Service {
	return descriptor.MustService(interopService,
		descriptor.MustMethod(methodUnary, descriptor.Unary),
		descriptor.MustMethod(methodClientStream, descriptor.ClientStreaming),
		descriptor.MustMethod(methodServerStream, descriptor.ServerStreaming),
		descriptor.MustMethod(methodBidi, descriptor.BidiStreaming),
	)
}

func drainRecv(st stream.Stream) {
	for {
		var discard testpb.EchoMessage
		if err := st.Recv(&discard); err != nil {
			return
		}
	}
}

func argosEchoHandlers() map[string]filter.Handler {
	return map[string]filter.Handler{
		"Unary":        argosUnary,
		"ClientStream": argosClientStream,
		"ServerStream": argosServerStream,
		"Bidi":         argosBidi,
	}
}

func argosUnary(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
	if md, ok := metadata.FromContext(ctx); ok {
		_ = md.AddOutgoingHeader("x-argos-hdr", "hdr-v")
		_ = md.AddOutgoingTrailer("x-argos-trl", "trl-v")
	}
	var req testpb.EchoMessage
	if err := st.Recv(&req); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	drainRecv(st)
	if code, ok := parseForcedCode(req.GetValue()); ok {
		if code == status.OK {
			return st.Send(&testpb.EchoMessage{Value: "echo:" + req.GetValue()})
		}
		return status.Error(code, "argos forced "+req.GetValue())
	}
	return st.Send(&testpb.EchoMessage{Value: "echo:" + req.GetValue()})
}

func argosClientStream(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
	if md, ok := metadata.FromContext(ctx); ok {
		_ = md.AddOutgoingTrailer("x-argos-trl", "trl-v")
	}
	var last string
	for {
		var req testpb.EchoMessage
		err := st.Recv(&req)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		last = req.GetValue()
	}
	return st.Send(&testpb.EchoMessage{Value: "echo:" + last})
}

func argosServerStream(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
	if md, ok := metadata.FromContext(ctx); ok {
		_ = md.AddOutgoingHeader("x-argos-hdr", "hdr-v")
		_ = md.AddOutgoingTrailer("x-argos-trl", "trl-v")
	}
	var req testpb.EchoMessage
	if err := st.Recv(&req); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	drainRecv(st)
	for i := 0; i < 3; i++ {
		if err := st.Send(&testpb.EchoMessage{Value: fmt.Sprintf("%s-%d", req.GetValue(), i)}); err != nil {
			return err
		}
	}
	return nil
}

func argosBidi(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
	if md, ok := metadata.FromContext(ctx); ok {
		_ = md.AddOutgoingHeader("x-argos-hdr", "hdr-v")
		_ = md.AddOutgoingTrailer("x-argos-trl", "trl-v")
	}
	for {
		var req testpb.EchoMessage
		err := st.Recv(&req)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := st.Send(&testpb.EchoMessage{Value: "echo:" + req.GetValue()}); err != nil {
			return err
		}
	}
}

func parseForcedCode(v string) (status.Code, bool) {
	const p = "code:"
	if !strings.HasPrefix(v, p) {
		return 0, false
	}
	n, err := strconv.Atoi(v[len(p):])
	if err != nil || n < 0 || n > 16 {
		return 0, false
	}
	return status.Code(n), true
}

type grpcGoEcho struct {
	testpb.UnimplementedEchoServer
	forceTrailersOnly bool
	forceDetails      bool
	sawDeadline       chan<- time.Duration
	lastBin           chan<- []byte
}

func (s *grpcGoEcho) Unary(ctx context.Context, req *testpb.EchoMessage) (*testpb.EchoMessage, error) {
	if s.sawDeadline != nil {
		if dl, ok := ctx.Deadline(); ok {
			s.sawDeadline <- time.Until(dl)
		} else {
			s.sawDeadline <- 0
		}
	}
	if md, ok := grpcmd.FromIncomingContext(ctx); ok {
		if vs := md.Get("x-trace-bin"); len(vs) > 0 && s.lastBin != nil {
			s.lastBin <- []byte(vs[0])
		}
	}
	_ = grpc.SetHeader(ctx, grpcmd.Pairs("x-grpcgo-hdr", "hdr-v"))
	_ = grpc.SetTrailer(ctx, grpcmd.Pairs("x-grpcgo-trl", "trl-v"))
	if s.forceTrailersOnly {
		return nil, grpcstatus.Error(codes.NotFound, "trailers-only")
	}
	if s.forceDetails {
		st := grpcstatus.New(codes.FailedPrecondition, "from details proto")
		st, err := st.WithDetails(&errdetails.ErrorInfo{Reason: "ARGOS", Domain: "interop"})
		if err != nil {
			return nil, err
		}
		return nil, st.Err()
	}
	if code, ok := parseForcedCode(req.GetValue()); ok {
		if code == status.OK {
			return &testpb.EchoMessage{Value: "echo:" + req.GetValue()}, nil
		}
		return nil, grpcstatus.Error(codes.Code(code), "grpc-go forced "+req.GetValue())
	}
	return &testpb.EchoMessage{Value: "echo:" + req.GetValue()}, nil
}

func (s *grpcGoEcho) ClientStream(stream grpc.ClientStreamingServer[testpb.EchoMessage, testpb.EchoMessage]) error {
	_ = grpc.SetTrailer(stream.Context(), grpcmd.Pairs("x-grpcgo-trl", "trl-v"))
	var last string
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&testpb.EchoMessage{Value: "echo:" + last})
		}
		if err != nil {
			return err
		}
		last = req.GetValue()
	}
}

func (s *grpcGoEcho) ServerStream(req *testpb.EchoMessage, stream grpc.ServerStreamingServer[testpb.EchoMessage]) error {
	_ = grpc.SetHeader(stream.Context(), grpcmd.Pairs("x-grpcgo-hdr", "hdr-v"))
	_ = grpc.SetTrailer(stream.Context(), grpcmd.Pairs("x-grpcgo-trl", "trl-v"))
	for i := 0; i < 3; i++ {
		if err := stream.Send(&testpb.EchoMessage{Value: fmt.Sprintf("%s-%d", req.GetValue(), i)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *grpcGoEcho) Bidi(stream grpc.BidiStreamingServer[testpb.EchoMessage, testpb.EchoMessage]) error {
	_ = grpc.SetHeader(stream.Context(), grpcmd.Pairs("x-grpcgo-hdr", "hdr-v"))
	_ = grpc.SetTrailer(stream.Context(), grpcmd.Pairs("x-grpcgo-trl", "trl-v"))
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&testpb.EchoMessage{Value: "echo:" + req.GetValue()}); err != nil {
			return err
		}
	}
}

type argosServer struct {
	srv  *server.Server
	addr string
	tr   *argoshttp2.Transport
}

func startArgosEchoServer(t *testing.T, bindOpts []composeOpt, extra ...argos.ServerOption) *argosServer {
	t.Helper()
	// The session limits are client-only, so what used to be one option list
	// for this server is now a Config plus the server-side extras the caller
	// adds (argos.WithFilter).
	cfg := &argos.Config{
		MaxConcurrentCalls:     64,
		MaxBufferedBytes:       64 * 16 * 1024 * 1024,
		MaxIdleSessions:        8,
		MaxSessionsPerEndpoint: 8,
		MaxInboundConnIdle:     30 * time.Second,
		MaxInboundConnAge:      30 * time.Minute,
		ListenAddress:          "127.0.0.1:0",
	}

	var srvTr *argoshttp2.Transport
	bound := make(chan struct{})
	srv := server.New(append(append([]argos.ServerOption{argos.WithConfig(cfg)}, extra...),
		argos.WithService(interopService,
			grpcServerService(bindOpts, &srvTr, bound),
			argos.ServiceListenAddress(cfg.ListenAddress),
		))...)
	if err := srv.Register(interopDesc(), argosEchoHandlers()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("argos server protocol not assembled")
	}
	addr := waitAddr(t, srvTr)
	t.Cleanup(func() { _ = srv.Close() })
	return &argosServer{srv: srv, addr: addr, tr: srvTr}
}

func newArgosClient(t *testing.T, addr string, bindOpts []composeOpt, extra ...argos.ClientOption) *client.Client {
	t.Helper()
	cfg := &argos.Config{
		MaxConcurrentCalls:     64,
		MaxBufferedBytes:       64 * 16 * 1024 * 1024,
		MaxIdleSessions:        8,
		MaxSessionsPerEndpoint: 8,
	}
	tFn, fFn, cFn := axesFrom(bindOpts...)
	cli, err := client.New(append([]argos.ClientOption{
		argos.WithConfig(cfg),
		argos.WithServiceName(interopService),
		argos.JoinClient(
			argos.WithTransport(tFn),
			argos.WithFraming(fFn),
			argos.WithCodec(cFn),
		),
		argos.WithTarget("ip://" + addr),
	}, extra...)...)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func startGRPCGoEchoServer(t *testing.T, srvTLS *tls.Config, impl testpb.EchoServer) (addr string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var opts []grpc.ServerOption
	if srvTLS != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(srvTLS)))
	}
	gs := grpc.NewServer(opts...)
	testpb.RegisterEchoServer(gs, impl)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func dialGRPCGo(t *testing.T, addr string, cliTLS *tls.Config, dialOpts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	opts := append([]grpc.DialOption{}, dialOpts...)
	if cliTLS != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(cliTLS)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	cc, err := grpc.NewClient(addr, opts...)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func argosUnaryCall(t *testing.T, cli *client.Client, payload string) (string, metadata.Metadata, metadata.Metadata, error) {
	t.Helper()
	cs, err := cli.Open(context.Background(), descriptor.MustMethod(methodUnary, descriptor.Unary))
	if err != nil {
		return "", nil, nil, err
	}
	defer cs.Close()
	if err := cs.Send(&testpb.EchoMessage{Value: payload}); err != nil {
		return "", nil, nil, err
	}
	if err := cs.HalfClose(); err != nil {
		return "", nil, nil, err
	}
	var got testpb.EchoMessage
	err = cs.Recv(&got)
	// Header is ready after first Recv (or terminal status); calling Header
	// before Recv deadlocks because CallStream only marks headers on Recv.
	hdr, _ := cs.Header()
	trl := cs.Trailer()
	if err != nil {
		return "", hdr, trl, err
	}
	drainRecv(cs)
	return got.GetValue(), hdr, cs.Trailer(), nil
}

func argosClientStreamCall(t *testing.T, cli *client.Client, payloads ...string) (string, error) {
	t.Helper()
	cs, err := cli.Open(context.Background(), descriptor.MustMethod(methodClientStream, descriptor.ClientStreaming))
	if err != nil {
		return "", err
	}
	defer cs.Close()
	for _, p := range payloads {
		if err := cs.Send(&testpb.EchoMessage{Value: p}); err != nil {
			return "", err
		}
	}
	if err := cs.HalfClose(); err != nil {
		return "", err
	}
	var got testpb.EchoMessage
	if err := cs.Recv(&got); err != nil {
		return "", err
	}
	drainRecv(cs)
	return got.GetValue(), nil
}

func argosServerStreamCall(t *testing.T, cli *client.Client, payload string) ([]string, error) {
	t.Helper()
	cs, err := cli.Open(context.Background(), descriptor.MustMethod(methodServerStream, descriptor.ServerStreaming))
	if err != nil {
		return nil, err
	}
	defer cs.Close()
	if err := cs.Send(&testpb.EchoMessage{Value: payload}); err != nil {
		return nil, err
	}
	if err := cs.HalfClose(); err != nil {
		return nil, err
	}
	var out []string
	for {
		var got testpb.EchoMessage
		err := cs.Recv(&got)
		if err == nil {
			out = append(out, got.GetValue())
			continue
		}
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		return out, err
	}
}

func argosBidiCall(t *testing.T, cli *client.Client, payloads ...string) ([]string, error) {
	t.Helper()
	cs, err := cli.Open(context.Background(), descriptor.MustMethod(methodBidi, descriptor.BidiStreaming))
	if err != nil {
		return nil, err
	}
	defer cs.Close()
	var out []string
	for _, p := range payloads {
		if err := cs.Send(&testpb.EchoMessage{Value: p}); err != nil {
			return nil, err
		}
		var got testpb.EchoMessage
		if err := cs.Recv(&got); err != nil {
			return out, err
		}
		out = append(out, got.GetValue())
	}
	if err := cs.HalfClose(); err != nil {
		return out, err
	}
	drainRecv(cs)
	return out, nil
}

// interopTransport is one cell of the §9-6 transport axis (h2c or TLS/ALPN).
// Subtests sharing a listener are sequential; do not t.Parallel() siblings
// that dial the same server.
type interopTransport struct {
	name   string
	srvTLS *tls.Config // nil → h2c
	cliTLS *tls.Config
}

func interopTransports(t *testing.T) []interopTransport {
	t.Helper()
	srvTLS, cliTLS := selfSigned(t)
	return []interopTransport{
		{name: "h2c"},
		{name: "tls", srvTLS: srvTLS, cliTLS: cliTLS},
	}
}

func (tr interopTransport) argosServerOpts() []composeOpt {
	if tr.srvTLS == nil {
		return nil
	}
	return []composeOpt{WithServerTLS(tr.srvTLS)}
}

func (tr interopTransport) argosClientOpts() []composeOpt {
	if tr.cliTLS == nil {
		return nil
	}
	return []composeOpt{
		WithClientTLS(tr.cliTLS),
		WithAuthority("127.0.0.1"),
	}
}

func assertGRPCGoClientShapes(t *testing.T, cli testpb.EchoClient) {
	t.Helper()
	t.Run("unary", func(t *testing.T) {
		rsp, err := cli.Unary(context.Background(), &testpb.EchoMessage{Value: "u"})
		if err != nil {
			t.Fatalf("Unary: %v", err)
		}
		if rsp.GetValue() != "echo:u" {
			t.Fatalf("got %q", rsp.GetValue())
		}
	})
	t.Run("client_stream", func(t *testing.T) {
		st, err := cli.ClientStream(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range []string{"a", "b", "c"} {
			if err := st.Send(&testpb.EchoMessage{Value: v}); err != nil {
				t.Fatal(err)
			}
		}
		rsp, err := st.CloseAndRecv()
		if err != nil {
			t.Fatal(err)
		}
		if rsp.GetValue() != "echo:c" {
			t.Fatalf("got %q", rsp.GetValue())
		}
	})
	t.Run("server_stream", func(t *testing.T) {
		st, err := cli.ServerStream(context.Background(), &testpb.EchoMessage{Value: "s"})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for {
			m, err := st.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, m.GetValue())
		}
		want := []string{"s-0", "s-1", "s-2"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("got %v want %v", got, want)
		}
	})
	t.Run("bidi", func(t *testing.T) {
		st, err := cli.Bidi(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range []string{"x", "y"} {
			if err := st.Send(&testpb.EchoMessage{Value: v}); err != nil {
				t.Fatal(err)
			}
			m, err := st.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if m.GetValue() != "echo:"+v {
				t.Fatalf("got %q", m.GetValue())
			}
		}
		if err := st.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("final Recv = %v, want EOF", err)
		}
	})
}

func assertArgosClientShapes(t *testing.T, cli *client.Client) {
	t.Helper()
	t.Run("unary", func(t *testing.T) {
		got, _, _, err := argosUnaryCall(t, cli, "u")
		if err != nil {
			t.Fatal(err)
		}
		if got != "echo:u" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("client_stream", func(t *testing.T) {
		got, err := argosClientStreamCall(t, cli, "a", "b", "c")
		if err != nil {
			t.Fatal(err)
		}
		if got != "echo:c" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("server_stream", func(t *testing.T) {
		got, err := argosServerStreamCall(t, cli, "s")
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"s-0", "s-1", "s-2"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("got %v want %v", got, want)
		}
	})
	t.Run("bidi", func(t *testing.T) {
		got, err := argosBidiCall(t, cli, "x", "y")
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"echo:x", "echo:y"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("got %v want %v", got, want)
		}
	})
}

// --- OK matrix: 4 shapes × both directions × h2c/TLS ---

func TestInteropOK_Shapes(t *testing.T) {
	for _, tr := range interopTransports(t) {
		tr := tr
		t.Run(tr.name, func(t *testing.T) {
			t.Run("argos_server_grpcgo_client", func(t *testing.T) {
				as := startArgosEchoServer(t, tr.argosServerOpts())
				cc := dialGRPCGo(t, as.addr, tr.cliTLS)
				assertGRPCGoClientShapes(t, testpb.NewEchoClient(cc))
			})
			t.Run("argos_client_grpcgo_server", func(t *testing.T) {
				addr := startGRPCGoEchoServer(t, tr.srvTLS, &grpcGoEcho{})
				cli := newArgosClient(t, addr, tr.argosClientOpts())
				assertArgosClientShapes(t, cli)
			})
		})
	}
}

// --- 17 status codes × both directions × h2c/TLS (unary) ---

func TestInteropStatusCodes_Unary(t *testing.T) {
	codesTable := make([]status.Code, 17)
	for i := range codesTable {
		codesTable[i] = status.Code(i)
	}

	for _, tr := range interopTransports(t) {
		tr := tr
		t.Run(tr.name, func(t *testing.T) {
			t.Run("argos_server_grpcgo_client", func(t *testing.T) {
				as := startArgosEchoServer(t, tr.argosServerOpts())
				cc := dialGRPCGo(t, as.addr, tr.cliTLS)
				cli := testpb.NewEchoClient(cc)

				for _, code := range codesTable {
					code := code
					t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
						rsp, err := cli.Unary(context.Background(), &testpb.EchoMessage{Value: fmt.Sprintf("code:%d", code)})
						if code == status.OK {
							if err != nil {
								t.Fatalf("OK path err: %v", err)
							}
							if rsp.GetValue() != "echo:code:0" {
								t.Fatalf("got %q", rsp.GetValue())
							}
							return
						}
						if err == nil {
							t.Fatal("expected error")
						}
						st, ok := grpcstatus.FromError(err)
						if !ok {
							t.Fatalf("not a grpc status: %v", err)
						}
						if uint32(st.Code()) != uint32(code) {
							t.Fatalf("wire code = %d (%v), want %d", st.Code(), st.Code(), code)
						}
					})
				}
			})

			t.Run("argos_client_grpcgo_server", func(t *testing.T) {
				addr := startGRPCGoEchoServer(t, tr.srvTLS, &grpcGoEcho{})
				cli := newArgosClient(t, addr, tr.argosClientOpts())

				for _, code := range codesTable {
					code := code
					t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
						got, _, _, err := argosUnaryCall(t, cli, fmt.Sprintf("code:%d", code))
						if code == status.OK {
							if err != nil {
								t.Fatalf("OK path err: %v", err)
							}
							if got != "echo:code:0" {
								t.Fatalf("got %q", got)
							}
							return
						}
						if err == nil {
							t.Fatal("expected error")
						}
						if uint32(status.CodeOf(err)) != uint32(code) {
							t.Fatalf("CodeOf = %d, want %d; err=%v", status.CodeOf(err), code, err)
						}
					})
				}
			})
		})
	}
}

// --- Metadata: binary -bin + trailing ---

func TestInteropMetadata_BinaryAndTrailers(t *testing.T) {
	binPayload := string([]byte{0x00, 0xff, 0x10, 0x80, 'a'})

	t.Run("grpcgo_client_to_argos_server", func(t *testing.T) {
		as := startArgosEchoServer(t, nil, argos.WithFilter(func(ctx context.Context, m descriptor.Method, st stream.Stream, next filter.Handler) error {
			md, ok := metadata.FromContext(ctx)
			if !ok {
				t.Error("missing CallMetadata")
				return next(ctx, m, st)
			}
			got := md.IncomingHeaders()["x-trace-bin"]
			if len(got) != 1 || got[0] != binPayload {
				t.Errorf("x-trace-bin = %v, want %q", got, binPayload)
			}
			return next(ctx, m, st)
		}))
		cc := dialGRPCGo(t, as.addr, nil)
		cli := testpb.NewEchoClient(cc)

		ctx := grpcmd.NewOutgoingContext(context.Background(), grpcmd.Pairs(
			"x-trace-bin", binPayload,
			"x-text", "plain",
		))
		var trailer grpcmd.MD
		rsp, err := cli.Unary(ctx, &testpb.EchoMessage{Value: "md"}, grpc.Trailer(&trailer))
		if err != nil {
			t.Fatal(err)
		}
		if rsp.GetValue() != "echo:md" {
			t.Fatalf("got %q", rsp.GetValue())
		}
		if got := trailer.Get("x-argos-trl"); len(got) != 1 || got[0] != "trl-v" {
			t.Fatalf("trailer x-argos-trl = %v", got)
		}
	})

	t.Run("argos_client_to_grpcgo_server", func(t *testing.T) {
		binCh := make(chan []byte, 1)
		addr := startGRPCGoEchoServer(t, nil, &grpcGoEcho{lastBin: binCh})
		cli := newArgosClient(t, addr, nil, argos.WithOpenFilter(func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
			md, ok := metadata.FromContext(ctx)
			if !ok {
				t.Error("missing CallMetadata")
				return next(ctx, m)
			}
			_ = md.AddOutgoingHeader("x-trace-bin", binPayload)
			_ = md.AddOutgoingHeader("x-text", "plain")
			return next(ctx, m)
		}))

		got, hdr, trl, err := argosUnaryCall(t, cli, "md")
		if err != nil {
			t.Fatal(err)
		}
		if got != "echo:md" {
			t.Fatalf("got %q", got)
		}
		select {
		case b := <-binCh:
			if string(b) != binPayload {
				t.Fatalf("server saw bin %q", b)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server did not observe x-trace-bin")
		}
		if vs := hdr["x-grpcgo-hdr"]; len(vs) != 1 || vs[0] != "hdr-v" {
			t.Fatalf("header x-grpcgo-hdr = %v", vs)
		}
		if vs := trl["x-grpcgo-trl"]; len(vs) != 1 || vs[0] != "trl-v" {
			t.Fatalf("trailer x-grpcgo-trl = %v", vs)
		}
	})
}

// --- Trailers-only (argos server ← grpc-go client) ---

func TestInteropTrailersOnly(t *testing.T) {

	// Argos server returns NotFound before any DATA → HTTP/2 trailers-only.
	as := startArgosEchoServer(t, nil, argos.WithFilter(func(ctx context.Context, m descriptor.Method, st stream.Stream, next filter.Handler) error {
		if m.Name() == "Unary" {
			return status.Error(status.NotFound, "missing")
		}
		return next(ctx, m, st)
	}))
	cc := dialGRPCGo(t, as.addr, nil)
	cli := testpb.NewEchoClient(cc)

	var header grpcmd.MD
	_, err := cli.Unary(context.Background(), &testpb.EchoMessage{Value: "x"}, grpc.Header(&header))
	if err == nil {
		t.Fatal("expected NotFound")
	}
	st, ok := grpcstatus.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("got %v, want NotFound", err)
	}
	if uint32(st.Code()) != uint32(status.NotFound) {
		t.Fatalf("numeric code = %d, want %d", st.Code(), status.NotFound)
	}
	// Trailers-only: no user initial metadata.
	if len(header.Get("x-argos-hdr")) != 0 {
		t.Fatalf("unexpected initial metadata: %v", header)
	}
}

// --- Zero-message client-streaming (cardinality / EOF after HalfClose) × h2c/TLS ---

func TestInteropZeroMessageClientStream(t *testing.T) {
	for _, tr := range interopTransports(t) {
		tr := tr
		t.Run(tr.name, func(t *testing.T) {
			t.Run("argos_server_grpcgo_client", func(t *testing.T) {
				as := startArgosEchoServer(t, tr.argosServerOpts())
				cc := dialGRPCGo(t, as.addr, tr.cliTLS)
				st, err := testpb.NewEchoClient(cc).ClientStream(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				rsp, err := st.CloseAndRecv()
				if err != nil {
					t.Fatal(err)
				}
				if rsp.GetValue() != "echo:" {
					t.Fatalf("got %q, want echo: (empty last)", rsp.GetValue())
				}
			})

			t.Run("argos_client_grpcgo_server", func(t *testing.T) {
				addr := startGRPCGoEchoServer(t, tr.srvTLS, &grpcGoEcho{})
				cli := newArgosClient(t, addr, tr.argosClientOpts())
				got, err := argosClientStreamCall(t, cli)
				if err != nil {
					t.Fatal(err)
				}
				if got != "echo:" {
					t.Fatalf("got %q, want echo: (empty last)", got)
				}
			})
		})
	}
}

// --- Half-close timing: client HalfClose / CloseAndRecv then read response × h2c/TLS ---

func TestInteropHalfCloseTiming(t *testing.T) {
	// Client finishes the send half, then reads the aggregated response.
	// Peer must observe EOF and still produce OK (not hang waiting for more).
	for _, tr := range interopTransports(t) {
		tr := tr
		t.Run(tr.name, func(t *testing.T) {
			t.Run("argos_server_grpcgo_client", func(t *testing.T) {
				as := startArgosEchoServer(t, tr.argosServerOpts())
				cc := dialGRPCGo(t, as.addr, tr.cliTLS)
				st, err := testpb.NewEchoClient(cc).ClientStream(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if err := st.Send(&testpb.EchoMessage{Value: "late"}); err != nil {
					t.Fatal(err)
				}
				rsp, err := st.CloseAndRecv()
				if err != nil {
					t.Fatal(err)
				}
				if rsp.GetValue() != "echo:late" {
					t.Fatalf("got %q", rsp.GetValue())
				}
			})

			t.Run("argos_client_grpcgo_server", func(t *testing.T) {
				addr := startGRPCGoEchoServer(t, tr.srvTLS, &grpcGoEcho{})
				cli := newArgosClient(t, addr, tr.argosClientOpts())
				cs, err := cli.Open(context.Background(), descriptor.MustMethod(methodClientStream, descriptor.ClientStreaming))
				if err != nil {
					t.Fatal(err)
				}
				defer cs.Close()
				if err := cs.Send(&testpb.EchoMessage{Value: "late"}); err != nil {
					t.Fatal(err)
				}
				if err := cs.HalfClose(); err != nil {
					t.Fatal(err)
				}
				var got testpb.EchoMessage
				if err := cs.Recv(&got); err != nil {
					t.Fatal(err)
				}
				drainRecv(cs)
				if got.GetValue() != "echo:late" {
					t.Fatalf("got %q", got.GetValue())
				}
			})
		})
	}
}

// --- Details protobuf path (e2e with grpc-go; conflict/corrupt in framing/grpc) ---

func TestInteropDetailsAdoptMessage(t *testing.T) {
	info := &errdetails.ErrorInfo{Reason: "ARGOS", Domain: "interop"}
	detailVal, err := proto.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	detailType := "type.googleapis.com/google.rpc.ErrorInfo"

	t.Run("argos_server_grpcgo_client", func(t *testing.T) {
		as := startArgosEchoServer(t, nil, argos.WithFilter(func(ctx context.Context, m descriptor.Method, st stream.Stream, next filter.Handler) error {
			if m.Name() != "Unary" {
				return next(ctx, m, st)
			}
			return status.WithDetails(
				status.Error(status.FailedPrecondition, "from details proto"),
				status.Detail{TypeURL: detailType, Value: detailVal},
			)
		}))
		cc := dialGRPCGo(t, as.addr, nil)
		_, err := testpb.NewEchoClient(cc).Unary(context.Background(), &testpb.EchoMessage{Value: "d"})
		if err == nil {
			t.Fatal("expected error")
		}
		st, ok := grpcstatus.FromError(err)
		if !ok {
			t.Fatalf("not grpc status: %v", err)
		}
		if st.Code() != codes.FailedPrecondition {
			t.Fatalf("code = %v", st.Code())
		}
		if st.Message() != "from details proto" {
			t.Fatalf("message = %q", st.Message())
		}
		if len(st.Details()) == 0 {
			t.Fatal("expected details on grpc-go status")
		}
	})

	t.Run("argos_client_grpcgo_server", func(t *testing.T) {
		addr := startGRPCGoEchoServer(t, nil, &grpcGoEcho{forceDetails: true})
		cli := newArgosClient(t, addr, nil)
		_, _, _, err := argosUnaryCall(t, cli, "d")
		if err == nil {
			t.Fatal("expected error")
		}
		if status.CodeOf(err) != status.FailedPrecondition {
			t.Fatalf("CodeOf = %v, want FailedPrecondition; err=%v", status.CodeOf(err), err)
		}
		if err.Error() != "from details proto" {
			t.Fatalf("message = %q, want details message", err.Error())
		}
		ds := status.DetailsOf(err)
		if len(ds) == 0 {
			t.Fatal("expected details attached to argos error")
		}
	})
}

// --- gzip smoke both directions ---

func TestInteropGzipSmoke(t *testing.T) {
	compOpts := []composeOpt{
		WithCompressor(gzip.New()),
		composeWithSendCompressor(gzip.Name),
	}

	t.Run("argos_server_grpcgo_client", func(t *testing.T) {
		as := startArgosEchoServer(t, compOpts)
		cc := dialGRPCGo(t, as.addr, nil)
		cli := testpb.NewEchoClient(cc)

		rsp, err := cli.Unary(context.Background(), &testpb.EchoMessage{Value: "gz"},
			grpc.UseCompressor(grpcgzip.Name))
		if err != nil {
			t.Fatal(err)
		}
		if rsp.GetValue() != "echo:gz" {
			t.Fatalf("got %q", rsp.GetValue())
		}
	})

	t.Run("argos_client_grpcgo_server", func(t *testing.T) {
		addr := startGRPCGoEchoServer(t, nil, &grpcGoEcho{})
		cli := newArgosClient(t, addr, compOpts)
		got, _, _, err := argosUnaryCall(t, cli, "gz")
		if err != nil {
			t.Fatal(err)
		}
		if got != "echo:gz" {
			t.Fatalf("got %q", got)
		}
	})
}

// --- deadline / timeout propagation ---

func TestInteropDeadline(t *testing.T) {

	t.Run("grpcgo_client_deadline_reaches_argos", func(t *testing.T) {
		saw := make(chan time.Duration, 1)
		as := startArgosEchoServer(t, nil, argos.WithFilter(func(ctx context.Context, m descriptor.Method, st stream.Stream, next filter.Handler) error {
			if dl, ok := ctx.Deadline(); ok {
				saw <- time.Until(dl)
			} else {
				saw <- 0
			}
			return next(ctx, m, st)
		}))
		cc := dialGRPCGo(t, as.addr, nil)
		cli := testpb.NewEchoClient(cc)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err := cli.Unary(ctx, &testpb.EchoMessage{Value: "dl"})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case rem := <-saw:
			if rem <= 0 || rem > 200*time.Millisecond {
				t.Fatalf("remaining deadline %v, want (0,200ms]", rem)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server did not observe deadline")
		}
	})

	t.Run("argos_client_deadline_reaches_grpcgo", func(t *testing.T) {
		saw := make(chan time.Duration, 1)
		addr := startGRPCGoEchoServer(t, nil, &grpcGoEcho{sawDeadline: saw})
		cli := newArgosClient(t, addr, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		cs, err := cli.Open(ctx, descriptor.MustMethod(methodUnary, descriptor.Unary))
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		if err := cs.Send(&testpb.EchoMessage{Value: "dl"}); err != nil {
			t.Fatal(err)
		}
		if err := cs.HalfClose(); err != nil {
			t.Fatal(err)
		}
		var got testpb.EchoMessage
		if err := cs.Recv(&got); err != nil {
			t.Fatal(err)
		}
		drainRecv(cs)
		select {
		case rem := <-saw:
			if rem <= 0 || rem > 200*time.Millisecond {
				t.Fatalf("remaining deadline %v, want (0,200ms]", rem)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("grpc-go server did not observe deadline")
		}
	})
}
