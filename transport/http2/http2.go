// Package http2 implements gRPC-over-HTTP/2 (h2c) for Argos.
package http2

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	grpcContentType = "application/grpc"
	trailerStatus   = "grpc-status"
	trailerMessage  = "grpc-message"
)

type channel struct {
	listen *transport.ListenState
}

// New creates an h2c transport compatible with grpcurl.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

func init() {
	transport.Register("http2", New)
}

// Addr returns the listener address after ListenAndServe has started.
func (t *channel) Addr() net.Addr {
	return t.listen.BoundAddr()
}

func (t *channel) ListenAndServe(
	ctx context.Context,
	onCall func(context.Context, string, transport.Framer) error,
	opts ...transport.ServerOption,
) error {
	serve := transport.ApplyServerOptions(opts)
	listener, err := transport.ListenTCP(serve)
	var bound net.Addr
	if listener != nil {
		bound = listener.Addr()
	}
	t.listen.MarkListen(serve.ListenAddress, bound, err)
	if err != nil {
		return err
	}

	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serveCall(w, r, onCall)
		}),
		Protocols: protocols,
	}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()

	err = server.Serve(listener)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	call := transport.ApplyClientOptions(opts)
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	return &clientFramer{
		ctx:      ctx,
		endpoint: "http://" + addr + "/" + method,
		metadata: metadata.FromContext(ctx),
		client:   h2cClient(),
		ready:    make(chan struct{}),
	}, nil
}

func h2cClient() *http.Client {
	transport := &http.Transport{}
	transport.Protocols = &http.Protocols{}
	transport.Protocols.SetUnencryptedHTTP2(true)
	return &http.Client{Transport: transport}
}

func serveCall(
	w http.ResponseWriter,
	r *http.Request,
	onCall func(context.Context, string, transport.Framer) error,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !isGRPCContentType(r.Header.Get("Content-Type")) {
		writeStatusTrailers(w, nil, errs.Error(errs.InvalidArgument, "invalid content-type"))
		return
	}
	if !strings.Contains(strings.ToLower(r.Header.Get("Te")), "trailers") {
		writeStatusTrailers(w, nil, errs.Error(errs.InvalidArgument, "missing te: trailers"))
		return
	}

	method := strings.TrimPrefix(r.URL.Path, "/")
	if method == "" || strings.Count(method, "/") != 1 {
		writeStatusTrailers(w, nil, errs.Error(errs.NotFound, "invalid method path"))
		return
	}

	ctx := metadata.With(r.Context(), metadataFromHeaders(r.Header))
	f := &serverFramer{
		ctx:      ctx,
		request:  r.Body,
		response: w,
		flusher:  httpFlusher(w),
	}
	callErr := onCall(ctx, method, f)
	writeStatusTrailers(w, f, callErr)
}

func isGRPCContentType(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return value == grpcContentType || value == "application/grpc+proto"
}

func metadataFromHeaders(headers http.Header) metadata.Metadata {
	md := make(metadata.Metadata)
	for key, values := range headers {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, ":") {
			continue
		}
		switch lower {
		case "content-type", "te", "user-agent", "grpc-timeout", "grpc-encoding",
			"grpc-accept-encoding", "grpc-message-type":
			continue
		}
		if strings.HasPrefix(lower, "grpc-") {
			continue
		}
		md[lower] = append([]string(nil), values...)
	}
	return md
}

type serverFramer struct {
	mu sync.Mutex

	ctx      context.Context
	request  io.ReadCloser
	response http.ResponseWriter
	flusher  http.Flusher

	headersSent bool
	closeSend   bool
	sentEnd     bool
}

func (f *serverFramer) Recv() (io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	payload, err := readLPM(f.request)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(payload), nil
}

func (f *serverFramer) Send() (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sentEnd || f.closeSend {
		return nil, errors.New("http2: send after CloseSend")
	}
	if err := f.ensureHeaders(); err != nil {
		return nil, err
	}
	return &messageWriter{send: func(payload []byte) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.sentEnd || f.closeSend {
			return errors.New("http2: send after CloseSend")
		}
		return writeLPM(f.response, payload)
	}}, nil
}

func (f *serverFramer) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeSend = true
	return nil
}

func (f *serverFramer) ensureHeaders() error {
	if f.headersSent {
		return nil
	}
	f.response.Header().Set("Content-Type", grpcContentType)
	f.response.WriteHeader(http.StatusOK)
	f.headersSent = true
	return nil
}

func writeStatusTrailers(w http.ResponseWriter, f *serverFramer, callErr error) {
	code := errs.OK
	msg := ""
	if callErr != nil {
		code = errs.CodeOf(callErr)
		msg = callErr.Error()
	}

	if f != nil {
		_ = f.ensureHeaders()
		if f.flusher != nil {
			f.flusher.Flush()
		}
	} else {
		w.Header().Set("Content-Type", grpcContentType)
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}

	w.Header().Set(http.TrailerPrefix+trailerStatus, strconv.FormatUint(uint64(statusmap.GRPCStatus(code)), 10))
	w.Header().Set(http.TrailerPrefix+trailerMessage, msg)
}

type clientFramer struct {
	mu sync.Mutex

	ctx      context.Context
	endpoint string
	metadata metadata.Metadata
	client   *http.Client

	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	started    bool
	closeSend  bool
	ready      chan struct{}

	resp        *http.Response
	respErr     error
	bodyReader  io.Reader
	terminalErr error
}

func (f *clientFramer) Send() (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closeSend {
		return nil, errors.New("http2: send after CloseSend")
	}
	if !f.started {
		pr, pw := io.Pipe()
		f.pipeReader = pr
		f.pipeWriter = pw
		f.started = true
		go f.doRequest()
	}
	return &messageWriter{send: func(payload []byte) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.closeSend {
			return errors.New("http2: send after CloseSend")
		}
		return writeLPM(f.pipeWriter, payload)
	}}, nil
}

func (f *clientFramer) doRequest() {
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.endpoint, f.pipeReader)
	if err != nil {
		f.finishRequest(nil, err)
		return
	}
	req.Header.Set("Content-Type", grpcContentType)
	req.Header.Set("Te", "trailers")
	for key, values := range f.metadata {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	resp, err := f.client.Do(req)
	f.finishRequest(resp, err)
}

func (f *clientFramer) finishRequest(resp *http.Response, err error) {
	f.mu.Lock()
	f.resp = resp
	f.respErr = err
	f.mu.Unlock()
	close(f.ready)
}

func (f *clientFramer) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closeSend {
		return nil
	}
	f.closeSend = true
	if f.pipeWriter != nil {
		return f.pipeWriter.Close()
	}
	return nil
}

func (f *clientFramer) Recv() (io.Reader, error) {
	if err := f.waitResponse(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.terminalErr != nil {
		return nil, f.terminalErr
	}
	if f.bodyReader == nil {
		f.bodyReader = f.resp.Body
	}

	payload, err := readLPM(f.bodyReader)
	if err == io.EOF {
		return nil, f.finish(trailerError(f.resp))
	}
	if err != nil {
		return nil, f.finish(err)
	}
	return bytes.NewReader(payload), nil
}

func (f *clientFramer) waitResponse() error {
	f.mu.Lock()
	started := f.started
	f.mu.Unlock()
	if !started {
		return errors.New("http2: receive before send")
	}
	<-f.ready
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.respErr
}

func (f *clientFramer) finish(err error) error {
	f.terminalErr = err
	if f.resp != nil && f.resp.Body != nil {
		_ = f.resp.Body.Close()
	}
	return err
}

func trailerError(resp *http.Response) error {
	if resp == nil {
		return io.EOF
	}
	statusText := resp.Trailer.Get(trailerStatus)
	if statusText == "" {
		statusText = resp.Header.Get(trailerStatus)
	}
	if statusText == "" || statusText == "0" {
		return io.EOF
	}
	code, err := strconv.ParseUint(statusText, 10, 32)
	if err != nil {
		return errs.Error(errs.Unknown, "invalid grpc-status trailer")
	}
	msg := resp.Trailer.Get(trailerMessage)
	if msg == "" {
		msg = resp.Header.Get(trailerMessage)
	}
	return errs.Error(statusmap.FromGRPCStatus(uint32(code)), msg)
}

func writeLPM(w io.Writer, payload []byte) error {
	if len(payload) > int(^uint32(0)) {
		return errors.New("http2: message is too large")
	}
	var header [5]byte
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readLPM(r io.Reader) ([]byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	if header[0] != 0 {
		return nil, errors.New("http2: compressed messages are not supported")
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length == 0 {
		return nil, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

type messageWriter struct {
	bytes.Buffer
	once sync.Once
	send func([]byte) error
	err  error
}

func (w *messageWriter) Close() error {
	w.once.Do(func() {
		w.err = w.send(w.Bytes())
	})
	return w.err
}

func httpFlusher(w http.ResponseWriter) http.Flusher {
	flusher, _ := w.(http.Flusher)
	return flusher
}
