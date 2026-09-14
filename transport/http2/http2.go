// Package http2 implements gRPC-over-HTTP/2 (h2c) for Argos.
package http2

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

const (
	grpcContentType = "application/grpc"
	trailerStatus   = "grpc-status"
	trailerMessage  = "grpc-message"
)

type channel struct {
	listen *transport.ListenState
	client *http.Client
}

// New creates an h2c transport compatible with grpcurl.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState(), client: h2cClient()}
}

func (*channel) TransportName() string { return "http2" }

func (*channel) SupportsStreaming() bool { return true }

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
	if ctx == nil {
		return errors.New("http2: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if onCall == nil {
		return errors.New("http2: nil onCall")
	}
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
			serveCall(w, r, onCall, transport.ResolveMaxMessageSize(serve.MaxMessageSize))
		}),
		Protocols:      protocols,
		MaxHeaderBytes: serve.MaxHeaderBytes,
	}
	serveDone := make(chan struct{})
	defer close(serveDone)
	defer func() { _ = server.Close() }()
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-serveDone:
		}
	}()

	err = server.Serve(listener)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	if ctx == nil {
		return nil, errors.New("http2: nil context")
	}
	call := transport.ApplyClientOptions(opts)
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithCancel(ctx)
	client := t.client
	if client == nil {
		client = h2cClient()
	}
	return &clientFramer{
		ctx:            callCtx,
		endpoint:       (&url.URL{Scheme: "http", Host: addr, Path: "/" + method}).String(),
		metadata:       metadata.FromContext(callCtx),
		client:         client,
		ready:          make(chan struct{}),
		cancel:         cancel,
		maxMessageSize: transport.ResolveMaxMessageSize(call.MaxMessageSize),
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
	maxMessageSize int64,
) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !isGRPCContentType(r.Header.Get("Content-Type")) {
		writeStatusTrailers(w, nil, errs.Error(errs.InvalidArgument, "invalid content-type"), maxMessageSize)
		return
	}
	if !strings.Contains(strings.ToLower(r.Header.Get("Te")), "trailers") {
		writeStatusTrailers(w, nil, errs.Error(errs.InvalidArgument, "missing te: trailers"), maxMessageSize)
		return
	}

	method := strings.TrimPrefix(r.URL.Path, "/")
	if method == "" || strings.Count(method, "/") != 1 {
		writeStatusTrailers(w, nil, errs.Error(errs.NotFound, "invalid method path"), maxMessageSize)
		return
	}

	md, err := metadataFromHeaders(r.Header)
	if err != nil {
		writeStatusTrailers(w, nil, err, maxMessageSize)
		return
	}
	ctx := metadata.With(r.Context(), md)
	f := &serverFramer{
		ctx:            ctx,
		request:        r.Body,
		response:       w,
		flusher:        httpFlusher(w),
		maxMessageSize: maxMessageSize,
	}
	defer func() { _ = f.Close() }()
	callErr := onCall(ctx, method, f)
	writeStatusTrailers(w, f, callErr, maxMessageSize)
}

func isGRPCContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return false
	}
	mediaType = strings.ToLower(mediaType)
	return mediaType == grpcContentType || strings.HasPrefix(mediaType, grpcContentType+"+")
}

func metadataFromHeaders(headers http.Header) (metadata.Metadata, error) {
	md := make(metadata.Metadata)
	for key, values := range headers {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, ":") {
			continue
		}
		if isReservedHeader(lower) {
			continue
		}
		if strings.HasPrefix(lower, "grpc-") {
			continue
		}
		md[lower] = append([]string(nil), values...)
	}
	if err := validateMetadata(md); err != nil {
		return nil, errs.Error(errs.InvalidArgument, err.Error())
	}
	return md, nil
}

func validateMetadata(md metadata.Metadata) error {
	var size int64
	for key, values := range md {
		if isReservedHeader(key) {
			continue
		}
		for _, value := range values {
			entrySize := int64(len(key)) + int64(len(value))
			if entrySize > transport.DefaultMaxMetadataSize ||
				size > transport.DefaultMaxMetadataSize-entrySize {
				return errors.New("http2: metadata exceeds configured limit")
			}
			size += entrySize
		}
	}
	return nil
}

func isReservedHeader(key string) bool {
	key = strings.ToLower(key)
	if strings.HasPrefix(key, ":") || strings.HasPrefix(key, "grpc-") {
		return true
	}
	switch key {
	case "content-type", "content-length", "host", "connection", "keep-alive",
		"proxy-authenticate", "proxy-authorization", "te", "trailer",
		"transfer-encoding", "upgrade", "user-agent", "accept", "accept-encoding",
		"grpc-timeout", "grpc-encoding", "grpc-accept-encoding", "grpc-message-type":
		return true
	default:
		return false
	}
}

type serverFramer struct {
	stateMu sync.Mutex
	recvMu  sync.Mutex
	sendMu  sync.Mutex

	ctx      context.Context
	request  io.ReadCloser
	response http.ResponseWriter
	flusher  http.Flusher

	headersSent    bool
	closeSend      bool
	sentEnd        bool
	closed         bool
	maxMessageSize int64
}

func (f *serverFramer) Recv() (io.Reader, error) {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	f.stateMu.Lock()
	if f.closed {
		f.stateMu.Unlock()
		return nil, net.ErrClosed
	}
	request := f.request
	f.stateMu.Unlock()

	payload, err := readLPM(request, f.maxMessageSize)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(payload), nil
}

func (f *serverFramer) Send() (io.WriteCloser, error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	f.stateMu.Lock()
	closed := f.closed
	f.stateMu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	if f.sentEnd || f.closeSend {
		return nil, errors.New("http2: send after CloseSend")
	}
	if err := f.ensureHeadersLocked(); err != nil {
		return nil, err
	}
	return &messageWriter{send: func(payload []byte) error {
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		f.stateMu.Lock()
		closed := f.closed
		f.stateMu.Unlock()
		if closed {
			return net.ErrClosed
		}
		if f.sentEnd || f.closeSend {
			return errors.New("http2: send after CloseSend")
		}
		if err := writeLPM(f.response, payload, f.maxMessageSize); err != nil {
			return err
		}
		if f.flusher != nil {
			f.flusher.Flush()
		}
		return nil
	}}, nil
}

func (f *serverFramer) CloseSend() error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	f.stateMu.Lock()
	closed := f.closed
	f.stateMu.Unlock()
	if closed {
		return net.ErrClosed
	}
	f.closeSend = true
	return nil
}

// Close terminates the server-side HTTP/2 call by closing the request body.
func (f *serverFramer) Close() error {
	f.stateMu.Lock()
	if f.closed {
		f.stateMu.Unlock()
		return nil
	}
	f.closed = true
	request := f.request
	f.stateMu.Unlock()
	if request != nil {
		return request.Close()
	}
	return nil
}

func (f *serverFramer) ensureHeaders() error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	return f.ensureHeadersLocked()
}

func (f *serverFramer) ensureHeadersLocked() error {
	if f.headersSent {
		return nil
	}
	f.response.Header().Set("Content-Type", grpcContentType)
	f.response.WriteHeader(http.StatusOK)
	f.headersSent = true
	return nil
}

func writeStatusTrailers(w http.ResponseWriter, f *serverFramer, callErr error, maxMessageSize int64) {
	code := errs.OK
	msg := ""
	if callErr != nil {
		code = errs.CodeOf(callErr)
		msg = statusmap.LimitStatusMessage(callErr.Error(), maxMessageSize)
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
	w.Header().Set(http.TrailerPrefix+trailerMessage, url.PathEscape(msg))
}

type clientFramer struct {
	stateMu sync.Mutex
	sendMu  sync.Mutex
	recvMu  sync.Mutex

	ctx      context.Context
	cancel   context.CancelFunc
	endpoint string
	metadata metadata.Metadata
	client   *http.Client

	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	started    bool
	closeSend  bool
	ready      chan struct{}
	readyOnce  sync.Once

	resp           *http.Response
	respErr        error
	bodyReader     io.Reader
	terminalErr    error
	closed         bool
	closeOnce      sync.Once
	closeErr       error
	maxMessageSize int64
}

func (f *clientFramer) Send() (io.WriteCloser, error) {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	f.stateMu.Lock()
	if f.closed {
		f.stateMu.Unlock()
		return nil, net.ErrClosed
	}
	if f.closeSend {
		f.stateMu.Unlock()
		return nil, errors.New("http2: send after CloseSend")
	}
	if !f.started {
		pr, pw := io.Pipe()
		f.pipeReader = pr
		f.pipeWriter = pw
		f.started = true
		go f.doRequest()
	}
	f.stateMu.Unlock()
	return &messageWriter{send: func(payload []byte) error {
		if ctxErr := f.contextErr(); ctxErr != nil {
			return ctxErr
		}
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		f.stateMu.Lock()
		closed := f.closed
		closeSend := f.closeSend
		pipeWriter := f.pipeWriter
		f.stateMu.Unlock()
		if closed {
			return net.ErrClosed
		}
		if closeSend {
			return errors.New("http2: send after CloseSend")
		}
		return f.contextOr(writeLPM(pipeWriter, payload, f.maxMessageSize))
	}}, nil
}

func (f *clientFramer) doRequest() {
	f.stateMu.Lock()
	ctx := f.ctx
	endpoint := f.endpoint
	pipeReader := f.pipeReader
	metadata := metadata.Clone(f.metadata)
	f.stateMu.Unlock()
	if err := validateMetadata(metadata); err != nil {
		f.finishRequest(nil, err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, pipeReader)
	if err != nil {
		f.finishRequest(nil, err)
		return
	}
	req.Header.Set("Content-Type", grpcContentType)
	req.Header.Set("Te", "trailers")
	for key, values := range metadata {
		if isReservedHeader(key) {
			continue
		}
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	resp, err := f.client.Do(req)
	f.finishRequest(resp, err)
}

func (f *clientFramer) finishRequest(resp *http.Response, err error) {
	f.stateMu.Lock()
	closed := f.closed
	pipeReader := f.pipeReader
	if !closed {
		f.resp = resp
		f.respErr = err
	}
	f.stateMu.Unlock()
	if err != nil && pipeReader != nil {
		// An early request failure (for example, invalid metadata or an
		// invalid URL) can happen before net/http starts consuming the pipe.
		// Closing the reader propagates the failure to a writer that was
		// already returned by Send instead of leaving it blocked forever.
		_ = pipeReader.CloseWithError(err)
	}
	if closed {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return
	}
	f.readyOnce.Do(func() { close(f.ready) })
}

func (f *clientFramer) CloseSend() error {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	f.stateMu.Lock()
	if f.closed {
		f.stateMu.Unlock()
		return net.ErrClosed
	}
	if f.closeSend {
		f.stateMu.Unlock()
		return nil
	}
	if !f.started {
		pr, pw := io.Pipe()
		f.pipeReader = pr
		f.pipeWriter = pw
		f.started = true
		go f.doRequest()
	}
	f.closeSend = true
	pipeWriter := f.pipeWriter
	f.stateMu.Unlock()
	if pipeWriter != nil {
		return f.contextOr(pipeWriter.Close())
	}
	return nil
}

// Close terminates the HTTP/2 call, cancels an in-flight request and closes
// any response body or request pipe owned by the framer.
func (f *clientFramer) Close() error {
	f.closeOnce.Do(func() {
		if f.cancel != nil {
			f.cancel()
		}
		f.stateMu.Lock()
		f.closed = true
		pipeWriter := f.pipeWriter
		response := f.resp
		f.stateMu.Unlock()
		if pipeWriter != nil {
			_ = pipeWriter.Close()
		}
		if response != nil && response.Body != nil {
			f.closeErr = response.Body.Close()
		}
	})
	return f.closeErr
}

func (f *clientFramer) Recv() (io.Reader, error) {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	if err := f.waitResponse(); err != nil {
		return nil, err
	}

	f.stateMu.Lock()
	if f.terminalErr != nil {
		err := f.terminalErr
		f.stateMu.Unlock()
		return nil, err
	}
	if ctxErr := f.contextErr(); ctxErr != nil {
		f.stateMu.Unlock()
		return nil, ctxErr
	}
	if f.closed {
		f.stateMu.Unlock()
		return nil, net.ErrClosed
	}
	resp := f.resp
	bodyReader := f.bodyReader
	if bodyReader == nil && resp != nil {
		bodyReader = resp.Body
		f.bodyReader = bodyReader
	}
	maxMessageSize := f.maxMessageSize
	f.stateMu.Unlock()

	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	payload, err := readLPM(bodyReader, maxMessageSize)
	if err == io.EOF {
		return nil, f.finish(trailerError(resp))
	}
	if err != nil {
		return nil, f.finish(f.contextOr(err))
	}
	return bytes.NewReader(payload), nil
}

func (f *clientFramer) waitResponse() error {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	f.stateMu.Lock()
	started := f.started
	closed := f.closed
	f.stateMu.Unlock()
	if closed {
		return net.ErrClosed
	}
	if !started {
		return errors.New("http2: receive before send")
	}
	select {
	case <-f.ready:
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if f.respErr != nil {
		return f.contextOr(f.respErr)
	}
	if f.closed {
		return net.ErrClosed
	}
	return nil
}

func (f *clientFramer) finish(err error) error {
	err = f.contextOr(err)
	f.stateMu.Lock()
	if f.terminalErr == nil {
		f.terminalErr = err
	}
	response := f.resp
	terminal := f.terminalErr
	f.stateMu.Unlock()
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	return terminal
}

func (f *clientFramer) contextErr() error {
	if f.ctx == nil {
		return nil
	}
	return f.ctx.Err()
}

func (f *clientFramer) contextOr(err error) error {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func trailerError(resp *http.Response) error {
	if resp == nil {
		return errors.New("http2: missing grpc-status trailer")
	}
	statusText := headerValue(resp.Trailer, trailerStatus)
	if statusText == "" {
		statusText = headerValue(resp.Header, trailerStatus)
	}
	if statusText == "" {
		return errors.New("http2: missing grpc-status trailer")
	}
	if statusText == "0" {
		return io.EOF
	}
	code, err := strconv.ParseUint(statusText, 10, 32)
	if err != nil {
		return errs.Error(errs.Unknown, "invalid grpc-status trailer")
	}
	msg := headerValue(resp.Trailer, trailerMessage)
	if msg == "" {
		msg = headerValue(resp.Header, trailerMessage)
	}
	msg, err = url.PathUnescape(msg)
	if err != nil {
		return errs.Error(errs.Unknown, "invalid grpc-message trailer")
	}
	return errs.Error(statusmap.FromGRPCStatus(uint32(code)), statusmap.LimitStatusMessage(msg, statusmap.MaxStatusMessageBytes))
}

func headerValue(headers http.Header, key string) string {
	if value := headers.Get(key); value != "" {
		return value
	}
	for name, values := range headers {
		if strings.EqualFold(name, key) && len(values) != 0 {
			return values[0]
		}
	}
	return ""
}

func writeLPM(w io.Writer, payload []byte, limits ...int64) error {
	if len(limits) > 0 && limits[0] > 0 && int64(len(payload)) > limits[0] {
		return errors.New("http2: message exceeds configured limit")
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return errors.New("http2: message is too large")
	}
	var header [5]byte
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func readLPM(r io.Reader, limits ...int64) ([]byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	if header[0] != 0 {
		return nil, errors.New("http2: compressed messages are not supported")
	}
	length := binary.BigEndian.Uint32(header[1:])
	if len(limits) > 0 && limits[0] > 0 && int64(length) > limits[0] {
		return nil, errors.New("http2: message exceeds configured limit")
	}
	if length == 0 {
		return nil, nil
	}
	if uint64(length) > uint64(^uint(0)>>1) {
		return nil, errors.New("http2: message length overflows int")
	}
	payload := make([]byte, int(length))
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
