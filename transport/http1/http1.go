// Package http1 implements unary Argos calls over HTTP/1.1.
package http1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/limits"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

type channel struct {
	listen *transport.ListenState
}

var noRedirectHTTPClient = &http.Client{
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// Error responses are control data, not application messages. Keep their
// bound independent from MaxMessageSize so a small application limit can still
// carry a structured error back to the caller.
const maxErrorBodyBytes int64 = 64 << 10

// New creates an HTTP/1.1 transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

func (*channel) TransportName() string { return "http1" }

func (*channel) SupportsStreaming() bool { return false }

func init() {
	transport.Register("http1", New)
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
		return errors.New("http1: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if onCall == nil {
		return errors.New("http1: nil onCall")
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

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serveCall(w, r, onCall, transport.ResolveMaxMessageSize(serve.MaxMessageSize))
		}),
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
		return nil, errors.New("http1: nil context")
	}
	call := transport.ApplyClientOptions(opts)
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithCancel(ctx)
	endpoint := url.URL{Scheme: "http", Host: addr, Path: "/" + method}
	return &clientFramer{
		ctx:            callCtx,
		endpoint:       endpoint.String(),
		metadata:       metadata.FromContext(callCtx),
		client:         noRedirectHTTPClient,
		cancel:         cancel,
		maxMessageSize: transport.ResolveMaxMessageSize(call.MaxMessageSize),
	}, nil
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
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		writeError(w, errs.Error(errs.InvalidArgument, "invalid content-type"), maxMessageSize)
		return
	}
	method := strings.TrimPrefix(r.URL.Path, "/")
	if method == "" || strings.Count(method, "/") != 1 {
		writeError(w, errs.Error(errs.NotFound, "invalid method path"), maxMessageSize)
		return
	}

	md, err := metadataFromHeaders(r.Header)
	if err != nil {
		writeError(w, err, maxMessageSize)
		return
	}
	ctx := metadata.With(r.Context(), md)
	// The strict reader probes one extra byte so a body whose payload is exactly
	// maxMessageSize is accepted while an oversized body is rejected.
	f := &serverFramer{
		request: &limitedReadCloser{
			Reader: limits.NewReader(r.Body, maxMessageSize),
			closer: r.Body,
		},
		response:       w,
		maxMessageSize: maxMessageSize,
	}
	defer func() { _ = f.Close() }()
	err = onCall(ctx, method, f)
	if err != nil && !f.hasSent() {
		writeError(w, err, maxMessageSize)
		return
	}
	if !f.hasSent() {
		w.WriteHeader(http.StatusOK)
	}
}

func metadataFromHeaders(headers http.Header) (metadata.Metadata, error) {
	md := make(metadata.Metadata, len(headers))
	for key, values := range headers {
		lower := strings.ToLower(key)
		if isReservedHeader(lower) {
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
				return errors.New("http1: metadata exceeds configured limit")
			}
			size += entrySize
		}
	}
	return nil
}

func isReservedHeader(key string) bool {
	switch strings.ToLower(key) {
	case "content-type", "content-length", "host", "connection", "keep-alive",
		"proxy-authenticate", "proxy-authorization", "te", "trailer",
		"transfer-encoding", "upgrade", "user-agent", "accept", "accept-encoding":
		return true
	default:
		return false
	}
}

func isJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

type serverFramer struct {
	mu             sync.Mutex
	request        io.Reader
	response       http.ResponseWriter
	maxMessageSize int64
	received       bool
	sent           bool
	committed      bool
	closeSend      bool
	closed         bool
}

func (f *serverFramer) Recv() (io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, net.ErrClosed
	}
	if f.received {
		return nil, errors.New("http1: unary stream received more than one request")
	}
	f.received = true
	return f.request, nil
}

func (f *serverFramer) Send() (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, net.ErrClosed
	}
	if f.closeSend {
		return nil, errors.New("http1: send after CloseSend")
	}
	if f.sent {
		return nil, errors.New("http1: unary stream sent more than one response")
	}
	f.sent = true
	return &serverWriteCloser{framer: f}, nil
}

func (f *serverFramer) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return net.ErrClosed
	}
	f.closeSend = true
	return nil
}

// Close releases the request body. The HTTP server owns the response writer
// and finalizes it when the handler returns.
func (f *serverFramer) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	request := f.request
	f.mu.Unlock()
	if closer, ok := request.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (f *serverFramer) hasSent() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.committed
}

type clientFramer struct {
	mu             sync.Mutex
	ctx            context.Context
	endpoint       string
	metadata       metadata.Metadata
	client         *http.Client
	response       *http.Response
	sendUsed       bool
	recvUsed       bool
	closeSend      bool
	closed         bool
	cancel         context.CancelFunc
	closeOnce      sync.Once
	closeErr       error
	maxMessageSize int64
}

func (f *clientFramer) Send() (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	if f.closed {
		return nil, net.ErrClosed
	}
	if f.closeSend {
		return nil, errors.New("http1: send after CloseSend")
	}
	if f.sendUsed {
		return nil, errors.New("http1: unary stream sent more than one request")
	}
	f.sendUsed = true
	return &messageWriter{send: f.sendRequest}, nil
}

func (f *clientFramer) sendRequest(body []byte) error {
	f.mu.Lock()
	closed := f.closed
	closeSend := f.closeSend
	ctx := f.ctx
	endpoint := f.endpoint
	client := f.client
	requestMetadata := metadata.Clone(f.metadata)
	maxMessageSize := f.maxMessageSize
	f.mu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if closed {
		return net.ErrClosed
	}
	if closeSend {
		return errors.New("http1: send after CloseSend")
	}
	if int64(len(body)) > maxMessageSize {
		return fmt.Errorf("http1: message is %d bytes; maximum is %d", len(body), maxMessageSize)
	}
	if err := validateMetadata(requestMetadata); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	for key, values := range requestMetadata {
		if isReservedHeader(key) {
			continue
		}
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return f.contextOr(err)
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		if ctxErr := f.contextErr(); ctxErr != nil {
			_ = response.Body.Close()
			return ctxErr
		}
		_ = response.Body.Close()
		return net.ErrClosed
	}
	f.response = response
	f.mu.Unlock()
	return nil
}

func (f *clientFramer) Recv() (io.Reader, error) {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		if ctxErr := f.contextErr(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, net.ErrClosed
	}
	if f.recvUsed {
		f.mu.Unlock()
		return nil, errors.New("http1: unary stream received more than one response")
	}
	f.recvUsed = true
	response := f.response
	maxMessageSize := f.maxMessageSize
	f.mu.Unlock()
	if response == nil {
		return nil, errors.New("http1: receive before request")
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		var status errorBody
		limited := limits.NewReader(response.Body, maxErrorBodyBytes)
		body, err := io.ReadAll(limited)
		if err != nil {
			return nil, fmt.Errorf("http1: HTTP %d: error body exceeds configured limit: %w", response.StatusCode, f.contextOr(err))
		}
		if err := json.Unmarshal(body, &status); err != nil {
			return nil, fmt.Errorf("http1: HTTP %d: invalid error body: %w", response.StatusCode, err)
		}
		if status.Code == errs.OK {
			return nil, fmt.Errorf("http1: HTTP %d: error body has OK status", response.StatusCode)
		}
		return nil, errs.Error(status.Code, statusmap.LimitStatusMessage(status.Message, statusmap.MaxStatusMessageBytes))
	}
	return &closeOnEOF{ReadCloser: &limitedReadCloser{
		Reader: limits.NewReader(response.Body, maxMessageSize),
		closer: response.Body,
	}, ctx: f.ctx}, nil
}

func (f *clientFramer) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if f.closed {
		return net.ErrClosed
	}
	f.closeSend = true
	return nil
}

// Close releases the response body, if a request has been sent.
func (f *clientFramer) Close() error {
	f.closeOnce.Do(func() {
		if f.cancel != nil {
			f.cancel()
		}
		f.mu.Lock()
		f.closed = true
		response := f.response
		f.mu.Unlock()
		if response != nil && response.Body != nil {
			f.closeErr = response.Body.Close()
		}
	})
	return f.closeErr
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

type errorBody struct {
	Code    errs.Code `json:"code"`
	Message string    `json:"message"`
}

func writeError(w http.ResponseWriter, err error, maxMessageSize int64) {
	_ = maxMessageSize // status text uses the independent control-data bound below.
	code := errs.CodeOf(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusmap.HTTPStatus(code))
	message := ""
	if err != nil {
		message = statusmap.LimitStatusMessage(err.Error(), statusmap.MaxStatusMessageBytes)
	}
	_ = json.NewEncoder(w).Encode(errorBody{Code: code, Message: message})
}

type serverWriteCloser struct {
	framer *serverFramer
	mu     sync.Mutex
	bytes.Buffer
	once     sync.Once
	closed   bool
	writeErr error
	err      error
}

func (w *serverWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		w.writeErr = errors.New("http1: write after writer close")
		return 0, w.writeErr
	}
	w.framer.mu.Lock()
	defer w.framer.mu.Unlock()
	if w.framer.closed {
		w.writeErr = net.ErrClosed
		return 0, w.writeErr
	}
	if w.framer.closeSend {
		w.writeErr = errors.New("http1: send after CloseSend")
		return 0, w.writeErr
	}
	maxMessageSize := transport.ResolveMaxMessageSize(w.framer.maxMessageSize)
	if int64(w.Len()) > maxMessageSize-int64(len(p)) {
		w.writeErr = fmt.Errorf("http1: message is larger than %d bytes", maxMessageSize)
		return 0, w.writeErr
	}
	return w.Buffer.Write(p)
}

func (w *serverWriteCloser) Close() error {
	w.once.Do(func() {
		w.mu.Lock()
		w.closed = true
		payload := append([]byte(nil), w.Bytes()...)
		writeErr := w.writeErr
		w.mu.Unlock()
		if writeErr != nil {
			w.err = writeErr
			return
		}
		w.framer.mu.Lock()
		defer w.framer.mu.Unlock()
		if w.framer.closed {
			w.err = net.ErrClosed
			return
		}
		if w.framer.closeSend {
			w.err = errors.New("http1: send after CloseSend")
			return
		}
		w.framer.response.Header().Set("Content-Type", "application/json")
		w.framer.response.WriteHeader(http.StatusOK)
		w.framer.committed = true
		n, err := w.framer.response.Write(payload)
		if err == nil && n != len(payload) {
			err = io.ErrShortWrite
		}
		w.err = err
	})
	return w.err
}

type closeOnEOF struct {
	io.ReadCloser
	ctx         context.Context
	mu          sync.Mutex
	done        bool
	terminalErr error
}

type limitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *limitedReadCloser) Close() error { return r.closer.Close() }

func (r *closeOnEOF) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return 0, r.terminalErr
	}
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			r.done = true
			r.terminalErr = err
			_ = r.ReadCloser.Close()
			return 0, err
		}
	}
	n, err := r.ReadCloser.Read(p)
	if r.ctx != nil {
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
	}
	if err != nil {
		r.done = true
		r.terminalErr = err
		_ = r.ReadCloser.Close()
	}
	return n, err
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
