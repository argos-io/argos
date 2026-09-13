// Package http1 implements unary Argos calls over HTTP/1.1.
package http1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

type channel struct {
	listen *transport.ListenState
}

// New creates an HTTP/1.1 transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

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
			serveCall(w, r, onCall)
		}),
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
	endpoint := url.URL{Scheme: "http", Host: addr, Path: "/" + method}
	return &clientFramer{
		ctx:      ctx,
		endpoint: endpoint.String(),
		metadata: metadata.FromContext(ctx),
		client:   http.DefaultClient,
	}, nil
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
	method := strings.TrimPrefix(r.URL.Path, "/")
	if method == "" || strings.Count(method, "/") != 1 {
		writeError(w, errs.Error(errs.NotFound, "invalid method path"))
		return
	}

	ctx := metadata.With(r.Context(), metadataFromHeaders(r.Header))
	f := &serverFramer{request: r.Body, response: w}
	err := onCall(ctx, method, f)
	if err != nil && !f.hasSent() {
		writeError(w, err)
		return
	}
	if !f.hasSent() {
		w.WriteHeader(http.StatusOK)
	}
}

func metadataFromHeaders(headers http.Header) metadata.Metadata {
	md := make(metadata.Metadata, len(headers))
	for key, values := range headers {
		md[strings.ToLower(key)] = append([]string(nil), values...)
	}
	return md
}

type serverFramer struct {
	mu       sync.Mutex
	request  io.Reader
	response http.ResponseWriter
	received bool
	sent     bool
}

func (f *serverFramer) Recv() (io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.received {
		return nil, errors.New("http1: unary stream received more than one request")
	}
	f.received = true
	return f.request, nil
}

func (f *serverFramer) Send() (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sent {
		return nil, errors.New("http1: unary stream sent more than one response")
	}
	f.sent = true
	f.response.Header().Set("Content-Type", "application/json")
	f.response.WriteHeader(http.StatusOK)
	return nopWriteCloser{Writer: f.response}, nil
}

func (f *serverFramer) CloseSend() error { return nil }

func (f *serverFramer) hasSent() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent
}

type clientFramer struct {
	mu       sync.Mutex
	ctx      context.Context
	endpoint string
	metadata metadata.Metadata
	client   *http.Client
	response *http.Response
	sendUsed bool
	recvUsed bool
}

func (f *clientFramer) Send() (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendUsed {
		return nil, errors.New("http1: unary stream sent more than one request")
	}
	f.sendUsed = true
	return &messageWriter{send: f.sendRequest}, nil
}

func (f *clientFramer) sendRequest(body []byte) error {
	request, err := http.NewRequestWithContext(
		f.ctx,
		http.MethodPost,
		f.endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	for key, values := range f.metadata {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	response, err := f.client.Do(request)
	if err != nil {
		return err
	}

	f.mu.Lock()
	f.response = response
	f.mu.Unlock()
	return nil
}

func (f *clientFramer) Recv() (io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recvUsed {
		return nil, errors.New("http1: unary stream received more than one response")
	}
	f.recvUsed = true
	if f.response == nil {
		return nil, errors.New("http1: receive before request")
	}
	if f.response.StatusCode != http.StatusOK {
		defer f.response.Body.Close()
		var status errorBody
		if err := json.NewDecoder(f.response.Body).Decode(&status); err != nil {
			return nil, fmt.Errorf("http1: HTTP %d: invalid error body: %w", f.response.StatusCode, err)
		}
		return nil, errs.Error(status.Code, status.Message)
	}
	return closeOnEOF{ReadCloser: f.response.Body}, nil
}

func (f *clientFramer) CloseSend() error { return nil }

type errorBody struct {
	Code    errs.Code `json:"code"`
	Message string    `json:"message"`
}

func writeError(w http.ResponseWriter, err error) {
	code := errs.CodeOf(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusmap.HTTPStatus(code))
	_ = json.NewEncoder(w).Encode(errorBody{Code: code, Message: err.Error()})
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

type closeOnEOF struct {
	io.ReadCloser
}

func (r closeOnEOF) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil {
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
