// Package telnet implements a newline-framed text debug transport over TCP.
package telnet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

const (
	errPrefix       = "ERR "
	drainTimeout    = 5 * time.Second
	statusLineLimit = 64 << 10
)

type channel struct {
	listen *transport.ListenState
}

// New creates a telnet debug transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

func (*channel) TransportName() string { return "telnet" }

func (*channel) SupportsStreaming() bool { return false }

func init() {
	transport.Register("telnet", New)
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
		return errors.New("telnet: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if onCall == nil {
		return errors.New("telnet: nil onCall")
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
	defer listener.Close()
	maxMessageSize := transport.ResolveMaxMessageSize(serve.MaxMessageSize)
	var active sync.Map
	serveDone := make(chan struct{})
	defer close(serveDone)
	defer func() {
		active.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close()
			return true
		})
	}()

	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
			active.Range(func(key, _ any) bool {
				_ = key.(net.Conn).Close()
				return true
			})
		case <-serveDone:
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		active.Store(conn, struct{}{})
		go func() {
			defer active.Delete(conn)
			t.serveConn(ctx, conn, onCall, maxMessageSize)
		}()
	}
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	if ctx == nil {
		return nil, errors.New("telnet: nil context")
	}
	call := transport.ApplyClientOptions(opts)
	if err := validateMethod(method); err != nil {
		return nil, err
	}
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	f := &framer{
		conn:           conn,
		ctx:            ctx,
		initiator:      true,
		reader:         bufio.NewReader(conn),
		frames:         make(chan []byte, 1),
		done:           make(chan struct{}),
		maxMessageSize: transport.ResolveMaxMessageSize(call.MaxMessageSize),
	}
	f.watchContext()
	if err := writeAll(conn, []byte(method+"\n")); err != nil {
		_ = f.Close()
		return nil, f.contextOr(err)
	}
	go f.readAhead()
	return f, nil
}

func (t *channel) serveConn(
	ctx context.Context,
	conn net.Conn,
	onCall func(context.Context, string, transport.Framer) error,
	maxMessageSize int64,
) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	method, err := readLine(reader, maxMessageSize)
	if err != nil || method == "" {
		return
	}
	if err := validateMethod(method); err != nil {
		return
	}

	md, payload, recvErr := readRequest(reader, maxMessageSize)
	callCtx := metadata.With(ctx, md)
	f := &framer{
		conn:           conn,
		ctx:            callCtx,
		reader:         reader,
		pending:        payload,
		pendingReady:   true,
		pendingErr:     recvErr,
		done:           make(chan struct{}),
		maxMessageSize: maxMessageSize,
	}
	f.watchContext()
	defer func() { _ = f.Close() }()
	callErr := onCall(callCtx, method, f)

	code := errs.OK
	msg := ""
	if callErr != nil {
		code = errs.CodeOf(callErr)
		msg = callErr.Error()
	}
	if err := writeErrLine(conn, code, msg, maxMessageSize); err != nil {
		return
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.CloseWrite(); err == nil {
			_ = tcpConn.SetReadDeadline(time.Now().Add(drainTimeout))
			_, _ = io.Copy(io.Discard, tcpConn)
		}
	}
}

type framer struct {
	conn           net.Conn
	ctx            context.Context
	initiator      bool
	reader         *bufio.Reader
	maxMessageSize int64
	stateMu        sync.RWMutex
	closed         bool
	closeOnce      sync.Once
	closeErr       error

	sendMu       sync.Mutex
	requestSent  bool
	responseSent bool
	sendClosed   bool

	recvMu       sync.Mutex
	received     bool
	terminalErr  error
	pending      []byte
	pendingReady bool
	pendingErr   error

	frames  chan []byte
	readErr error
	done    chan struct{}
}

func (f *framer) readAhead() {
	defer close(f.frames)
	for {
		line, err := readLine(f.reader, lineLimit(f.maxMessageSize))
		if err != nil {
			if ctxErr := f.contextErr(); ctxErr != nil {
				// The context watcher closes the socket to interrupt ReadSlice;
				// retain the context cause for the caller.
				f.readErr = ctxErr
			} else if errors.Is(err, io.EOF) && !f.isClosed() {
				f.readErr = errors.New("telnet: response ended before status line")
			} else {
				f.readErr = err
			}
			return
		}
		if strings.HasPrefix(line, errPrefix) {
			f.readErr = parseErrLine(line)
			return
		}
		select {
		case f.frames <- []byte(line):
		case <-f.ctx.Done():
			f.readErr = f.ctx.Err()
			return
		case <-f.done:
			f.readErr = net.ErrClosed
			return
		}
	}
}

// watchContext makes direct Transport.Open users get the same cancellation
// behavior as users of client.Client: a blocked socket read is interrupted.
func (f *framer) watchContext() {
	if f.ctx == nil || f.ctx.Done() == nil || f.done == nil {
		return
	}
	go func() {
		select {
		case <-f.ctx.Done():
			_ = f.Close()
		case <-f.done:
		}
	}()
}

func (f *framer) Recv() (io.Reader, error) {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	if f.terminalErr != nil {
		return nil, f.terminalErr
	}
	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	if f.isClosed() {
		return nil, net.ErrClosed
	}

	if f.initiator {
		if f.received {
			_, ok := <-f.frames
			if !ok {
				return nil, f.finish(f.readErr)
			}
			return nil, f.finish(errors.New("telnet: unary stream received more than one message"))
		}
		f.received = true
		payload, ok := <-f.frames
		if !ok {
			return nil, f.finish(f.contextOr(f.readErr))
		}
		return bytes.NewReader(payload), nil
	}

	if f.received {
		return nil, f.finish(errors.New("telnet: unary stream received more than one message"))
	}
	f.received = true
	if f.pendingErr != nil {
		return nil, f.finish(f.pendingErr)
	}
	if f.pendingReady {
		payload := f.pending
		f.pending = nil
		f.pendingReady = false
		return bytes.NewReader(payload), nil
	}

	return nil, f.finish(errors.New("telnet: request was not primed"))
}

func (f *framer) finish(err error) error {
	f.terminalErr = err
	if f.initiator {
		_ = f.Close()
	}
	return err
}

func (f *framer) Send() (io.WriteCloser, error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	if f.isClosed() {
		return nil, net.ErrClosed
	}
	if f.sendClosed {
		return nil, errors.New("telnet: send after CloseSend")
	}
	if f.initiator {
		if f.requestSent {
			return nil, errors.New("telnet: unary stream sent more than one request")
		}
		return &messageWriter{closeFn: f.sendRequest}, nil
	}
	if f.responseSent {
		return nil, errors.New("telnet: unary stream sent more than one response")
	}
	return &messageWriter{closeFn: f.sendResponse}, nil
}

func (f *framer) sendRequest(payload []byte) error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if f.isClosed() {
		return net.ErrClosed
	}
	if f.sendClosed {
		return errors.New("telnet: send after CloseSend")
	}
	if int64(len(payload)) > f.maxMessageSize {
		return fmt.Errorf("telnet: message is %d bytes; maximum is %d", len(payload), f.maxMessageSize)
	}
	if f.requestSent {
		return errors.New("telnet: unary stream sent more than one request")
	}
	md := metadata.FromContext(f.ctx)
	keys := make([]string, 0, len(md))
	for key := range md {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var metadataSize int64
	for _, key := range keys {
		values := md[key]
		for _, value := range values {
			entrySize, err := metadataEntrySize(key, value)
			if err != nil {
				return err
			}
			if metadataSize > transport.DefaultMaxMetadataSize-entrySize {
				return fmt.Errorf("telnet: metadata exceeds configured limit")
			}
			metadataSize += entrySize
			if err := writeAll(f.conn, []byte("m "+key+" "+value+"\n")); err != nil {
				return f.contextOr(err)
			}
		}
	}
	if err := writeAll(f.conn, payload); err != nil {
		return f.contextOr(err)
	}
	if err := writeAll(f.conn, []byte{'\n'}); err != nil {
		return f.contextOr(err)
	}
	f.requestSent = true
	return nil
}

func (f *framer) sendResponse(payload []byte) error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if f.isClosed() {
		return net.ErrClosed
	}
	if f.sendClosed {
		return errors.New("telnet: send after CloseSend")
	}
	if int64(len(payload)) > f.maxMessageSize {
		return fmt.Errorf("telnet: message is %d bytes; maximum is %d", len(payload), f.maxMessageSize)
	}
	if f.responseSent {
		return errors.New("telnet: unary stream sent more than one response")
	}
	if err := writeAll(f.conn, payload); err != nil {
		return f.contextOr(err)
	}
	if err := writeAll(f.conn, []byte{'\n'}); err != nil {
		return f.contextOr(err)
	}
	f.responseSent = true
	return nil
}

func (f *framer) CloseSend() error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if f.isClosed() {
		return net.ErrClosed
	}
	f.sendClosed = true
	return nil
}

// Close terminates the Telnet call and releases the connection. Closing the
// connection also unblocks readAhead when a stream consumer abandons it.
func (f *framer) Close() error {
	f.closeOnce.Do(func() {
		f.stateMu.Lock()
		f.closed = true
		f.stateMu.Unlock()
		if f.done != nil {
			close(f.done)
		}
		f.closeErr = f.conn.Close()
	})
	return f.closeErr
}

func (f *framer) isClosed() bool {
	f.stateMu.RLock()
	defer f.stateMu.RUnlock()
	return f.closed
}

func (f *framer) contextErr() error {
	if f.ctx == nil {
		return nil
	}
	return f.ctx.Err()
}

func (f *framer) contextOr(err error) error {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	return err
}

type messageWriter struct {
	bytes.Buffer
	once    sync.Once
	closeFn func([]byte) error
	err     error
}

func (w *messageWriter) Close() error {
	w.once.Do(func() {
		w.err = w.closeFn(w.Bytes())
	})
	return w.err
}

func readLine(r *bufio.Reader, limits ...int64) (string, error) {
	var line []byte
	var max int64
	if len(limits) > 0 {
		max = limits[0]
	}
	for {
		part, err := r.ReadSlice('\n')
		line = append(line, part...)
		if err == bufio.ErrBufferFull {
			// There is no delimiter in this chunk, so every byte is payload.
			if max > 0 && int64(len(line)) > max {
				return "", errors.New("telnet: line exceeds configured limit")
			}
			continue
		}
		if err != nil {
			if max > 0 && int64(len(line)) > max {
				return "", errors.New("telnet: line exceeds configured limit")
			}
			return "", err
		}
		contentLen := len(line)
		if contentLen > 0 && line[contentLen-1] == '\n' {
			contentLen--
		}
		if max > 0 && int64(contentLen) > max {
			return "", errors.New("telnet: line exceeds configured limit")
		}
		return strings.TrimSuffix(string(line), "\n"), nil
	}
}

// readRequest consumes the Telnet request metadata and first payload before
// invoking the server callback. This makes request metadata available to
// filters through the callback context, matching the other transports.
func readRequest(r *bufio.Reader, maxMessageSize int64) (metadata.Metadata, []byte, error) {
	md := make(metadata.Metadata)
	var metadataSize int64
	for {
		line, err := readLine(r, maxMessageSize)
		if err != nil {
			return md, nil, err
		}
		if !strings.HasPrefix(line, "m ") {
			return md, []byte(line), nil
		}
		key, value, ok := strings.Cut(strings.TrimPrefix(line, "m "), " ")
		if !ok {
			return md, nil, errors.New("telnet: malformed metadata line")
		}
		entrySize, err := metadataEntrySize(key, value)
		if err != nil {
			return md, nil, err
		}
		if metadataSize > transport.DefaultMaxMetadataSize-entrySize {
			return md, nil, fmt.Errorf("telnet: metadata exceeds configured limit")
		}
		metadataSize += entrySize
		md[key] = append(md[key], value)
	}
}

func metadataEntrySize(key, value string) (int64, error) {
	if key == "" || strings.ContainsAny(key, " \t\r\n") {
		return 0, errors.New("telnet: metadata key is not a token")
	}
	if !utf8.ValidString(key) || !utf8.ValidString(value) || strings.ContainsAny(key+value, "\r\n") {
		return 0, errors.New("telnet: metadata contains invalid line characters")
	}
	if len(key) > int(transport.DefaultMaxMetadataSize) || len(value) > int(transport.DefaultMaxMetadataSize) {
		return 0, errors.New("telnet: metadata entry exceeds configured limit")
	}
	return int64(len(key)) + int64(len(value)) + 4, nil
}

func validateMethod(method string) error {
	if method == "" || !utf8.ValidString(method) || strings.ContainsAny(method, "\r\n") {
		return errors.New("telnet: invalid method")
	}
	return nil
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

func writeErrLine(w io.Writer, code errs.Code, msg string, maxMessageSize int64) error {
	prefix := fmt.Sprintf("%s%d", errPrefix, code)
	_ = maxMessageSize // status lines use the independent control-data bound below.
	maxLineMessage := statusLineLimit - int64(len(prefix)) - 1
	msg = escapeLineMessage(statusmap.LimitStatusMessage(msg, statusmap.MaxStatusMessageBytes))
	// Escaping backslashes and line breaks can expand the text. Apply the
	// line limit once more to guarantee that a status remains one bounded line.
	msg = statusmap.LimitStatusMessage(msg, maxLineMessage)
	var line string
	if msg == "" {
		line = prefix + "\n"
	} else {
		line = prefix + " " + msg + "\n"
	}
	return writeAll(w, []byte(line))
}

func lineLimit(messageSize int64) int64 {
	if messageSize < statusLineLimit {
		return statusLineLimit
	}
	return messageSize
}

// escapeLineMessage keeps the status message on one Telnet line while
// preserving literal backslashes and line breaks for a peer that understands
// the escape convention.
func escapeLineMessage(msg string) string {
	msg = strings.ReplaceAll(msg, `\`, `\\`)
	msg = strings.ReplaceAll(msg, "\r", `\r`)
	return strings.ReplaceAll(msg, "\n", `\n`)
}

func unescapeLineMessage(msg string) string {
	var b strings.Builder
	b.Grow(len(msg))
	for i := 0; i < len(msg); i++ {
		if msg[i] != '\\' || i+1 == len(msg) {
			b.WriteByte(msg[i])
			continue
		}
		i++
		switch msg[i] {
		case 'r':
			b.WriteByte('\r')
		case 'n':
			b.WriteByte('\n')
		case '\\':
			b.WriteByte('\\')
		default:
			b.WriteByte('\\')
			b.WriteByte(msg[i])
		}
	}
	return b.String()
}

func parseErrLine(line string) error {
	rest := strings.TrimPrefix(line, errPrefix)
	codeText, msg, ok := strings.Cut(rest, " ")
	if !ok {
		code, err := strconv.Atoi(rest)
		if err != nil {
			return fmt.Errorf("telnet: invalid status line %q", line)
		}
		if errs.Code(code) == errs.OK {
			return io.EOF
		}
		return errs.Error(errs.Code(code), "")
	}
	code, err := strconv.Atoi(codeText)
	if err != nil {
		return fmt.Errorf("telnet: invalid status line %q", line)
	}
	if errs.Code(code) == errs.OK {
		return io.EOF
	}
	return errs.Error(errs.Code(code), statusmap.LimitStatusMessage(unescapeLineMessage(msg), statusmap.MaxStatusMessageBytes))
}
