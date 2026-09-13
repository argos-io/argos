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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

const (
	errPrefix    = "ERR "
	drainTimeout = 5 * time.Second
)

type channel struct {
	listen *transport.ListenState
}

// New creates a telnet debug transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

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

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go t.serveConn(ctx, conn, onCall)
	}
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	call := transport.ApplyClientOptions(opts)
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "%s\n", method); err != nil {
		_ = conn.Close()
		return nil, err
	}
	f := &framer{
		conn:      conn,
		ctx:       ctx,
		initiator: true,
		reader:    bufio.NewReader(conn),
		frames:    make(chan []byte, 1),
	}
	go f.readAhead()
	return f, nil
}

func (t *channel) serveConn(
	ctx context.Context,
	conn net.Conn,
	onCall func(context.Context, string, transport.Framer) error,
) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	method, err := readLine(reader)
	if err != nil || method == "" {
		return
	}

	callCtx := metadata.With(ctx, metadata.Metadata{})
	f := &framer{
		conn:   conn,
		ctx:    callCtx,
		reader: reader,
	}
	callErr := onCall(callCtx, method, f)

	code := errs.OK
	msg := ""
	if callErr != nil {
		code = errs.CodeOf(callErr)
		msg = callErr.Error()
	}
	if err := writeErrLine(conn, code, msg); err != nil {
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
	conn      net.Conn
	ctx       context.Context
	initiator bool
	reader    *bufio.Reader

	sendMu       sync.Mutex
	requestSent  bool
	responseSent bool
	sendClosed   bool

	recvMu      sync.Mutex
	received    bool
	terminalErr error

	frames  chan []byte
	readErr error
}

func (f *framer) readAhead() {
	defer close(f.frames)
	for {
		line, err := readLine(f.reader)
		if err != nil {
			f.readErr = err
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
		}
	}
}

func (f *framer) Recv() (io.Reader, error) {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()

	if f.terminalErr != nil {
		return nil, f.terminalErr
	}
	if f.received {
		return nil, f.finish(errors.New("telnet: unary stream received more than one message"))
	}
	f.received = true

	if f.initiator {
		payload, ok := <-f.frames
		if !ok {
			return nil, f.finish(f.readErr)
		}
		return bytes.NewReader(payload), nil
	}

	for {
		line, err := readLine(f.reader)
		if err != nil {
			return nil, f.finish(err)
		}
		if strings.HasPrefix(line, "m ") {
			parts := strings.SplitN(strings.TrimPrefix(line, "m "), " ", 2)
			if len(parts) == 2 {
				key, value := parts[0], parts[1]
				md := metadata.FromContext(f.ctx)
				if md == nil {
					f.ctx = metadata.With(f.ctx, metadata.Metadata{})
					md = metadata.FromContext(f.ctx)
				}
				md[key] = append(md[key], value)
			}
			continue
		}
		return bytes.NewReader([]byte(line)), nil
	}
}

func (f *framer) finish(err error) error {
	f.terminalErr = err
	return err
}

func (f *framer) Send() (io.WriteCloser, error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
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
	if f.requestSent {
		return errors.New("telnet: unary stream sent more than one request")
	}
	for key, values := range metadata.FromContext(f.ctx) {
		for _, value := range values {
			if _, err := fmt.Fprintf(f.conn, "m %s %s\n", key, value); err != nil {
				return err
			}
		}
	}
	if _, err := f.conn.Write(append(payload, '\n')); err != nil {
		return err
	}
	f.requestSent = true
	return nil
}

func (f *framer) sendResponse(payload []byte) error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if f.responseSent {
		return errors.New("telnet: unary stream sent more than one response")
	}
	if _, err := f.conn.Write(append(payload, '\n')); err != nil {
		return err
	}
	f.responseSent = true
	return nil
}

func (f *framer) CloseSend() error {
	return nil
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

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\n"), nil
}

func writeErrLine(w io.Writer, code errs.Code, msg string) error {
	if msg == "" {
		_, err := fmt.Fprintf(w, "%s%d\n", errPrefix, code)
		return err
	}
	_, err := fmt.Fprintf(w, "%s%d %s\n", errPrefix, code, msg)
	return err
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
	return errs.Error(errs.Code(code), msg)
}
