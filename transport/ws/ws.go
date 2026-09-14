// Package ws implements the Argos binary envelope over WebSocket (RFC6455).
package ws

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/internal/wire"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
	"github.com/coder/websocket"
)

type channel struct {
	listen *transport.ListenState
}

// New creates a WebSocket transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

func (*channel) TransportName() string { return "ws" }

func (*channel) SupportsStreaming() bool { return true }

func init() {
	transport.Register("ws", New)
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
		return errors.New("ws: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if onCall == nil {
		return errors.New("ws: nil onCall")
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
	var active sync.Map
	serveDone := make(chan struct{})
	defer close(serveDone)
	defer func() {
		active.Range(func(key, _ any) bool {
			_ = key.(*websocket.Conn).Close(websocket.StatusGoingAway, "server shutting down")
			return true
		})
	}()

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.serveUpgrade(ctx, w, r, onCall, transport.ResolveMaxMessageSize(serve.MaxMessageSize), &active)
		}),
		MaxHeaderBytes: serve.MaxHeaderBytes,
	}
	defer func() { _ = server.Close() }()
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
			active.Range(func(key, _ any) bool {
				_ = key.(*websocket.Conn).Close(websocket.StatusGoingAway, "server shutting down")
				return true
			})
		case <-serveDone:
		}
	}()

	err = server.Serve(listener)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (t *channel) serveUpgrade(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	onCall func(context.Context, string, transport.Framer) error,
	maxMessageSize int64,
	active *sync.Map,
) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(transport.ResolveMaxFrameSize(maxMessageSize))
	active.Store(conn, struct{}{})
	go func() {
		defer active.Delete(conn)
		t.serveConn(ctx, conn, onCall, maxMessageSize)
	}()
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	if ctx == nil {
		return nil, errors.New("ws: nil context")
	}
	call := transport.ApplyClientOptions(opts)
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	endpoint := "ws://" + addr + "/"
	conn, _, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		return nil, err
	}
	maxMessageSize := transport.ResolveMaxMessageSize(call.MaxMessageSize)
	conn.SetReadLimit(transport.ResolveMaxFrameSize(maxMessageSize))
	f := &framer{
		conn:           conn,
		ctx:            ctx,
		method:         method,
		initiator:      true,
		frames:         make(chan wire.Envelope, 1),
		done:           make(chan struct{}),
		maxMessageSize: maxMessageSize,
	}
	f.watchContext()
	go f.readAhead()
	return f, nil
}

func (t *channel) serveConn(
	ctx context.Context,
	conn *websocket.Conn,
	onCall func(context.Context, string, transport.Framer) error,
	maxMessageSize int64,
) {
	defer conn.Close(websocket.StatusNormalClosure, "")

	msgType, blob, err := conn.Read(ctx)
	if err != nil {
		return
	}
	if msgType == websocket.MessageText {
		callErr := errs.Error(errs.Unimplemented, "ws: text frame is not supported")
		code := errs.CodeOf(callErr)
		_ = writeEnvelope(ctx, conn, wire.Envelope{
			Flags:   wire.FlagStatus,
			Payload: wire.MarshalStatus(uint32(code), statusmap.LimitStatusMessage(callErr.Error(), maxMessageSize-4)),
		})
		return
	}
	if msgType != websocket.MessageBinary {
		return
	}
	if int64(len(blob)) > transport.ResolveMaxFrameSize(maxMessageSize) {
		return
	}

	first, err := wire.UnmarshalEnvelope(blob)
	if err != nil || first.Method == "" {
		return
	}
	if first.StreamID != 0 || (first.Flags != 0 && first.Flags != wire.FlagEnd) ||
		(first.Flags == wire.FlagEnd && len(first.Payload) != 0) {
		return
	}
	if err := wire.ValidateEnvelope(first, maxMessageSize, transport.DefaultMaxMetadataSize); err != nil {
		return
	}
	md, err := decodeMetadata(first.Metadata)
	if err != nil {
		return
	}
	callCtx := metadata.With(ctx, md)
	f := &framer{
		conn:           conn,
		ctx:            callCtx,
		pending:        &first,
		done:           make(chan struct{}),
		maxMessageSize: maxMessageSize,
	}
	f.watchContext()
	defer func() { _ = f.Close() }()
	callErr := onCall(callCtx, first.Method, f)

	code := errs.OK
	description := ""
	if callErr != nil {
		code = errs.CodeOf(callErr)
		description = callErr.Error()
	}
	_ = f.writeStatus(first.StreamID, code, description)
}

type framer struct {
	conn      *websocket.Conn
	ctx       context.Context
	stateMu   sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error

	sendMu         sync.Mutex
	method         string
	initiator      bool
	sentFirst      bool
	sentEnd        bool
	maxMessageSize int64

	recvMu      sync.Mutex
	pending     *wire.Envelope
	terminalErr error

	frames  chan wire.Envelope
	readErr error
	done    chan struct{}
}

func (f *framer) readAhead() {
	defer close(f.frames)
	for {
		env, err := f.readEnvelope()
		if err != nil {
			if ctxErr := f.contextErr(); ctxErr != nil {
				// Close interrupts the WebSocket read on cancellation. Return
				// the context cause so callers can distinguish timeout/cancel
				// from a peer protocol failure.
				f.readErr = ctxErr
			} else if errors.Is(err, io.EOF) && !f.isClosed() {
				f.readErr = errors.New("ws: response ended before status trailer")
			} else {
				f.readErr = err
			}
			return
		}
		if env.Flags == wire.FlagStatus && env.Method == "" && env.StreamID == 0 && len(env.Metadata) == 0 {
			f.readErr = statusError(env.Payload)
			return
		}
		if env.Flags == wire.FlagEnd && env.Method == "" && env.StreamID == 0 && len(env.Metadata) == 0 && len(env.Payload) == 0 {
			continue
		}
		if env.Flags != 0 || env.Method != "" || env.StreamID != 0 || len(env.Metadata) != 0 {
			f.readErr = errors.New("ws: invalid response envelope")
			return
		}
		select {
		case f.frames <- env:
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
// behavior as users of client.Client: a blocked WebSocket read is interrupted
// and the connection is released.
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

	env, err := f.nextEnvelope()
	if err != nil {
		return nil, f.finish(f.contextOr(err))
	}
	if env.Method != "" {
		return nil, f.finish(errors.New("ws: method is only allowed on the first request"))
	}
	if env.StreamID != 0 {
		return nil, f.finish(fmt.Errorf("ws: unsupported stream id %d", env.StreamID))
	}
	if env.Flags&wire.FlagStatus != 0 {
		if env.Flags != wire.FlagStatus || env.Method != "" || env.StreamID != 0 || len(env.Metadata) != 0 {
			return nil, f.finish(errors.New("ws: invalid status trailer"))
		}
		return nil, f.finish(errors.New("ws: status trailer from the call initiator"))
	}
	if env.Flags == wire.FlagEnd {
		if len(env.Metadata) != 0 || len(env.Payload) != 0 {
			return nil, f.finish(errors.New("ws: invalid end frame"))
		}
		return nil, f.finish(io.EOF)
	}
	if env.Flags != 0 {
		return nil, f.finish(errors.New("ws: invalid envelope flags"))
	}
	if len(env.Metadata) != 0 {
		return nil, f.finish(errors.New("ws: metadata is only allowed on the first request"))
	}
	return bytes.NewReader(env.Payload), nil
}

func (f *framer) nextEnvelope() (wire.Envelope, error) {
	if f.pending != nil {
		env := *f.pending
		f.pending = nil
		env.Method = ""
		env.Metadata = nil
		return env, nil
	}
	if f.frames != nil {
		env, ok := <-f.frames
		if !ok {
			return wire.Envelope{}, f.readErr
		}
		return env, nil
	}
	return f.readEnvelope()
}

func (f *framer) readEnvelope() (wire.Envelope, error) {
	msgType, blob, err := f.conn.Read(f.ctx)
	if err != nil {
		return wire.Envelope{}, err
	}
	if msgType == websocket.MessageText {
		return wire.Envelope{}, errs.Error(errs.Unimplemented, "ws: text frame is not supported")
	}
	if msgType != websocket.MessageBinary {
		return wire.Envelope{}, fmt.Errorf("ws: unsupported message type %d", msgType)
	}
	if int64(len(blob)) > transport.ResolveMaxFrameSize(f.maxMessageSize) {
		return wire.Envelope{}, errors.New("ws: frame exceeds configured limit")
	}
	env, err := wire.UnmarshalEnvelope(blob)
	if err != nil {
		return wire.Envelope{}, err
	}
	if err := wire.ValidateEnvelope(env, f.maxMessageSize, transport.DefaultMaxMetadataSize); err != nil {
		return wire.Envelope{}, err
	}
	return env, nil
}

func (f *framer) finish(err error) error {
	f.terminalErr = err
	if f.initiator {
		f.closeConn()
	}
	return err
}

func (f *framer) closeConn() {
	_ = f.Close()
}

// Close terminates the WebSocket call and releases the connection.
func (f *framer) Close() error {
	f.closeOnce.Do(func() {
		f.stateMu.Lock()
		f.closed = true
		f.stateMu.Unlock()
		if f.done != nil {
			close(f.done)
		}
		f.closeErr = f.conn.Close(websocket.StatusNormalClosure, "")
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

func statusError(payload []byte) error {
	code, description, err := wire.UnmarshalStatus(payload)
	if err != nil {
		return err
	}
	if errs.Code(code) == errs.OK {
		return io.EOF
	}
	return errs.Error(errs.Code(code), statusmap.LimitStatusMessage(description, statusmap.MaxStatusMessageBytes))
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
	if f.sentEnd {
		return nil, errors.New("ws: send after CloseSend")
	}
	return &messageWriter{close: func(payload []byte) error {
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		if f.sentEnd {
			return errors.New("ws: send after CloseSend")
		}
		return f.contextOr(f.writeEnvelope(wire.Envelope{Payload: payload}))
	}}, nil
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
	if f.sentEnd {
		return nil
	}
	if err := f.writeEnvelope(wire.Envelope{Flags: wire.FlagEnd}); err != nil {
		return f.contextOr(err)
	}
	f.sentEnd = true
	return nil
}

func (f *framer) writeStatus(streamID uint32, code errs.Code, description string) error {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	return f.contextOr(f.writeEnvelope(wire.Envelope{
		StreamID: streamID,
		Flags:    wire.FlagStatus,
		Payload: wire.MarshalStatus(
			uint32(code),
			statusmap.LimitStatusMessage(description, f.maxMessageSize-4),
		),
	}))
}

func (f *framer) writeEnvelope(env wire.Envelope) error {
	if f.isClosed() {
		return net.ErrClosed
	}
	if !f.sentFirst {
		if f.initiator {
			env.Method = f.method
			md, err := encodeMetadata(metadata.FromContext(f.ctx))
			if err != nil {
				return err
			}
			env.Metadata = md
		}
	}
	if err := wire.ValidateEnvelope(env, f.maxMessageSize, transport.DefaultMaxMetadataSize); err != nil {
		return err
	}
	if err := writeEnvelope(f.ctx, f.conn, env); err != nil {
		return err
	}
	f.sentFirst = true
	return nil
}

func writeEnvelope(ctx context.Context, conn *websocket.Conn, env wire.Envelope) error {
	blob, err := wire.MarshalEnvelope(env)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageBinary, blob)
}

type messageWriter struct {
	bytes.Buffer
	once  sync.Once
	close func([]byte) error
	err   error
}

func (w *messageWriter) Close() error {
	w.once.Do(func() {
		w.err = w.close(w.Bytes())
	})
	return w.err
}

func encodeMetadata(md metadata.Metadata) ([]byte, error) {
	keys := make([]string, 0, len(md))
	for key := range md {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, key := range keys {
		for _, value := range md[key] {
			if len(key) > int(^uint16(0)) || len(value) > int(^uint16(0)) {
				return nil, errors.New("ws: metadata key or value exceeds 65535 bytes")
			}
			entrySize := int64(4) + int64(len(key)) + int64(len(value))
			if entrySize > transport.DefaultMaxMetadataSize || int64(buf.Len()) > transport.DefaultMaxMetadataSize-entrySize {
				return nil, errors.New("ws: metadata exceeds configured limit")
			}
			var length [2]byte
			binary.BigEndian.PutUint16(length[:], uint16(len(key)))
			buf.Write(length[:])
			buf.WriteString(key)
			binary.BigEndian.PutUint16(length[:], uint16(len(value)))
			buf.Write(length[:])
			buf.WriteString(value)
		}
	}
	return buf.Bytes(), nil
}

func decodeMetadata(data []byte) (metadata.Metadata, error) {
	if int64(len(data)) > transport.DefaultMaxMetadataSize {
		return nil, errors.New("ws: metadata exceeds configured limit")
	}
	md := make(metadata.Metadata)
	for len(data) != 0 {
		if len(data) < 2 {
			return nil, errors.New("ws: truncated metadata key length")
		}
		keyLen := int(binary.BigEndian.Uint16(data))
		data = data[2:]
		if len(data) < keyLen+2 {
			return nil, errors.New("ws: truncated metadata key")
		}
		key := string(data[:keyLen])
		data = data[keyLen:]
		valueLen := int(binary.BigEndian.Uint16(data))
		data = data[2:]
		if len(data) < valueLen {
			return nil, errors.New("ws: truncated metadata value")
		}
		md[key] = append(md[key], string(data[:valueLen]))
		data = data[valueLen:]
	}
	return md, nil
}
