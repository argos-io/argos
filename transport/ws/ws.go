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
	"time"

	"github.com/argos-io/argos/errs"
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

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.serveUpgrade(ctx, w, r, onCall)
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

func (t *channel) serveUpgrade(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	onCall func(context.Context, string, transport.Framer) error,
) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	go t.serveConn(ctx, conn, onCall)
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
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
	f := &framer{
		conn:      conn,
		ctx:       ctx,
		method:    method,
		initiator: true,
		frames:    make(chan wire.Envelope, 1),
	}
	go f.readAhead()
	return f, nil
}

func (t *channel) serveConn(
	ctx context.Context,
	conn *websocket.Conn,
	onCall func(context.Context, string, transport.Framer) error,
) {
	defer conn.Close(websocket.StatusNormalClosure, "")

	msgType, blob, err := conn.Read(ctx)
	if err != nil {
		return
	}
	if msgType == websocket.MessageText {
		callErr := onCall(ctx, "", &textFramer{})
		if callErr == nil {
			callErr = errs.Error(errs.Unimplemented, "ws: text frame is not supported")
		}
		code := errs.CodeOf(callErr)
		_ = writeEnvelope(ctx, conn, wire.Envelope{
			Flags:   wire.FlagStatus,
			Payload: wire.MarshalStatus(uint32(code), callErr.Error()),
		})
		return
	}
	if msgType != websocket.MessageBinary {
		return
	}

	first, err := wire.UnmarshalEnvelope(blob)
	if err != nil || first.Method == "" {
		return
	}
	md, err := decodeMetadata(first.Metadata)
	if err != nil {
		return
	}
	callCtx := metadata.With(ctx, md)
	f := &framer{
		conn:    conn,
		ctx:     callCtx,
		pending: &first,
	}
	callErr := onCall(callCtx, first.Method, f)
	if callErr == nil {
		f.drainPeerEndFrames()
	}

	code := errs.OK
	description := ""
	if callErr != nil {
		code = errs.CodeOf(callErr)
		description = callErr.Error()
	}
	_ = f.writeStatus(first.StreamID, code, description)
}

type textFramer struct{}

func (*textFramer) Recv() (io.Reader, error) {
	return nil, errs.Error(errs.Unimplemented, "ws: text frame is not supported")
}

func (*textFramer) Send() (io.WriteCloser, error) {
	return nil, errs.Error(errs.Unimplemented, "ws: text frame is not supported")
}

func (*textFramer) CloseSend() error { return nil }

type framer struct {
	conn      *websocket.Conn
	ctx       context.Context
	closeOnce sync.Once

	sendMu    sync.Mutex
	method    string
	initiator bool
	sentFirst bool
	sentEnd   bool

	recvMu      sync.Mutex
	pending     *wire.Envelope
	terminalErr error

	frames  chan wire.Envelope
	readErr error
}

func (f *framer) drainPeerEndFrames() {
	ctx, cancel := context.WithTimeout(f.ctx, 100*time.Millisecond)
	defer cancel()
	for {
		msgType, blob, err := f.conn.Read(ctx)
		if err != nil {
			return
		}
		if msgType != websocket.MessageBinary {
			continue
		}
		env, err := wire.UnmarshalEnvelope(blob)
		if err != nil {
			return
		}
		if env.Flags&wire.FlagEnd != 0 && len(env.Payload) == 0 {
			return
		}
	}
}

func (f *framer) readAhead() {
	defer close(f.frames)
	for {
		env, err := f.readEnvelope()
		if err != nil {
			f.readErr = err
			return
		}
		if env.Flags&wire.FlagStatus != 0 {
			f.readErr = statusError(env.Payload)
			return
		}
		if env.Flags&wire.FlagEnd != 0 && len(env.Payload) == 0 {
			continue
		}
		select {
		case f.frames <- env:
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

	env, err := f.nextEnvelope()
	if err != nil {
		return nil, f.finish(err)
	}
	if env.Method != "" {
		return nil, f.finish(errors.New("ws: method is only allowed on the first request"))
	}
	if env.StreamID != 0 {
		return nil, f.finish(fmt.Errorf("ws: unsupported stream id %d", env.StreamID))
	}
	if env.Flags&wire.FlagStatus != 0 {
		return nil, f.finish(errors.New("ws: status trailer from the call initiator"))
	}
	if len(env.Metadata) != 0 {
		md, err := decodeMetadata(env.Metadata)
		if err != nil {
			return nil, f.finish(err)
		}
		metadata.With(f.ctx, md)
	}
	if env.Flags&wire.FlagEnd != 0 && len(env.Payload) == 0 {
		return nil, f.finish(io.EOF)
	}
	return bytes.NewReader(env.Payload), nil
}

func (f *framer) nextEnvelope() (wire.Envelope, error) {
	if f.pending != nil {
		env := *f.pending
		f.pending = nil
		env.Method = ""
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
	return wire.UnmarshalEnvelope(blob)
}

func (f *framer) finish(err error) error {
	f.terminalErr = err
	if f.initiator {
		f.closeConn()
	}
	return err
}

func (f *framer) closeConn() {
	f.closeOnce.Do(func() { _ = f.conn.Close(websocket.StatusNormalClosure, "") })
}

func statusError(payload []byte) error {
	code, description, err := wire.UnmarshalStatus(payload)
	if err != nil {
		return err
	}
	if errs.Code(code) == errs.OK {
		return io.EOF
	}
	return errs.Error(errs.Code(code), description)
}

func (f *framer) Send() (io.WriteCloser, error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if f.sentEnd {
		return nil, errors.New("ws: send after CloseSend")
	}
	return &messageWriter{close: func(payload []byte) error {
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		if f.sentEnd {
			return errors.New("ws: send after CloseSend")
		}
		return f.writeEnvelope(wire.Envelope{Payload: payload}, true)
	}}, nil
}

func (f *framer) CloseSend() error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if f.sentEnd {
		return nil
	}
	if err := f.writeEnvelope(wire.Envelope{Flags: wire.FlagEnd}, false); err != nil {
		return err
	}
	f.sentEnd = true
	return nil
}

func (f *framer) writeStatus(streamID uint32, code errs.Code, description string) error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	return f.writeEnvelope(wire.Envelope{
		StreamID: streamID,
		Flags:    wire.FlagStatus,
		Payload:  wire.MarshalStatus(uint32(code), description),
	}, false)
}

func (f *framer) writeEnvelope(env wire.Envelope, message bool) error {
	if !f.sentFirst {
		if f.initiator {
			env.Method = f.method
		}
		if message {
			md, err := encodeMetadata(metadata.FromContext(f.ctx))
			if err != nil {
				return err
			}
			env.Metadata = md
		}
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
