// Package tcp implements the Argos binary envelope over TCP.
package tcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/internal/wire"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

// drainTimeout bounds how long a served connection waits for the initiator to
// hang up after the status trailer.
const drainTimeout = 5 * time.Second

type channel struct {
	listen *transport.ListenState
}

// New creates a TCP transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

func (*channel) TransportName() string { return "tcp" }

func (*channel) SupportsStreaming() bool { return true }

func init() {
	transport.Register("tcp", New)
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
		return errors.New("tcp: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if onCall == nil {
		return errors.New("tcp: nil onCall")
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
		return nil, errors.New("tcp: nil context")
	}
	call := transport.ApplyClientOptions(opts)
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
		method:         method,
		initiator:      true,
		frames:         make(chan wire.Envelope, 1),
		done:           make(chan struct{}),
		maxMessageSize: transport.ResolveMaxMessageSize(call.MaxMessageSize),
		maxFrameSize:   transport.ResolveMaxFrameSize(call.MaxMessageSize),
	}
	f.watchContext()
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

	blob, err := readFrame(conn, transport.ResolveMaxFrameSize(maxMessageSize))
	if err != nil {
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
		maxFrameSize:   transport.ResolveMaxFrameSize(maxMessageSize),
	}
	f.watchContext()
	defer func() { _ = f.Close() }()
	callErr := onCall(callCtx, first.Method, f)
	if callErr == nil && !f.peerEndReceived() {
		f.drainPeerEndFrames()
	}

	// The trailer is unconditional: a filter that short-circuits never closes the
	// send side, so the trailer is the only frame that carries the outcome.
	code := errs.OK
	description := ""
	if callErr != nil {
		code = errs.CodeOf(callErr)
		description = callErr.Error()
	}
	_ = f.writeStatus(first.StreamID, code, description)

	// The initiator may still be writing its own end-of-direction frame. Closing
	// now would answer it with a reset and cost it the status it is about to read,
	// so wait for its FIN instead. The deadline bounds an initiator that never
	// hangs up; a well-behaved one hangs up as soon as it has the status.
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
		_ = tcpConn.SetReadDeadline(time.Now().Add(drainTimeout))
		_, _ = io.Copy(io.Discard, tcpConn)
	}
}

type framer struct {
	conn           net.Conn
	ctx            context.Context
	stateMu        sync.RWMutex
	closed         bool
	closeOnce      sync.Once
	closeErr       error
	maxMessageSize int64
	maxFrameSize   int64

	sendMu    sync.Mutex
	method    string
	initiator bool
	sentFirst bool
	sentEnd   bool

	recvMu      sync.Mutex
	pending     *wire.Envelope
	recvEnded   bool
	terminalErr error

	// frames carries read-ahead messages to the initiator, whose Recv calls stop
	// at the last message it cares about rather than at the status trailer.
	// Reading ahead is what lets the connection be released when the call ends.
	// readErr is written before frames is closed, so a receiver that sees the
	// closed channel also sees the terminal error.
	frames  chan wire.Envelope
	readErr error
	done    chan struct{}
}

// readAhead drains the responder's frames until the status trailer, which is the
// last thing any call carries. Abandoning a stream halfway leaves this blocked on
// the handoff until the call context is cancelled.
// drainPeerEndFrames reads the initiator's end-of-direction frame when it is
// already on the wire. A short deadline avoids blocking forever when the client
// has not half-closed yet (the post-status drain still waits for FIN).
func (f *framer) drainPeerEndFrames() {
	_ = f.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer func() { _ = f.conn.SetReadDeadline(time.Time{}) }()
	for {
		env, err := f.readEnvelope()
		if err != nil {
			return
		}
		if env.Flags == wire.FlagEnd && env.Method == "" && env.StreamID == 0 &&
			len(env.Metadata) == 0 && len(env.Payload) == 0 {
			return
		}
	}
}

func (f *framer) readAhead() {
	defer close(f.frames)
	for {
		env, err := f.readEnvelope()
		if err != nil {
			if ctxErr := f.contextErr(); ctxErr != nil {
				// Closing the socket is how cancellation interrupts a blocked
				// read. Preserve the context cause instead of leaking the
				// platform-specific "use of closed network connection" error.
				f.readErr = ctxErr
			} else if errors.Is(err, io.EOF) && !f.isClosed() {
				f.readErr = errors.New("tcp: response ended before status trailer")
			} else {
				f.readErr = err
			}
			return
		}
		if env.Flags == wire.FlagStatus && env.Method == "" && env.StreamID == 0 && len(env.Metadata) == 0 {
			f.readErr = statusError(env.Payload)
			return
		}
		// A bare end-of-direction frame tells the initiator nothing the trailer
		// will not: dropping it keeps the handoff free for the next message.
		if env.Flags == wire.FlagEnd && env.Method == "" && env.StreamID == 0 && len(env.Metadata) == 0 && len(env.Payload) == 0 {
			continue
		}
		if env.Flags != 0 || env.Method != "" || env.StreamID != 0 || len(env.Metadata) != 0 {
			f.readErr = errors.New("tcp: invalid response envelope")
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

	env, err := f.nextEnvelope()
	if err != nil {
		return nil, f.finish(f.contextOr(err))
	}
	if env.Method != "" {
		return nil, f.finish(errors.New("tcp: method is only allowed on the first request"))
	}
	if env.StreamID != 0 {
		return nil, f.finish(fmt.Errorf("tcp: unsupported stream id %d", env.StreamID))
	}
	if env.Flags&wire.FlagStatus != 0 {
		if env.Flags != wire.FlagStatus || env.Method != "" || env.StreamID != 0 || len(env.Metadata) != 0 {
			return nil, f.finish(errors.New("tcp: invalid status trailer"))
		}
		return nil, f.finish(errors.New("tcp: status trailer from the call initiator"))
	}
	if env.Flags == wire.FlagEnd {
		if len(env.Metadata) != 0 || len(env.Payload) != 0 {
			return nil, f.finish(errors.New("tcp: invalid end frame"))
		}
		f.recvEnded = true
		return nil, f.finish(io.EOF)
	}
	if env.Flags != 0 {
		return nil, f.finish(errors.New("tcp: invalid envelope flags"))
	}
	if len(env.Metadata) != 0 {
		return nil, f.finish(errors.New("tcp: metadata is only allowed on the first request"))
	}
	return bytes.NewReader(env.Payload), nil
}

func (f *framer) peerEndReceived() bool {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	return f.recvEnded
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
	blob, err := readFrame(f.conn, f.maxFrameSize)
	if err != nil {
		return wire.Envelope{}, err
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

// finish records err as the end of the receive side. The initiator owns its
// connection, so this is where its file descriptor goes back.
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

// Close terminates the TCP call and releases the connection.
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

// statusError turns a status trailer into the result of the call. OK is the
// normal end of the stream, which readers see as io.EOF.
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
		return nil, errors.New("tcp: send after CloseSend")
	}
	return &messageWriter{close: func(payload []byte) error {
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		if f.sentEnd {
			return errors.New("tcp: send after CloseSend")
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

// writeStatus writes the trailer that ends the call. It shares sendMu with the
// handler's own writes even though the handler has returned by then.
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
	blob, err := wire.MarshalEnvelope(env)
	if err != nil {
		return err
	}
	if err := writeFrame(f.conn, blob); err != nil {
		return err
	}
	f.sentFirst = true
	return nil
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

func writeFrame(w io.Writer, blob []byte) error {
	if uint64(len(blob)) > uint64(^uint32(0)) {
		return errors.New("tcp: frame is too large")
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(blob)))
	if err := writeAll(w, prefix[:]); err != nil {
		return err
	}
	return writeAll(w, blob)
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

func readFrame(r io.Reader, limits ...int64) ([]byte, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if len(limits) > 0 && limits[0] > 0 && int64(size) > limits[0] {
		return nil, fmt.Errorf("tcp: frame is %d bytes; maximum is %d", size, limits[0])
	}
	if uint64(size) > uint64(^uint(0)>>1) {
		return nil, errors.New("tcp: frame length overflows int")
	}
	blob := make([]byte, int(size))
	if _, err := io.ReadFull(r, blob); err != nil {
		return nil, err
	}
	return blob, nil
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
				return nil, errors.New("tcp: metadata key or value exceeds 65535 bytes")
			}
			entrySize := int64(4 + len(key) + len(value))
			if int64(buf.Len()) > transport.DefaultMaxMetadataSize-entrySize {
				return nil, errors.New("tcp: metadata exceeds configured limit")
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
			return nil, errors.New("tcp: truncated metadata key length")
		}
		keyLen := int(binary.BigEndian.Uint16(data))
		data = data[2:]
		if len(data) < keyLen+2 {
			return nil, errors.New("tcp: truncated metadata key")
		}
		key := string(data[:keyLen])
		data = data[keyLen:]
		valueLen := int(binary.BigEndian.Uint16(data))
		data = data[2:]
		if len(data) < valueLen {
			return nil, errors.New("tcp: truncated metadata value")
		}
		md[key] = append(md[key], string(data[:valueLen]))
		data = data[valueLen:]
	}
	return md, nil
}
