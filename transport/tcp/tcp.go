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
	conn net.Conn,
	onCall func(context.Context, string, transport.Framer) error,
) {
	defer conn.Close()

	blob, err := readFrame(conn)
	if err != nil {
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
	conn      net.Conn
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

	// frames carries read-ahead messages to the initiator, whose Recv calls stop
	// at the last message it cares about rather than at the status trailer.
	// Reading ahead is what lets the connection be released when the call ends.
	// readErr is written before frames is closed, so a receiver that sees the
	// closed channel also sees the terminal error.
	frames  chan wire.Envelope
	readErr error
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
		// A bare end-of-direction frame tells the initiator nothing the trailer
		// will not: dropping it keeps the handoff free for the next message.
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
		return nil, f.finish(errors.New("tcp: method is only allowed on the first request"))
	}
	if env.StreamID != 0 {
		return nil, f.finish(fmt.Errorf("tcp: unsupported stream id %d", env.StreamID))
	}
	if env.Flags&wire.FlagStatus != 0 {
		return nil, f.finish(errors.New("tcp: status trailer from the call initiator"))
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
	blob, err := readFrame(f.conn)
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.UnmarshalEnvelope(blob)
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
	f.closeOnce.Do(func() { _ = f.conn.Close() })
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
	return errs.Error(errs.Code(code), description)
}

func (f *framer) Send() (io.WriteCloser, error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	if f.sentEnd {
		return nil, errors.New("tcp: send after CloseSend")
	}
	return &messageWriter{close: func(payload []byte) error {
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		if f.sentEnd {
			return errors.New("tcp: send after CloseSend")
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

// writeStatus writes the trailer that ends the call. It shares sendMu with the
// handler's own writes even though the handler has returned by then.
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
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(prefix[:])
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
