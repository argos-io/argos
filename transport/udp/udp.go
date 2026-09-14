// Package udp implements unary Argos calls over UDP.
package udp

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
	"sync/atomic"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/internal/wire"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

const maxDatagramSize = 65507

var nextStreamID atomic.Uint32

type channel struct {
	listen *transport.ListenState
}

// New creates a UDP transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

func (*channel) TransportName() string { return "udp" }

func (*channel) SupportsStreaming() bool { return false }

func init() {
	transport.Register("udp", New)
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
		return errors.New("udp: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if onCall == nil {
		return errors.New("udp: nil onCall")
	}
	serve := transport.ApplyServerOptions(opts)
	conn, err := transport.ListenUDP(serve)
	var bound net.Addr
	if conn != nil {
		bound = conn.LocalAddr()
	}
	t.listen.MarkListen(serve.ListenAddress, bound, err)
	if err != nil {
		return err
	}
	defer conn.Close()
	maxMessageSize := transport.ResolveMaxMessageSize(serve.MaxMessageSize)
	if maxMessageSize > maxDatagramSize {
		maxMessageSize = maxDatagramSize
	}
	serveCtx, stop := context.WithCancel(ctx)
	serveDone := make(chan struct{})
	defer close(serveDone)
	defer stop()

	go func() {
		select {
		case <-serveCtx.Done():
			_ = conn.Close()
		case <-serveDone:
		}
	}()

	buf := make([]byte, maxDatagramSize)
	for {
		n, clientAddr, err := conn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		payload := append([]byte(nil), buf[:n]...)
		go t.serveDatagram(serveCtx, conn, payload, clientAddr, onCall, maxMessageSize)
	}
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	if ctx == nil {
		return nil, errors.New("udp: nil context")
	}
	call := transport.ApplyClientOptions(opts)
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	network := "udp4"
	if remoteAddr.IP != nil && remoteAddr.IP.To4() == nil {
		network = "udp6"
	}
	localConn, err := net.ListenUDP(network, nil)
	if err != nil {
		return nil, err
	}
	streamID := nextStreamID.Add(1)
	if streamID == 0 {
		streamID = nextStreamID.Add(1)
	}
	maxMessageSize := transport.ResolveMaxMessageSize(call.MaxMessageSize)
	if maxMessageSize > maxDatagramSize {
		maxMessageSize = maxDatagramSize
	}
	f := &framer{
		conn:           localConn,
		remoteAddr:     remoteAddr,
		ctx:            ctx,
		method:         method,
		initiator:      true,
		streamID:       streamID,
		maxMessageSize: maxMessageSize,
		done:           make(chan struct{}),
	}
	f.watchContext()
	return f, nil
}

func (t *channel) serveDatagram(
	ctx context.Context,
	conn net.PacketConn,
	data []byte,
	clientAddr net.Addr,
	onCall func(context.Context, string, transport.Framer) error,
	maxMessageSize int64,
) {
	frames, err := readDatagram(data, int64(len(data)))
	if err != nil || len(frames) != 2 {
		return
	}
	first, err := wire.UnmarshalEnvelope(frames[0])
	if err != nil || first.Method == "" {
		return
	}
	if first.StreamID == 0 || first.Flags != 0 {
		return
	}
	end, err := wire.UnmarshalEnvelope(frames[1])
	if err != nil || end.Method != "" || end.StreamID != first.StreamID || end.Flags != wire.FlagEnd || len(end.Metadata) != 0 || len(end.Payload) != 0 {
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
		remoteAddr:     clientAddr,
		ctx:            callCtx,
		streamID:       first.StreamID,
		maxMessageSize: maxMessageSize,
		pending:        &first,
		done:           make(chan struct{}),
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
	_ = f.writeResponse(code, description)
}

type framer struct {
	conn           net.PacketConn
	remoteAddr     net.Addr
	ctx            context.Context
	method         string
	initiator      bool
	streamID       uint32
	maxMessageSize int64

	stateMu   sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
	done      chan struct{}

	sendMu sync.Mutex
	recvMu sync.Mutex

	requestPayload []byte
	sentEnd        bool
	sendUsed       bool

	pending      *wire.Envelope
	response     *wire.Envelope
	recvDone     bool
	terminalErr  error
	responseOnce sync.Once
	responseErr  error

	responsePayload []byte
	responseStarted bool
	responseSent    bool
}

func (f *framer) Recv() (io.Reader, error) {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return nil, ctxErr
	}
	if f.isClosed() {
		f.recvMu.Lock()
		recvDone := f.recvDone
		terminalErr := f.terminalErr
		f.recvMu.Unlock()
		if recvDone && terminalErr != nil {
			return nil, terminalErr
		}
		return nil, net.ErrClosed
	}

	if f.initiator {
		f.recvMu.Lock()
		recvDone := f.recvDone
		terminalErr := f.terminalErr
		responseLoaded := f.response != nil || terminalErr != nil
		f.recvMu.Unlock()
		if !f.sendEnded() {
			return nil, errors.New("udp: receive before request")
		}
		if recvDone {
			if terminalErr != nil {
				return nil, terminalErr
			}
			return nil, errors.New("udp: unary stream received more than one response")
		}

		if !responseLoaded {
			if err := f.ensureResponse(); err != nil {
				f.recvMu.Lock()
				f.recvDone = true
				f.recvMu.Unlock()
				return nil, err
			}
		}

		f.recvMu.Lock()
		defer f.recvMu.Unlock()
		if f.recvDone {
			if f.terminalErr != nil {
				return nil, f.terminalErr
			}
			return nil, errors.New("udp: unary stream received more than one response")
		}
		f.recvDone = true
		if f.response == nil {
			if f.terminalErr != nil {
				return nil, f.terminalErr
			}
			return nil, io.EOF
		}
		return bytes.NewReader(f.response.Payload), nil
	}

	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	if f.sendEnded() {
		return nil, errors.New("udp: receive after CloseSend")
	}

	if f.recvDone {
		return nil, io.EOF
	}
	f.recvDone = true
	if f.pending != nil {
		payload := f.pending.Payload
		f.pending = nil
		return bytes.NewReader(payload), nil
	}
	return nil, errors.New("udp: no request payload")
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

	if f.initiator {
		if f.sendUsed {
			return nil, errors.New("udp: unary stream sent more than one request")
		}
		f.sendUsed = true
		return &messageWriter{close: func(payload []byte) error {
			f.sendMu.Lock()
			defer f.sendMu.Unlock()
			if ctxErr := f.contextErr(); ctxErr != nil {
				return ctxErr
			}
			if f.isClosed() {
				return net.ErrClosed
			}
			if f.sendEnded() {
				return errors.New("udp: send after CloseSend")
			}
			if int64(len(payload)) > f.maxMessageSize {
				return fmt.Errorf("udp: message is %d bytes; maximum is %d", len(payload), f.maxMessageSize)
			}
			f.requestPayload = append([]byte(nil), payload...)
			return nil
		}}, nil
	}

	if f.responseSent {
		return nil, errors.New("udp: unary stream sent more than one response")
	}
	if f.responseStarted {
		return nil, errors.New("udp: unary stream has an unfinished response")
	}
	f.responseStarted = true
	return &messageWriter{close: func(payload []byte) error {
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		if ctxErr := f.contextErr(); ctxErr != nil {
			return ctxErr
		}
		if f.isClosed() {
			return net.ErrClosed
		}
		if f.sendEnded() {
			return errors.New("udp: send after CloseSend")
		}
		if int64(len(payload)) > f.maxMessageSize {
			return fmt.Errorf("udp: message is %d bytes; maximum is %d", len(payload), f.maxMessageSize)
		}
		f.responsePayload = append([]byte(nil), payload...)
		f.responseSent = true
		return nil
	}}, nil
}

func (f *framer) CloseSend() error {
	f.sendMu.Lock()
	if ctxErr := f.contextErr(); ctxErr != nil {
		f.sendMu.Unlock()
		return ctxErr
	}
	if f.isClosed() {
		f.sendMu.Unlock()
		return net.ErrClosed
	}

	if f.initiator {
		if f.sendEnded() {
			f.sendMu.Unlock()
			return nil
		}
		md, err := encodeMetadata(metadata.FromContext(f.ctx))
		if err != nil {
			f.sendMu.Unlock()
			return err
		}
		request, err := wire.MarshalEnvelope(wire.Envelope{
			Method:   f.method,
			StreamID: f.streamID,
			Metadata: md,
			Payload:  f.requestPayload,
		})
		if err != nil {
			f.sendMu.Unlock()
			return err
		}
		end, err := wire.MarshalEnvelope(wire.Envelope{StreamID: f.streamID, Flags: wire.FlagEnd})
		if err != nil {
			f.sendMu.Unlock()
			return err
		}
		datagram, err := marshalDatagram(request, end)
		if err != nil {
			f.sendMu.Unlock()
			return err
		}
		if len(datagram) > maxDatagramSize {
			f.sendMu.Unlock()
			return fmt.Errorf("udp: request datagram is %d bytes; maximum is %d", len(datagram), maxDatagramSize)
		}
		if n, err := f.conn.WriteTo(datagram, f.remoteAddr); err != nil {
			f.sendMu.Unlock()
			return f.contextOr(err)
		} else if n != len(datagram) {
			f.sendMu.Unlock()
			return io.ErrShortWrite
		}
		f.setSendEnded()
		f.sendMu.Unlock()
		return nil
	}
	f.setSendEnded()
	f.sendMu.Unlock()
	return nil
}

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

// Close terminates the UDP call. A client owns its packet connection; a
// server framer shares the listener and therefore only marks the call closed.
func (f *framer) Close() error {
	f.closeOnce.Do(func() {
		f.stateMu.Lock()
		f.closed = true
		f.stateMu.Unlock()
		if f.done != nil {
			close(f.done)
		}
		if f.initiator {
			f.closeErr = f.conn.Close()
		}
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

func (f *framer) sendEnded() bool {
	f.stateMu.RLock()
	defer f.stateMu.RUnlock()
	return f.sentEnd
}

func (f *framer) setSendEnded() {
	f.stateMu.Lock()
	f.sentEnd = true
	f.stateMu.Unlock()
}

func (f *framer) readResponse() error {
	buf := make([]byte, maxDatagramSize)
	type readResult struct {
		n    int
		addr net.Addr
		err  error
	}
	ch := make(chan readResult, 1)
	go func() {
		for {
			n, addr, err := f.conn.ReadFrom(buf)
			if err != nil {
				ch <- readResult{n: n, addr: addr, err: err}
				return
			}
			if sameUDPAddr(addr, f.remoteAddr) {
				ch <- readResult{n: n, addr: addr}
				return
			}
		}
	}()
	select {
	case <-f.ctx.Done():
		_ = f.Close()
		return f.ctx.Err()
	case res := <-ch:
		if res.err != nil {
			if f.isClosed() {
				if f.ctx != nil && f.ctx.Err() != nil {
					return f.ctx.Err()
				}
				return net.ErrClosed
			}
			return res.err
		}
		return f.parseResponse(buf[:res.n])
	}
}

// ensureResponse keeps response I/O in Recv so CloseSend remains a true
// half-close operation. sync.Once also serializes concurrent Recv calls and
// prevents multiple readers from consuming the same UDP socket response.
func (f *framer) ensureResponse() error {
	f.responseOnce.Do(func() {
		err := f.readResponse()
		f.responseErr = err
		if err != nil {
			f.recvMu.Lock()
			if f.terminalErr == nil {
				f.terminalErr = err
			}
			f.recvMu.Unlock()
			_ = f.Close()
		}
	})
	return f.responseErr
}

func (f *framer) parseResponse(data []byte) error {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()
	frames, err := readDatagram(data, int64(len(data)))
	if err != nil {
		return err
	}
	statusSeen := false
	endSeen := false
	for _, blob := range frames {
		if statusSeen {
			return errors.New("udp: response status frame must be last")
		}
		env, err := wire.UnmarshalEnvelope(blob)
		if err != nil {
			return err
		}
		if err := wire.ValidateEnvelope(env, f.maxMessageSize, transport.DefaultMaxMetadataSize); err != nil {
			return err
		}
		if env.StreamID != f.streamID {
			return fmt.Errorf("udp: response stream id %d does not match request %d", env.StreamID, f.streamID)
		}
		if env.Method != "" {
			return errors.New("udp: response must not contain a method")
		}
		if len(env.Metadata) != 0 {
			return errors.New("udp: response metadata is not allowed")
		}
		if env.Flags&wire.FlagStatus != 0 {
			if env.Flags != wire.FlagStatus || !endSeen || len(env.Metadata) != 0 {
				return errors.New("udp: invalid response status frame")
			}
			f.terminalErr = statusError(env.Payload)
			statusSeen = true
			continue
		}
		if env.Flags&wire.FlagEnd != 0 && len(env.Payload) == 0 {
			if endSeen || env.Flags != wire.FlagEnd || len(env.Metadata) != 0 {
				return errors.New("udp: invalid response end frame")
			}
			endSeen = true
			continue
		}
		if env.Flags != 0 {
			return errors.New("udp: invalid response flags")
		}
		if endSeen {
			return errors.New("udp: response message follows end frame")
		}
		if f.response != nil {
			return errors.New("udp: unexpected extra message in response")
		}
		f.response = &env
	}
	if !statusSeen || !endSeen {
		return errors.New("udp: response datagram missing status")
	}
	// A responder may have emitted a valid unary payload before discovering a
	// later application error (for example, a server-streaming method used over
	// this unary-only transport). Preserve both pieces of information: Recv
	// returns the payload first and the terminal error on the next call.
	return nil
}

func (f *framer) writeResponse(code errs.Code, description string) error {
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if f.isClosed() {
		return net.ErrClosed
	}
	f.sendMu.Lock()
	responseSent := f.responseSent
	responsePayload := append([]byte(nil), f.responsePayload...)
	f.sendMu.Unlock()
	var blobs [][]byte
	if responseSent {
		if int64(len(responsePayload)) > f.maxMessageSize {
			return fmt.Errorf("udp: message is %d bytes; maximum is %d", len(responsePayload), f.maxMessageSize)
		}
		blob, err := wire.MarshalEnvelope(wire.Envelope{StreamID: f.streamID, Payload: responsePayload})
		if err != nil {
			return err
		}
		blobs = append(blobs, blob)
	}
	end, err := wire.MarshalEnvelope(wire.Envelope{StreamID: f.streamID, Flags: wire.FlagEnd})
	if err != nil {
		return err
	}
	blobs = append(blobs, end)
	baseDatagram, err := marshalDatagram(blobs...)
	if err != nil {
		return err
	}
	statusEmpty, err := wire.MarshalEnvelope(wire.Envelope{
		StreamID: f.streamID,
		Flags:    wire.FlagStatus,
		Payload:  wire.MarshalStatus(uint32(code), ""),
	})
	if err != nil {
		return err
	}
	statusDescriptionLimit := int64(maxDatagramSize - len(baseDatagram) - 4 - len(statusEmpty))
	if statusDescriptionLimit < 0 {
		return fmt.Errorf("udp: response datagram is too large before status description")
	}
	statusDescriptionLimit = minStatusLimit(statusDescriptionLimit, f.maxMessageSize-4)
	statusEnv := wire.Envelope{
		StreamID: f.streamID,
		Flags:    wire.FlagStatus,
		Payload: wire.MarshalStatus(
			uint32(code),
			statusmap.LimitStatusMessage(description, statusDescriptionLimit),
		),
	}
	if err := wire.ValidateEnvelope(statusEnv, f.maxMessageSize, transport.DefaultMaxMetadataSize); err != nil {
		return err
	}
	status, err := wire.MarshalEnvelope(statusEnv)
	if err != nil {
		return err
	}
	blobs = append(blobs, status)
	datagram, err := marshalDatagram(blobs...)
	if err != nil {
		return err
	}
	if len(datagram) > maxDatagramSize {
		return fmt.Errorf("udp: response datagram is %d bytes; maximum is %d", len(datagram), maxDatagramSize)
	}
	if ctxErr := f.contextErr(); ctxErr != nil {
		return ctxErr
	}
	if f.isClosed() {
		return net.ErrClosed
	}
	n, err := f.conn.WriteTo(datagram, f.remoteAddr)
	if err != nil {
		return f.contextOr(err)
	}
	if n != len(datagram) {
		return io.ErrShortWrite
	}
	return nil
}

func minStatusLimit(left, right int64) int64 {
	if right < 0 {
		return 0
	}
	if left < right {
		return left
	}
	return right
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

func marshalDatagram(blobs ...[]byte) ([]byte, error) {
	var buf bytes.Buffer
	for _, blob := range blobs {
		if err := writeFrame(&buf, blob); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func readDatagram(data []byte, limits ...int64) ([][]byte, error) {
	var frames [][]byte
	reader := bytes.NewReader(data)
	for reader.Len() > 0 {
		blob, err := readFrame(reader, limits...)
		if err != nil {
			return nil, err
		}
		frames = append(frames, blob)
	}
	return frames, nil
}

func writeFrame(w io.Writer, blob []byte) error {
	if uint64(len(blob)) > uint64(^uint32(0)) {
		return errors.New("udp: frame is too large")
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
		return nil, fmt.Errorf("udp: frame is %d bytes; maximum is %d", size, limits[0])
	}
	if uint64(size) > uint64(^uint(0)>>1) {
		return nil, errors.New("udp: frame length overflows int")
	}
	blob := make([]byte, int(size))
	if _, err := io.ReadFull(r, blob); err != nil {
		return nil, err
	}
	return blob, nil
}

func sameUDPAddr(a, b net.Addr) bool {
	left, leftOK := a.(*net.UDPAddr)
	right, rightOK := b.(*net.UDPAddr)
	if !leftOK || !rightOK {
		return a != nil && b != nil && a.String() == b.String()
	}
	return left.Port == right.Port && left.Zone == right.Zone && left.IP.Equal(right.IP)
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
				return nil, errors.New("udp: metadata key or value exceeds 65535 bytes")
			}
			entrySize := int64(4) + int64(len(key)) + int64(len(value))
			if entrySize > transport.DefaultMaxMetadataSize || int64(buf.Len()) > transport.DefaultMaxMetadataSize-entrySize {
				return nil, fmt.Errorf("udp: metadata is larger than %d bytes", transport.DefaultMaxMetadataSize)
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
		return nil, fmt.Errorf("udp: metadata is %d bytes; maximum is %d", len(data), transport.DefaultMaxMetadataSize)
	}
	md := make(metadata.Metadata)
	for len(data) != 0 {
		if len(data) < 2 {
			return nil, errors.New("udp: truncated metadata key length")
		}
		keyLen := int(binary.BigEndian.Uint16(data))
		data = data[2:]
		if len(data) < keyLen+2 {
			return nil, errors.New("udp: truncated metadata key")
		}
		key := string(data[:keyLen])
		data = data[keyLen:]
		valueLen := int(binary.BigEndian.Uint16(data))
		data = data[2:]
		if len(data) < valueLen {
			return nil, errors.New("udp: truncated metadata value")
		}
		md[key] = append(md[key], string(data[:valueLen]))
		data = data[valueLen:]
	}
	return md, nil
}
