// Package udp implements unary Argos calls over UDP.
package udp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sort"
	"sync"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/wire"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

const maxDatagramSize = 65507

type channel struct {
	listen *transport.ListenState
}

// New creates a UDP transport.
func New() transport.Transport {
	return &channel{listen: transport.NewListenState()}
}

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

	go func() {
		<-ctx.Done()
		_ = conn.Close()
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
		go t.serveDatagram(ctx, conn, payload, clientAddr, onCall)
	}
}

func (t *channel) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	call := transport.ApplyClientOptions(opts)
	addr, err := t.listen.DialAddress(ctx, call)
	if err != nil {
		return nil, err
	}
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	localConn, err := net.ListenPacket("udp", "")
	if err != nil {
		return nil, err
	}
	return &framer{
		conn:       localConn,
		remoteAddr: remoteAddr,
		ctx:        ctx,
		method:     method,
		initiator:  true,
	}, nil
}

func (t *channel) serveDatagram(
	ctx context.Context,
	conn net.PacketConn,
	data []byte,
	clientAddr net.Addr,
	onCall func(context.Context, string, transport.Framer) error,
) {
	frames, err := readDatagram(data)
	if err != nil || len(frames) == 0 {
		return
	}
	first, err := wire.UnmarshalEnvelope(frames[0])
	if err != nil || first.Method == "" {
		return
	}
	md, err := decodeMetadata(first.Metadata)
	if err != nil {
		return
	}
	callCtx := metadata.With(ctx, md)
	f := &framer{
		conn:       conn,
		remoteAddr: clientAddr,
		ctx:        callCtx,
		pending:    &first,
	}
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
	conn       net.PacketConn
	remoteAddr net.Addr
	ctx        context.Context
	method     string
	initiator  bool

	sendMu sync.Mutex
	recvMu sync.Mutex

	requestPayload []byte
	sentEnd        bool
	sendUsed       bool

	pending     *wire.Envelope
	response    *wire.Envelope
	recvDone    bool
	terminalErr error

	responsePayload []byte
	responseSent    bool
}

func (f *framer) Recv() (io.Reader, error) {
	f.recvMu.Lock()
	defer f.recvMu.Unlock()

	if f.initiator {
		if !f.sentEnd {
			return nil, errors.New("udp: receive before request")
		}
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

	if f.initiator {
		if f.sendUsed {
			return nil, errors.New("udp: unary stream sent more than one request")
		}
		f.sendUsed = true
		return &messageWriter{close: func(payload []byte) error {
			f.requestPayload = payload
			return nil
		}}, nil
	}

	if f.responseSent {
		return nil, errors.New("udp: unary stream sent more than one response")
	}
	f.responseSent = true
	return &messageWriter{close: func(payload []byte) error {
		f.responsePayload = payload
		return nil
	}}, nil
}

func (f *framer) CloseSend() error {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()

	if f.initiator {
		if f.sentEnd {
			return nil
		}
		md, err := encodeMetadata(metadata.FromContext(f.ctx))
		if err != nil {
			return err
		}
		request, err := wire.MarshalEnvelope(wire.Envelope{
			Method:   f.method,
			Metadata: md,
			Payload:  f.requestPayload,
		})
		if err != nil {
			return err
		}
		end, err := wire.MarshalEnvelope(wire.Envelope{Flags: wire.FlagEnd})
		if err != nil {
			return err
		}
		datagram, err := marshalDatagram(request, end)
		if err != nil {
			return err
		}
		if _, err := f.conn.WriteTo(datagram, f.remoteAddr); err != nil {
			return err
		}
		f.sentEnd = true
		return f.readResponse()
	}
	return nil
}

func (f *framer) readResponse() error {
	buf := make([]byte, maxDatagramSize)
	type readResult struct {
		n   int
		err error
	}
	ch := make(chan readResult, 1)
	go func() {
		n, _, err := f.conn.ReadFrom(buf)
		ch <- readResult{n: n, err: err}
	}()
	select {
	case <-f.ctx.Done():
		return f.ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		return f.parseResponse(buf[:res.n])
	}
}

func (f *framer) parseResponse(data []byte) error {
	frames, err := readDatagram(data)
	if err != nil {
		return err
	}
	for _, blob := range frames {
		env, err := wire.UnmarshalEnvelope(blob)
		if err != nil {
			return err
		}
		if env.Flags&wire.FlagStatus != 0 {
			f.terminalErr = statusError(env.Payload)
			continue
		}
		if env.Flags&wire.FlagEnd != 0 && len(env.Payload) == 0 {
			continue
		}
		if f.response != nil {
			return errors.New("udp: unexpected extra message in response")
		}
		f.response = &env
	}
	if f.terminalErr == nil {
		return errors.New("udp: response datagram missing status")
	}
	return nil
}

func (f *framer) writeResponse(code errs.Code, description string) error {
	var blobs [][]byte
	if f.responseSent {
		blob, err := wire.MarshalEnvelope(wire.Envelope{Payload: f.responsePayload})
		if err != nil {
			return err
		}
		blobs = append(blobs, blob)
	}
	end, err := wire.MarshalEnvelope(wire.Envelope{Flags: wire.FlagEnd})
	if err != nil {
		return err
	}
	blobs = append(blobs, end)
	status, err := wire.MarshalEnvelope(wire.Envelope{
		Flags:   wire.FlagStatus,
		Payload: wire.MarshalStatus(uint32(code), description),
	})
	if err != nil {
		return err
	}
	blobs = append(blobs, status)
	datagram, err := marshalDatagram(blobs...)
	if err != nil {
		return err
	}
	_, err = f.conn.WriteTo(datagram, f.remoteAddr)
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
	return errs.Error(errs.Code(code), description)
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

func readDatagram(data []byte) ([][]byte, error) {
	var frames [][]byte
	reader := bytes.NewReader(data)
	for reader.Len() > 0 {
		blob, err := readFrame(reader)
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
				return nil, errors.New("udp: metadata key or value exceeds 65535 bytes")
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
