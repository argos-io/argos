package probe

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
)

// §4.5 envelope 帧类型。探针手写线格式，里程碑 ② 的 framing/envelope 会替换它。
const (
	frameOpen    byte = 1
	frameHeaders byte = 2
	frameData    byte = 3
	frameEnd     byte = 4
	frameStatus  byte = 5

	openFlagEnd byte = 1 << 0 // OPEN|END：零消息调用

	// IPv4 UDP 单包有效载荷上限：65535 − 20 (IP) − 8 (UDP) = 65507。
	udpMaxPayload = 65507
)

type envHeader struct {
	Name, Value string
}

// countedPacketConn 包装 net.PacketConn，统计 ReadFrom / WriteTo 次数。
type countedPacketConn struct {
	net.PacketConn
	reads, writes atomic.Int32
}

func (c *countedPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addr, err = c.PacketConn.ReadFrom(p)
	if err != nil {
		return n, addr, err
	}
	c.reads.Add(1)
	return n, addr, err
}

func (c *countedPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	n, err = c.PacketConn.WriteTo(p, addr)
	if err != nil {
		return n, err
	}
	c.writes.Add(1)
	return n, err
}

func newCountedUDP(t *testing.T) *countedPacketConn {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return &countedPacketConn{PacketConn: pc}
}

func appendU16(dst []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(dst, b[:]...)
}

func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendU64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

func appendString(dst []byte, s string) []byte {
	if len(s) > 0xffff {
		panic("string too long for probe wire format")
	}
	dst = appendU16(dst, uint16(len(s)))
	return append(dst, s...)
}

func encodeMetadata(dst []byte, hdrs []envHeader) []byte {
	dst = appendU16(dst, uint16(len(hdrs)))
	for _, h := range hdrs {
		dst = appendString(dst, h.Name)
		dst = appendString(dst, h.Value)
	}
	return dst
}

func metadataWireLen(body []byte) (int, error) {
	if len(body) < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(body[0:2]))
	off := 2
	for range n {
		if off+2 > len(body) {
			return 0, io.ErrUnexpectedEOF
		}
		nameLen := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2 + nameLen
		if off+2 > len(body) {
			return 0, io.ErrUnexpectedEOF
		}
		valLen := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2 + valLen
	}
	return off, nil
}

func encodeFrameBody(typ byte, callID uint64, payload []byte) []byte {
	body := make([]byte, 0, 1+8+len(payload))
	body = append(body, typ)
	body = appendU64(body, callID)
	body = append(body, payload...)
	return body
}

func encodePrefixedFrame(typ byte, callID uint64, payload []byte) []byte {
	body := encodeFrameBody(typ, callID, payload)
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(len(body)))
	copy(out[4:], body)
	return out
}

func encodeOpenPayload(method string, hdrs []envHeader, withEnd bool) []byte {
	var flags byte
	if withEnd {
		flags |= openFlagEnd
	}
	out := []byte{flags}
	out = appendString(out, method)
	out = encodeMetadata(out, hdrs)
	return out
}

func encodeStatusPayload(code uint32, msg string, trailers []envHeader) []byte {
	out := appendU32(nil, code)
	out = appendString(out, msg)
	out = encodeMetadata(out, trailers)
	return out
}

// encodeUDPBatch 把多帧批量编码为一个 UDP 数据报（无 uint32 长度前缀）。
func encodeUDPBatch(frames ...[]byte) []byte {
	n := 0
	for _, f := range frames {
		n += len(f)
	}
	out := make([]byte, 0, n)
	for _, f := range frames {
		out = append(out, f...)
	}
	return out
}

func encodeRequestDatagram(callID uint64, method string, reqData []byte, zeroMsg bool) []byte {
	open := encodeFrameBody(frameOpen, callID, encodeOpenPayload(method, nil, zeroMsg))
	if zeroMsg {
		return encodeUDPBatch(open)
	}
	data := encodeFrameBody(frameData, callID, reqData)
	end := encodeFrameBody(frameEnd, callID, nil)
	return encodeUDPBatch(open, data, end)
}

func encodeResponseDatagram(callID uint64, hdrs []envHeader, rspData []byte, code uint32, msg string) []byte {
	var frames [][]byte
	if len(hdrs) > 0 {
		frames = append(frames, encodeFrameBody(frameHeaders, callID, encodeMetadata(nil, hdrs)))
	}
	if rspData != nil {
		frames = append(frames, encodeFrameBody(frameData, callID, rspData))
	}
	frames = append(frames, encodeFrameBody(frameStatus, callID, encodeStatusPayload(code, msg, nil)))
	return encodeUDPBatch(frames...)
}

type parsedFrame struct {
	typ    byte
	callID uint64
	body   []byte
}

func parseFrame(data []byte) (parsedFrame, int, error) {
	if len(data) < 9 {
		return parsedFrame{}, 0, io.ErrUnexpectedEOF
	}
	f := parsedFrame{
		typ:    data[0],
		callID: binary.BigEndian.Uint64(data[1:9]),
		body:   data[9:],
	}
	switch f.typ {
	case frameOpen:
		if len(f.body) < 3 {
			return parsedFrame{}, 0, io.ErrUnexpectedEOF
		}
		methodLen := int(binary.BigEndian.Uint16(f.body[1:3]))
		metaLen, err := metadataWireLen(f.body[3+methodLen:])
		if err != nil {
			return parsedFrame{}, 0, err
		}
		return f, 9 + 3 + methodLen + metaLen, nil
	case frameHeaders:
		metaLen, err := metadataWireLen(f.body)
		if err != nil {
			return parsedFrame{}, 0, err
		}
		return f, 9 + metaLen, nil
	case frameData:
		return f, len(data), nil // DATA 在批量编码里占剩余字节，由调用方界定
	case frameEnd:
		if len(f.body) != 0 {
			return parsedFrame{}, 0, errors.New("END must have empty body")
		}
		return f, 9, nil
	case frameStatus:
		if len(f.body) < 6 {
			return parsedFrame{}, 0, io.ErrUnexpectedEOF
		}
		msgLen := int(binary.BigEndian.Uint16(f.body[4:6]))
		metaLen, err := metadataWireLen(f.body[6+msgLen:])
		if err != nil {
			return parsedFrame{}, 0, err
		}
		return f, 9 + 6 + msgLen + metaLen, nil
	default:
		return parsedFrame{}, 0, errors.New("unknown frame type")
	}
}

type requestCall struct {
	callID uint64
	method string
	data   []byte
	zero   bool
}

func decodeRequestDatagram(pkt []byte) (requestCall, error) {
	open, openLen, err := parseFrame(pkt)
	if err != nil {
		return requestCall{}, err
	}
	if open.typ != frameOpen {
		return requestCall{}, errors.New("request must start with OPEN")
	}
	if len(open.body) < 1 {
		return requestCall{}, io.ErrUnexpectedEOF
	}
	flags := open.body[0]
	methodLen := binary.BigEndian.Uint16(open.body[1:3])
	method := string(open.body[3 : 3+methodLen])
	req := requestCall{callID: open.callID, method: method, zero: flags&openFlagEnd != 0}
	if req.zero {
		if len(pkt) != openLen {
			return requestCall{}, errors.New("OPEN|END must be sole frame")
		}
		return req, nil
	}
	if len(pkt) < openLen+9+9 {
		return requestCall{}, io.ErrUnexpectedEOF
	}
	if pkt[openLen] != frameData {
		return requestCall{}, errors.New("request must contain DATA after OPEN")
	}
	dataCallID := binary.BigEndian.Uint64(pkt[openLen+1 : openLen+9])
	if dataCallID != open.callID {
		return requestCall{}, errors.New("DATA call ID mismatch")
	}
	endOff := len(pkt) - 9
	if pkt[endOff] != frameEnd {
		return requestCall{}, errors.New("request must end with END")
	}
	endCallID := binary.BigEndian.Uint64(pkt[endOff+1 : endOff+9])
	if endCallID != open.callID {
		return requestCall{}, errors.New("END call ID mismatch")
	}
	req.data = append([]byte(nil), pkt[openLen+9:endOff]...)
	return req, nil
}

type responseCall struct {
	callID  uint64
	headers []envHeader
	data    []byte
	code    uint32
	msg     string
}

func decodeMetadata(body []byte) []envHeader {
	n, err := metadataWireLen(body)
	if err != nil || n == 0 {
		return nil
	}
	count := binary.BigEndian.Uint16(body[0:2])
	off := 2
	var hdrs []envHeader
	for range count {
		nameLen := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		name := string(body[off : off+nameLen])
		off += nameLen
		valLen := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		val := string(body[off : off+valLen])
		off += valLen
		hdrs = append(hdrs, envHeader{Name: name, Value: val})
	}
	return hdrs
}

func parseStatusFromEnd(pkt []byte) (parsedFrame, int, error) {
	for off := len(pkt) - 9; off >= 0; off-- {
		if pkt[off] != frameStatus {
			continue
		}
		f, n, err := parseFrame(pkt[off:])
		if err != nil || f.typ != frameStatus || off+n != len(pkt) {
			continue
		}
		return f, off, nil
	}
	return parsedFrame{}, 0, errors.New("response must end with STATUS")
}

func decodeResponseDatagram(pkt []byte, wantCallID uint64) (responseCall, bool, error) {
	if len(pkt) < 9 {
		return responseCall{}, false, io.ErrUnexpectedEOF
	}
	statusFrame, statusOff, err := parseStatusFromEnd(pkt)
	if err != nil {
		return responseCall{}, false, err
	}
	if statusFrame.callID != wantCallID {
		return responseCall{}, false, nil
	}

	var rsp responseCall
	rsp.callID = wantCallID
	if len(statusFrame.body) < 6 {
		return responseCall{}, false, io.ErrUnexpectedEOF
	}
	rsp.code = binary.BigEndian.Uint32(statusFrame.body[0:4])
	msgLen := binary.BigEndian.Uint16(statusFrame.body[4:6])
	rsp.msg = string(statusFrame.body[6 : 6+msgLen])

	prefix := pkt[:statusOff]
	off := 0
	if len(prefix) > 0 && prefix[0] == frameHeaders {
		hdrFrame, hdrLen, err := parseFrame(prefix)
		if err != nil {
			return responseCall{}, false, err
		}
		if hdrFrame.callID != wantCallID {
			return responseCall{}, false, nil
		}
		rsp.headers = decodeMetadata(hdrFrame.body)
		off = hdrLen
	}
	if off < len(prefix) {
		if prefix[off] != frameData {
			return responseCall{}, false, errors.New("unexpected frame before STATUS")
		}
		dataCallID := binary.BigEndian.Uint64(prefix[off+1 : off+9])
		if dataCallID != wantCallID {
			return responseCall{}, false, nil
		}
		rsp.data = append([]byte(nil), prefix[off+9:]...)
	}
	return rsp, true, nil
}

func recvFirstMessage(req requestCall) ([]byte, error) {
	if req.zero {
		return nil, io.EOF
	}
	return req.data, nil
}

func udpRoundTrip(t *testing.T, client, server *countedPacketConn, reqPkt, rspPkt []byte) responseCall {
	t.Helper()
	serverAddr := server.LocalAddr()

	if _, err := client.WriteTo(reqPkt, serverAddr); err != nil {
		t.Fatalf("client WriteTo: %v", err)
	}
	buf := make([]byte, udpMaxPayload)
	n, from, err := server.ReadFrom(buf)
	if err != nil {
		t.Fatalf("server ReadFrom: %v", err)
	}
	req, err := decodeRequestDatagram(buf[:n])
	if err != nil {
		t.Fatalf("server decode request: %v", err)
	}
	if _, err := recvFirstMessage(req); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("server recv: %v", err)
	}
	if _, err := server.WriteTo(rspPkt, from); err != nil {
		t.Fatalf("server WriteTo: %v", err)
	}
	n, from, err = client.ReadFrom(buf)
	if err != nil {
		t.Fatalf("client ReadFrom: %v", err)
	}
	if from.String() != serverAddr.String() {
		t.Fatalf("response from %v, want %v", from, serverAddr)
	}
	rsp, ok, err := decodeResponseDatagram(buf[:n], req.callID)
	if err != nil || !ok {
		t.Fatalf("client decode response: ok=%v err=%v", ok, err)
	}
	return rsp
}

func TestUDPSingleRequestResponseDatagram(t *testing.T) {
	client := newCountedUDP(t)
	server := newCountedUDP(t)

	const (
		callID = uint64(0x0102030405060708)
		method = "echo.v1.EchoService.Echo"
	)
	reqData := []byte("hello-udp")
	rspData := []byte("pong-udp")

	req := encodeRequestDatagram(callID, method, reqData, false)
	rsp := encodeResponseDatagram(callID, []envHeader{{Name: "x-trace", Value: "probe"}}, rspData, 0, "")

	decReq, err := decodeRequestDatagram(req)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	got, err := recvFirstMessage(decReq)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if string(got) != string(reqData) {
		t.Fatalf("request data = %q, want %q", got, reqData)
	}

	gotRsp := udpRoundTrip(t, client, server, req, rsp)

	if client.reads.Load() != 1 || client.writes.Load() != 1 {
		t.Fatalf("client datagrams read=%d write=%d, want 1/1", client.reads.Load(), client.writes.Load())
	}
	if server.reads.Load() != 1 || server.writes.Load() != 1 {
		t.Fatalf("server datagrams read=%d write=%d, want 1/1", server.reads.Load(), server.writes.Load())
	}
	if gotRsp.code != 0 || string(gotRsp.data) != string(rspData) {
		t.Fatalf("response = code %d data %q, want 0 %q", gotRsp.code, gotRsp.data, rspData)
	}
	if len(gotRsp.headers) != 1 || gotRsp.headers[0].Value != "probe" {
		t.Fatalf("headers = %+v, want x-trace=probe", gotRsp.headers)
	}
}

func TestUDPZeroMessageOpenEnd(t *testing.T) {
	client := newCountedUDP(t)
	server := newCountedUDP(t)

	const callID = uint64(42)
	req := encodeRequestDatagram(callID, "echo.v1.EchoService.Echo", nil, true)
	rsp := encodeResponseDatagram(callID, nil, []byte("ok"), 0, "")

	decReq, err := decodeRequestDatagram(req)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if !decReq.zero {
		t.Fatal("expected OPEN|END zero-message request")
	}
	if _, err := recvFirstMessage(decReq); !errors.Is(err, io.EOF) {
		t.Fatalf("first Recv = %v, want io.EOF", err)
	}

	udpRoundTrip(t, client, server, req, rsp)

	if client.reads.Load() != 1 || client.writes.Load() != 1 {
		t.Fatalf("client datagrams read=%d write=%d, want 1/1", client.reads.Load(), client.writes.Load())
	}
	if server.reads.Load() != 1 || server.writes.Load() != 1 {
		t.Fatalf("server datagrams read=%d write=%d, want 1/1", server.reads.Load(), server.writes.Load())
	}
}

func TestUDPWrongCallIDDiscarded(t *testing.T) {
	const wantID = uint64(100)
	const wrongID = uint64(999)

	good := encodeResponseDatagram(wantID, nil, []byte("ok"), 0, "")
	bad := encodeResponseDatagram(wrongID, nil, []byte("wrong"), 0, "")

	if _, ok, err := decodeResponseDatagram(bad, wantID); err != nil {
		t.Fatalf("decode bad: %v", err)
	} else if ok {
		t.Fatal("wrong call ID response must be discarded")
	}

	got, ok, err := decodeResponseDatagram(good, wantID)
	if err != nil || !ok {
		t.Fatalf("decode good: ok=%v err=%v", ok, err)
	}
	if string(got.data) != "ok" {
		t.Fatalf("data = %q, want ok", got.data)
	}

	client := newCountedUDP(t)
	server := newCountedUDP(t)
	req := encodeRequestDatagram(wantID, "echo.v1.EchoService.Echo", []byte("x"), false)
	if _, err := client.WriteTo(req, server.LocalAddr()); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	buf := make([]byte, udpMaxPayload)
	if _, from, err := server.ReadFrom(buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	} else if _, err := server.WriteTo(bad, from); err != nil {
		t.Fatalf("WriteTo bad: %v", err)
	} else if _, err := server.WriteTo(good, from); err != nil {
		t.Fatalf("WriteTo good: %v", err)
	}

	n, _, err := client.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if _, ok, err := decodeResponseDatagram(buf[:n], wantID); err != nil || ok {
		t.Fatalf("first response should be discarded: ok=%v err=%v", ok, err)
	}

	n, _, err = client.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	got, ok, err = decodeResponseDatagram(buf[:n], wantID)
	if err != nil || !ok {
		t.Fatalf("second response: ok=%v err=%v", ok, err)
	}
	if string(got.data) != "ok" {
		t.Fatalf("data = %q, want ok", got.data)
	}
}

func TestUDPDatagramSizeLimit(t *testing.T) {
	pc := newCountedUDP(t)
	addr := pc.LocalAddr()

	atLimit := make([]byte, udpMaxPayload)
	if _, err := pc.WriteTo(atLimit, addr); err != nil {
		t.Fatalf("WriteTo %d bytes: %v", udpMaxPayload, err)
	}

	overLimit := make([]byte, udpMaxPayload+1)
	_, err := pc.WriteTo(overLimit, addr)
	if err == nil {
		t.Fatalf("WriteTo %d bytes succeeded, want error", udpMaxPayload+1)
	}
	t.Logf("UDP max payload = %d; %d bytes rejected: %v", udpMaxPayload, udpMaxPayload+1, err)
}

func TestUDPPrefixedFrameEncoding(t *testing.T) {
	callID := uint64(7)
	open := encodePrefixedFrame(frameOpen, callID, encodeOpenPayload("m", nil, true))
	if len(open) < 4+9 {
		t.Fatalf("prefixed OPEN too short: %d", len(open))
	}
	flen := binary.BigEndian.Uint32(open[:4])
	if int(flen) != len(open)-4 {
		t.Fatalf("length prefix = %d, body = %d", flen, len(open)-4)
	}
}
