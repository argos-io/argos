#!/usr/bin/env bash
# 用 python3 直接说 TCP 长度前缀 + 二进制信封，对一个跑着 echo.v1.EchoService 的
# 地址发一次 Echo。不依赖 Go，用来验证线上格式而不是验证本仓库的实现。
#
# 用法：  scripts/accept-envelope.sh <host:port> [msg]
# 出口码：0 = 收到 hello <msg> 且状态为 OK
set -euo pipefail

if [[ $# -lt 1 ]]; then
	echo "用法: $0 <host:port> [msg]" >&2
	exit 2
fi

exec python3 - "$@" <<'PY'
import socket
import struct
import sys

FLAG_END = 1 << 0
FLAG_STATUS = 1 << 1
METHOD = "echo.v1.EchoService/Echo"
CODE_OK = 0


def fail(msg):
    sys.exit("accept-envelope: %s" % msg)


def marshal_envelope(method=b"", stream_id=0, flags=0, metadata=b"", payload=b""):
    """method len u8 + method + stream id u32be + flags u8 + md len u32be + md + payload."""
    if len(method) > 255:
        fail("method 超过 255 字节")
    return b"".join([
        bytes([len(method)]),
        method,
        struct.pack(">I", stream_id),
        bytes([flags]),
        struct.pack(">I", len(metadata)),
        metadata,
        payload,
    ])


def unmarshal_envelope(blob):
    if len(blob) < 10:
        fail("信封被截断: %d 字节" % len(blob))
    method_len = blob[0]
    offset = 1 + method_len
    method = blob[1:offset].decode("utf-8")
    stream_id, = struct.unpack(">I", blob[offset:offset + 4])
    offset += 4
    flags = blob[offset]
    offset += 1
    metadata_len, = struct.unpack(">I", blob[offset:offset + 4])
    offset += 4
    metadata = blob[offset:offset + metadata_len]
    offset += metadata_len
    return method, stream_id, flags, metadata, blob[offset:]


def marshal_metadata(pairs):
    """重复的 u16 key len + key + u16 val len + val。"""
    out = b""
    for key, value in pairs:
        key, value = key.encode("utf-8"), value.encode("utf-8")
        out += struct.pack(">H", len(key)) + key
        out += struct.pack(">H", len(value)) + value
    return out


def varint(value):
    out = b""
    while True:
        byte = value & 0x7F
        value >>= 7
        out += bytes([byte | (0x80 if value else 0)])
        if not value:
            return out


def read_varint(data, offset):
    value = shift = 0
    while True:
        if offset >= len(data):
            fail("varint 被截断")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7


def marshal_msg(text):
    """protobuf: field 1, wire type 2 —— EchoRequest.msg。"""
    raw = text.encode("utf-8")
    return b"\x0a" + varint(len(raw)) + raw


def unmarshal_msg(payload):
    """只认 EchoResponse 的 field 1，其余 tag 视为异常。"""
    if not payload:
        return ""
    tag, offset = read_varint(payload, 0)
    if tag != 0x0A:
        fail("响应里出现意外的 protobuf tag %#x" % tag)
    length, offset = read_varint(payload, offset)
    return payload[offset:offset + length].decode("utf-8")


def send_frame(sock, blob):
    sock.sendall(struct.pack(">I", len(blob)) + blob)


def read_exactly(sock, size):
    buf = b""
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            fail("连接在读到 %d/%d 字节时关闭" % (len(buf), size))
        buf += chunk
    return buf


def read_frame(sock):
    size, = struct.unpack(">I", read_exactly(sock, 4))
    return read_exactly(sock, size)


def main():
    address = sys.argv[1]
    text = sys.argv[2] if len(sys.argv) > 2 else "envelope"
    host, _, port = address.rpartition(":")
    sock = socket.create_connection((host or "127.0.0.1", int(port)), timeout=10)
    with sock:
        # 首帧带方法名与 metadata；随后的帧方法名长度为 0。
        send_frame(sock, marshal_envelope(
            method=METHOD.encode("utf-8"),
            metadata=marshal_metadata([("accept-envelope", "1")]),
            payload=marshal_msg(text),
        ))
        send_frame(sock, marshal_envelope(flags=FLAG_END))

        messages, status = [], None
        while status is None:
            method, stream_id, flags, _, payload = unmarshal_envelope(read_frame(sock))
            if method:
                fail("响应帧带了方法名 %r" % method)
            if stream_id:
                fail("响应帧的 stream id 是 %d，期望 0" % stream_id)
            if flags & FLAG_STATUS:
                status = payload
            elif flags & FLAG_END and not payload:
                continue
            else:
                messages.append(payload)

    if len(messages) != 1:
        fail("收到 %d 条消息，期望 1 条" % len(messages))
    got, want = unmarshal_msg(messages[0]), "hello " + text
    if got != want:
        fail("响应是 %r，期望 %r" % (got, want))
    if len(status) < 4:
        fail("状态帧被截断: %d 字节" % len(status))
    code, = struct.unpack(">I", status[:4])
    description = status[4:].decode("utf-8")
    if code != CODE_OK or description:
        fail("状态是 (%d, %r)，期望 (0, '')" % (code, description))
    print("accept-envelope: %s -> %r, 状态 OK" % (address, got))


main()
PY
