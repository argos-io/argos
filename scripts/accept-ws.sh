#!/usr/bin/env bash
# 对跑着 echo.v1.EchoService 的 WebSocket 地址用 python3 二进制信封发一次 Echo。
#
# 用法：  scripts/accept-ws.sh <host:port> [msg]
# 出口码：0 = 收到 hello <msg> 且状态为 OK
set -euo pipefail

if [[ $# -lt 1 ]]; then
	echo "用法: $0 <host:port> [msg]" >&2
	exit 2
fi

exec python3 - "$@" <<'PY'
import base64
import hashlib
import os
import socket
import struct
import sys

FLAG_END = 1 << 0
FLAG_STATUS = 1 << 1
METHOD = "echo.v1.EchoService/Echo"
CODE_OK = 0


def fail(msg):
    sys.exit("accept-ws: %s" % msg)


def varint(value):
    out = b""
    while True:
        byte = value & 0x7F
        value >>= 7
        out += bytes([byte | (0x80 if value else 0)])
        if not value:
            return out


def marshal_msg(text):
    raw = text.encode("utf-8")
    return b"\x0a" + varint(len(raw)) + raw


def unmarshal_msg(payload):
    if not payload:
        return ""
    tag = payload[0]
    if tag != 0x0A:
        fail("响应里出现意外的 protobuf tag %#x" % tag)
    length = payload[1]
    if length & 0x80:
        fail("暂不支持多字节 varint")
    return payload[2:2 + length].decode("utf-8")


def marshal_envelope(method=b"", stream_id=0, flags=0, metadata=b"", payload=b""):
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


def ws_connect(host, port, path="/"):
    sock = socket.create_connection((host, int(port)), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        "GET %s HTTP/1.1\r\n"
        "Host: %s:%s\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    ) % (path, host, port, key)
    sock.sendall(request.encode("ascii"))
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            fail("WebSocket 握手未完成")
        response += chunk
    if b" 101 " not in response.split(b"\r\n", 1)[0]:
        fail("WebSocket 握手失败: %r" % response[:200])
    accept = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
    ).decode("ascii")
    for line in response.split(b"\r\n"):
        if line.lower().startswith(b"sec-websocket-accept:"):
            got = line.split(b":", 1)[1].strip().decode("ascii")
            if got != accept:
                fail("Sec-WebSocket-Accept 不匹配")
            break
    else:
        fail("握手响应缺少 Sec-WebSocket-Accept")
    return sock


def ws_send_binary(sock, payload):
    mask = os.urandom(4)
    masked = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
    length = len(payload)
    if length < 126:
        header = struct.pack("!BB", 0x82, 0x80 | length)
    elif length < 65536:
        header = struct.pack("!BBH", 0x82, 0x80 | 126, length)
    else:
        header = struct.pack("!BBQ", 0x82, 0x80 | 127, length)
    sock.sendall(header + mask + masked)


def ws_recv_binary(sock):
    data = sock.recv(2)
    if len(data) < 2:
        fail("WebSocket 帧被截断")
    opcode = data[0] & 0x0F
    masked = data[1] & 0x80
    length = data[1] & 0x7F
    if length == 126:
        length, = struct.unpack("!H", sock.recv(2))
    elif length == 127:
        length, = struct.unpack("!Q", sock.recv(8))
    if masked:
        mask = sock.recv(4)
        payload = sock.recv(length)
        payload = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
    else:
        payload = sock.recv(length)
    if opcode != 0x2:
        fail("期望 binary 帧，收到 opcode %d" % opcode)
    return payload


def main():
    address = sys.argv[1]
    text = sys.argv[2] if len(sys.argv) > 2 else "ws"
    host, _, port = address.rpartition(":")
    sock = ws_connect(host or "127.0.0.1", port or "80")
    with sock:
        ws_send_binary(sock, marshal_envelope(
            method=METHOD.encode("utf-8"),
            payload=marshal_msg(text),
        ))
        ws_send_binary(sock, marshal_envelope(flags=FLAG_END))

        messages, status = [], None
        while status is None:
            method, stream_id, flags, _, payload = unmarshal_envelope(ws_recv_binary(sock))
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
    print("accept-ws: %s -> %r, 状态 OK" % (address, got))


main()
PY
