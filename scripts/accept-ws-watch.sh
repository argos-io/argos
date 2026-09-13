#!/usr/bin/env bash
# WebSocket 二进制信封：对 Watch server-streaming 发一次调用，读三条 Event。
#
# 用法：  scripts/accept-ws-watch.sh <host:port> [msg]
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
METHOD = "echo.v1.EchoService/Watch"
CODE_OK = 0


def fail(msg):
    sys.exit("accept-ws-watch: %s" % msg)


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
    if not payload or payload[0] != 0x0A:
        fail("unexpected protobuf in response")
    length = payload[1]
    if length & 0x80:
        fail("unsupported varint")
    return payload[2:2 + length].decode("utf-8")


def marshal_envelope(method=b"", stream_id=0, flags=0, metadata=b"", payload=b""):
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
    method_len = blob[0]
    offset = 1 + method_len
    flags = blob[1 + method_len + 4]
    metadata_len, = struct.unpack(">I", blob[offset + 5:offset + 9])
    offset = offset + 9 + metadata_len
    return flags, blob[offset:]


def ws_connect(host, port, path="/"):
    sock = socket.create_connection((host, int(port)), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        "GET %s HTTP/1.1\r\nHost: %s:%s\r\nUpgrade: websocket\r\n"
        "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n"
    ) % (path, host, port, key)
    sock.sendall(request.encode("ascii"))
    response = b""
    while b"\r\n\r\n" not in response:
        response += sock.recv(4096)
    if b" 101 " not in response.split(b"\r\n", 1)[0]:
        fail("WebSocket handshake failed")
    return sock


def ws_send_binary(sock, payload):
    mask = os.urandom(4)
    masked = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
    length = len(payload)
    header = struct.pack("!BB", 0x82, 0x80 | length) if length < 126 else struct.pack("!BBH", 0x82, 0x80 | 126, length)
    sock.sendall(header + mask + masked)


def ws_recv_binary(sock):
    data = sock.recv(2)
    length = data[1] & 0x7F
    if length == 126:
        length, = struct.unpack("!H", sock.recv(2))
    masked = data[1] & 0x80
    if masked:
        mask = sock.recv(4)
        payload = sock.recv(length)
        payload = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
    else:
        payload = sock.recv(length)
    return payload


def main():
    address = sys.argv[1]
    text = sys.argv[2] if len(sys.argv) > 2 else "watch"
    host, _, port = address.rpartition(":")
    sock = ws_connect(host or "127.0.0.1", port or "80")
    with sock:
        ws_send_binary(sock, marshal_envelope(method=METHOD.encode("utf-8"), payload=marshal_msg(text)))
        ws_send_binary(sock, marshal_envelope(flags=FLAG_END))
        messages, status = [], None
        while status is None:
            flags, payload = unmarshal_envelope(ws_recv_binary(sock))
            if flags & FLAG_STATUS:
                status = payload
            elif flags & FLAG_END and not payload:
                continue
            else:
                messages.append(unmarshal_msg(payload))

    want = ["%s one" % text, "%s two" % text, "%s three" % text]
    if messages != want:
        fail("messages = %r, want %r" % (messages, want))
    code, = struct.unpack(">I", status[:4])
    if code != CODE_OK:
        fail("status code = %d" % code)
    print("accept-ws-watch: %s -> %r OK" % (address, messages))


main()
PY
