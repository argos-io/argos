#!/usr/bin/env bash
# TCP 长度前缀信封：对 Watch server-streaming 发一次调用，读三条 Event。
#
# 用法：  scripts/accept-envelope-watch.sh <host:port> [msg]
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
METHOD = "echo.v1.EchoService/Watch"
CODE_OK = 0


def fail(msg):
    sys.exit("accept-envelope-watch: %s" % msg)


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
    stream_id, = struct.unpack(">I", blob[offset:offset + 4])
    offset += 4
    flags = blob[offset]
    offset += 1
    metadata_len, = struct.unpack(">I", blob[offset:offset + 4])
    offset += 4 + metadata_len
    return stream_id, flags, blob[offset:]


def send_frame(sock, blob):
    sock.sendall(struct.pack(">I", len(blob)) + blob)


def read_frame(sock):
    size, = struct.unpack(">I", sock.recv(4))
    buf = b""
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            fail("connection closed")
        buf += chunk
    return buf


def main():
    address = sys.argv[1]
    text = sys.argv[2] if len(sys.argv) > 2 else "watch"
    host, _, port = address.rpartition(":")
    sock = socket.create_connection((host or "127.0.0.1", int(port)), timeout=10)
    with sock:
        send_frame(sock, marshal_envelope(
            method=METHOD.encode("utf-8"),
            payload=marshal_msg(text),
        ))
        send_frame(sock, marshal_envelope(flags=FLAG_END))

        messages, status = [], None
        while status is None:
            _, flags, payload = unmarshal_envelope(read_frame(sock))
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
    print("accept-envelope-watch: %s -> %r OK" % (address, messages))


main()
PY
