#!/usr/bin/env bash
# 对跑着 echo.v1.EchoService 的 UDP 地址用 python3 长度前缀信封发一次 Echo。
#
# 用法：  scripts/accept-udp.sh <host:port> [msg]
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
    sys.exit("accept-udp: %s" % msg)


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


def marshal_datagram(*blobs):
    out = b""
    for blob in blobs:
        out += struct.pack(">I", len(blob)) + blob
    return out


def read_datagram(data):
    frames = []
    offset = 0
    while offset < len(data):
        if offset + 4 > len(data):
            fail("datagram 帧长度被截断")
        size, = struct.unpack(">I", data[offset:offset + 4])
        offset += 4
        frames.append(data[offset:offset + size])
        offset += size
    return frames


def main():
    address = sys.argv[1]
    text = sys.argv[2] if len(sys.argv) > 2 else "udp"
    host, _, port = address.rpartition(":")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(10)
    target = (host or "127.0.0.1", int(port))
    request = marshal_datagram(
        marshal_envelope(
            method=METHOD.encode("utf-8"),
            payload=marshal_msg(text),
        ),
        marshal_envelope(flags=FLAG_END),
    )
    sock.sendto(request, target)
    data, _ = sock.recvfrom(65507)
    sock.close()

    messages, status = [], None
    for blob in read_datagram(data):
        method, stream_id, flags, _, payload = unmarshal_envelope(blob)
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
    if status is None or len(status) < 4:
        fail("响应 datagram 缺少状态")
    code, = struct.unpack(">I", status[:4])
    description = status[4:].decode("utf-8")
    if code != CODE_OK or description:
        fail("状态是 (%d, %r)，期望 (0, '')" % (code, description))
    print("accept-udp: %s -> %r, 状态 OK" % (address, got))


main()
PY
