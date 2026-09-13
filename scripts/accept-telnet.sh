#!/usr/bin/env bash
# 对跑着 echo.v1.EchoService 的 telnet 调试口用 python3 发一次 Echo。
#
# 用法：  scripts/accept-telnet.sh <host:port> [msg]
# 出口码：0 = 收到 {"msg":"hello <msg>"} 且状态 ERR 0
set -euo pipefail

if [[ $# -lt 1 ]]; then
	echo "用法: $0 <host:port> [msg]" >&2
	exit 2
fi

exec python3 - "$@" <<'PY'
import json
import socket
import sys

METHOD = "echo.v1.EchoService/Echo"


def fail(msg):
    sys.exit("accept-telnet: %s" % msg)


def read_line(sock):
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            fail("连接在读到行尾前关闭")
        buf += chunk
    line, rest = buf.split(b"\n", 1)
    if rest:
        fail("单行协议收到多余数据")
    return line.decode("utf-8")


def main():
    address = sys.argv[1]
    text = sys.argv[2] if len(sys.argv) > 2 else "telnet"
    host, _, port = address.rpartition(":")
    sock = socket.create_connection((host or "127.0.0.1", int(port)), timeout=10)
    with sock:
        sock.sendall((METHOD + "\n").encode("utf-8"))
        sock.sendall(('{"msg":"%s"}\n' % text).encode("utf-8"))
        response_line = read_line(sock)
        status_line = read_line(sock)

    try:
        body = json.loads(response_line)
    except json.JSONDecodeError as err:
        fail("响应不是 JSON: %s (%r)" % (err, response_line))
    got, want = body.get("msg"), "hello " + text
    if got != want:
        fail("响应 msg 是 %r，期望 %r" % (got, want))
    if status_line != "ERR 0":
        fail("状态行是 %r，期望 'ERR 0'" % status_line)
    print("accept-telnet: %s -> %r, 状态 OK" % (address, got))


main()
PY
