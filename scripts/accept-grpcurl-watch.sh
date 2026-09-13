#!/usr/bin/env bash
# 对跑着 echo.v1.EchoService 的 h2c 地址用 grpcurl 发 server-streaming Watch。
#
# 用法：  scripts/accept-grpcurl-watch.sh <host:port> [msg]
# 出口码：0 = 收到三条 Event（<msg> one/two/three）
set -euo pipefail

if [[ $# -lt 1 ]]; then
	echo "用法: $0 <host:port> [msg]" >&2
	exit 2
fi

HOSTPORT=$1
MSG=${2:-watch}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PROTO=$ROOT/example/echo/echo.proto
IMPORT_PATH=$ROOT/example/echo

if ! command -v grpcurl >/dev/null 2>&1; then
	echo "accept-grpcurl-watch: grpcurl 未安装" >&2
	exit 127
fi

if ! RAW=$(grpcurl -plaintext -max-time 30 -proto "$PROTO" -import-path "$IMPORT_PATH" \
	-d "{\"msg\":\"$MSG\"}" "$HOSTPORT" echo.v1.EchoService/Watch 2>&1); then
	echo "accept-grpcurl-watch: grpcurl failed: $RAW" >&2
	exit 1
fi
if [[ -z "$RAW" ]]; then
	echo "accept-grpcurl-watch: grpcurl returned empty response" >&2
	exit 1
fi

ACCEPT_GRPCURL_WATCH_MSG=$MSG ACCEPT_GRPCURL_WATCH_RAW=$RAW python3 - <<'PY'
import json
import os
import sys

msg = os.environ["ACCEPT_GRPCURL_WATCH_MSG"]
raw = os.environ["ACCEPT_GRPCURL_WATCH_RAW"].strip()

events = []
decoder = json.JSONDecoder()
idx = 0
while idx < len(raw):
    chunk = raw[idx:].lstrip()
    if not chunk:
        break
    try:
        obj, end = decoder.raw_decode(chunk)
    except json.JSONDecodeError as err:
        sys.exit("accept-grpcurl-watch: decode JSON: %s\nraw:\n%s" % (err, raw))
    events.append(obj)
    idx += len(raw[idx:]) - len(chunk) + end

want = ["%s one" % msg, "%s two" % msg, "%s three" % msg]
got = [e.get("msg", "") for e in events]
if got != want:
    sys.exit("accept-grpcurl-watch: events = %r, want %r\nraw:\n%s" % (got, want, raw))
print("accept-grpcurl-watch: %d events OK" % len(events))
PY

echo "accept-grpcurl-watch: $HOSTPORT OK"
