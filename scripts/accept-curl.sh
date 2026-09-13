#!/usr/bin/env bash
# 对跑着 echo.v1.EchoService 的 http1 地址用 curl 发一次 Echo。
#
# 用法：  scripts/accept-curl.sh <host:port> [msg]
# 出口码：0 = 收到 {"msg":"hello <msg>"}（protojson 字段名）
set -euo pipefail

if [[ $# -lt 1 ]]; then
	echo "用法: $0 <host:port> [msg]" >&2
	exit 2
fi

HOSTPORT=$1
MSG=${2:-curl}

if ! command -v curl >/dev/null 2>&1; then
	echo "accept-curl: curl 未安装" >&2
	exit 127
fi

OUTPUT=$(curl -sS -X POST "http://${HOSTPORT}/echo.v1.EchoService/Echo" \
	-H 'Content-Type: application/json' \
	-d "{\"msg\":\"$MSG\"}")
WANT="{\"msg\":\"hello $MSG\"}"
if [[ "$OUTPUT" != "$WANT" ]]; then
	echo "accept-curl: 响应是 $OUTPUT，期望 $WANT" >&2
	exit 1
fi

echo "accept-curl: $HOSTPORT -> $OUTPUT"
