#!/usr/bin/env bash
# 对跑着 echo.v1.EchoService 的 h2c 地址用 grpcurl 发一次 Echo。
# 不依赖 Go，用来验证 gRPC-over-HTTP/2 组合而不是验证本仓库的实现细节。
#
# 用法：  scripts/accept-grpcurl.sh <host:port> [msg]
# 出口码：0 = 收到 {"msg":"hello <msg>"}
set -euo pipefail

if [[ $# -lt 1 ]]; then
	echo "用法: $0 <host:port> [msg]" >&2
	exit 2
fi

HOSTPORT=$1
MSG=${2:-grpcurl}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PROTO=$ROOT/example/echo/echo.proto
IMPORT_PATH=$ROOT/example/echo

if ! command -v grpcurl >/dev/null 2>&1; then
	echo "accept-grpcurl: grpcurl 未安装" >&2
	exit 127
fi

RAW=$(grpcurl -plaintext -proto "$PROTO" -import-path "$IMPORT_PATH" \
	-d "{\"msg\":\"$MSG\"}" "$HOSTPORT" echo.v1.EchoService/Echo)
OUTPUT=$(python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin), separators=(",", ":")))' <<<"$RAW")
WANT="{\"msg\":\"hello $MSG\"}"
if [[ "$OUTPUT" != "$WANT" ]]; then
	echo "accept-grpcurl: 响应是 $RAW，期望 $WANT" >&2
	exit 1
fi

echo "accept-grpcurl: $HOSTPORT -> $OUTPUT"
