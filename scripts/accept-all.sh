#!/usr/bin/env bash
# 仓库级自动化验收（静态检查 + go test + 可选外部工具）。
#
# 用法：  scripts/accept-all.sh
# 环境：  ACCEPT_EXTERNAL=1 时额外跑 grpcurl/curl/envelope 外部脚本（需先起六端口服务）
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

fail() {
	echo "accept-all: $*" >&2
	exit 1
}

echo "== #4 impl 零传输痕迹 =="
if grep -E 'http2|http1|websocket|telnet|grpc|transport/' example/echo/impl.go; then
	fail "impl.go 含传输或协议名"
fi
echo "ok"

echo "== #6 派发只在 binding.invoke =="
mapfile -t dispatch_files < <(
	find . -name '*.go' ! -path './.git/*' \
		-exec grep -lE '\bdispatch[[:space:]]*\(' {} + 2>/dev/null || true
)
if [[ ${#dispatch_files[@]} -ne 1 || ${dispatch_files[0]} != ./server/binding.go ]]; then
	printf 'accept-all: dispatch( 出现在: %s\n' "${dispatch_files[*]:-<none>}"
	fail "want only server/binding.go"
fi
if ! grep -qE 'func \(b \*binding\) invoke' server/binding.go; then
	fail "binding.invoke missing"
fi
echo "ok"

echo "== lint (go vet) =="
go vet ./...
echo "ok"

echo "== go test ./... =="
go test ./...
echo "ok"

echo "== argos generate stub --check =="
go run ./cmd/argos generate stub --check example/echo/echo.argos.go \
	--from proto --proto-path . example/echo/echo.proto
echo "ok"

if [[ "${ACCEPT_EXTERNAL:-}" == 1 ]]; then
	echo "== 外部客户端（需 example/echo/main 已在默认端口监听）=="
	scripts/accept-grpcurl.sh 127.0.0.1:9090 accept
	scripts/accept-curl.sh 127.0.0.1:8080 accept
	scripts/accept-envelope.sh 127.0.0.1:7000 accept
	scripts/accept-ws.sh 127.0.0.1:8081 accept
	scripts/accept-udp.sh 127.0.0.1:7001 accept
	scripts/accept-telnet.sh 127.0.0.1:2323 accept
	scripts/accept-grpcurl-watch.sh 127.0.0.1:9090 accept
	scripts/accept-envelope-watch.sh 127.0.0.1:7000 accept
	scripts/accept-ws-watch.sh 127.0.0.1:8081 accept
	echo "ok"
fi

echo "accept-all: 全部通过"
