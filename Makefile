.PHONY: test test-race lint verify accept test-generate test-integration test-deps build-argos

test:
	go test ./...

test-race:
	go test -race ./...

build-argos:
	go build -o bin/argos ./cmd/argos

# Stub consistency: regenerate and diff against committed example/echo outputs.
test-generate:
	go run ./cmd/argos generate stub --check example/echo/echo.argos.go \
		--from proto --proto-path . example/echo/echo.proto

# go vet 与当前 Go 工具链一致；staticcheck 可选
lint:
	go vet ./...
	@command -v staticcheck >/dev/null && staticcheck ./... || \
		echo "staticcheck 未安装，跳过（go install honnef.co/go/tools/cmd/staticcheck@latest）"

# Architecture accept gates (§3 / §3.1 / §9): root Invariant*|Accept*|Section9*.
accept:
	go test . -run 'Invariant|Accept|Section9' -count=1

# example/echo multi-transport end-to-end (binding + five transports).
test-integration:
	go test ./example/echo/ -count=1 -timeout 180s

# Transitive dependency gate (§3.1-15 / §9-2): envelope(+tcp/udp) and
# wholebody+http1 must not pull gRPC/genproto.
test-deps:
	go test . -run Transitive -count=1

# §13.1 full verify set (Task 6.1): lint + test + test-race + accept +
# test-generate + test-integration + transitive dependency gate.
verify: lint test test-race accept test-generate test-integration test-deps
