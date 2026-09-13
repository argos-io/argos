.PHONY: test test-unit test-race test-integration test-generate build-argos lint accept all verify

# 默认：单元 + 集成（全仓库）
test:
	go test ./...

# 仅内核与小包（不跑 transport / example 网络集成）
test-unit:
	go test ./errs/... ./metadata/... ./filter/... ./stream/... \
		./client/... ./server/... ./codec/... \
		./internal/codegen/... ./internal/wire/... ./internal/statusmap/...

# 传输与 echo 跨包集成（loopback：go test 内 goroutine 起服）
test-integration:
	go test ./example/echo/... ./transport/...

test-race:
	go test -race ./...

build-argos:
	go build -o bin/argos ./cmd/argos

test-generate:
	go run ./cmd/argos generate stub --check example/echo/echo.argos.go \
		--from proto --proto-path . example/echo/echo.proto

# go vet 与当前 Go 工具链一致；golangci-lint 需支持本仓库 go.mod 中的 Go 版本
lint:
	go vet ./...
	@command -v staticcheck >/dev/null && staticcheck ./... || \
		echo "staticcheck 未安装，跳过（go install honnef.co/go/tools/cmd/staticcheck@latest）"

accept:
	go test -run '^TestAccept$$' -count=1 .

all: lint test test-race test-generate accept

# 提交前完整验证（lint + 全量测试 + 协议/传输集成）
verify: all test-integration
