.PHONY: test test-race lint verify accept test-generate test-integration test-deps install

test:
	go test ./...

test-race:
	go test -race ./...

install:
	go install ./...

# Stub consistency: regenerate and diff against committed example/echo outputs.
test-generate:
	go run ./cmd/argos generate stub --check example/echo/echo.argos.go \
		--from proto --proto-path . example/echo/echo.proto

# go vet + gofmt + staticcheck. The previous recipe chained staticcheck with
# "|| echo 未安装，跳过": staticcheck exits non-zero when it has findings, so
# every finding was swallowed and reported as "not installed".
lint:
	go vet ./...
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	@command -v staticcheck >/dev/null 2>&1 || { \
		echo "staticcheck 未安装（go install honnef.co/go/tools/cmd/staticcheck@latest）"; exit 1; }
	staticcheck ./...

# Architecture accept gates (§3 / §3.1 / §9): root Invariant*|Accept*|Section9*.
accept:
	go test . -run 'Invariant|Accept|Section9' -count=1

# example/echo multi-transport end-to-end (grpc + httpunary).
test-integration:
	go test ./example/echo/ -count=1 -timeout 180s

# Transitive dependency gate (§3.1-15 / §9-2): resp+tcp, transport/udp, and
# httpunary+http1 must not pull gRPC/genproto.
test-deps:
	go test . -run Transitive -count=1

# §13.1 full verify set (Task 6.1): lint + test + test-race + accept +
# test-generate + test-integration + transitive dependency gate.
verify: lint test test-race accept test-generate test-integration test-deps
