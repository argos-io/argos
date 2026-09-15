.PHONY: test test-race lint verify

test:
	go test ./...

test-race:
	go test -race ./...

# go vet 与当前 Go 工具链一致；staticcheck 可选
lint:
	go vet ./...
	@command -v staticcheck >/dev/null && staticcheck ./... || \
		echo "staticcheck 未安装，跳过（go install honnef.co/go/tools/cmd/staticcheck@latest）"

# 里程碑 ⓪：verify = lint + test + test-race。
# accept / test-generate / test-integration 到任务 6.1 再补齐。
verify: lint test test-race
