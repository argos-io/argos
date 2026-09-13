#!/usr/bin/env bash
# Test plugin: {cmd} emit-ir -- <files...>  → stdout JSON IR v2
set -euo pipefail
if [[ "${1:-}" != emit-ir ]]; then
	echo "usage: $0 emit-ir -- <files...>" >&2
	exit 2
fi
shift
[[ "${1:-}" == "--" ]] && shift
cat <<'EOF'
{
  "ir_version": 2,
  "source": "plugin",
  "go_package": "echov1",
  "proto_package": "echo.v1",
  "input_base": "echo",
  "outputs": {
    "messages": "echo.msg.go",
    "stub": "echo.argos.go"
  },
  "messages": [
    {
      "go_name": "EchoRequest",
      "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }]
    },
    {
      "go_name": "EchoResponse",
      "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }]
    },
    {
      "go_name": "WatchRequest",
      "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }]
    },
    {
      "go_name": "Event",
      "fields": [{ "go_name": "Msg", "number": 1, "kind": "string" }]
    }
  ],
  "services": [
    {
      "go_name": "EchoService",
      "methods": [
        {
          "go_name": "Echo",
          "full_name": "echo.v1.EchoService/Echo",
          "input_type": "EchoRequest",
          "output_type": "EchoResponse"
        },
        {
          "go_name": "Watch",
          "full_name": "echo.v1.EchoService/Watch",
          "input_type": "WatchRequest",
          "output_type": "Event",
          "server_stream": true
        }
      ]
    }
  ]
}
EOF
