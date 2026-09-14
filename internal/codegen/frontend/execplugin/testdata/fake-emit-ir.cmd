@echo off
if /I not "%~1"=="emit-ir" exit /b 2
shift
if "%~1"=="--" shift
echo {
echo   "ir_version": 2,
echo   "source": "plugin",
echo   "go_package": "echov1",
echo   "proto_package": "echo.v1",
echo   "input_base": "echo",
echo   "outputs": {"messages": "echo.msg.go", "stub": "echo.argos.go"},
echo   "messages": [
echo     {"go_name": "EchoRequest", "fields": [{"go_name": "Msg", "number": 1, "kind": "string"}]},
echo     {"go_name": "EchoResponse", "fields": [{"go_name": "Msg", "number": 1, "kind": "string"}]},
echo     {"go_name": "WatchRequest", "fields": [{"go_name": "Msg", "number": 1, "kind": "string"}]},
echo     {"go_name": "Event", "fields": [{"go_name": "Msg", "number": 1, "kind": "string"}]}
echo   ],
echo   "services": [{"go_name": "EchoService", "methods": [
echo     {"go_name": "Echo", "full_name": "echo.v1.EchoService/Echo", "input_type": "EchoRequest", "output_type": "EchoResponse"},
echo     {"go_name": "Watch", "full_name": "echo.v1.EchoService/Watch", "input_type": "WatchRequest", "output_type": "Event", "server_stream": true}
echo   ]}]
echo }
