package argos

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// section9Evidence maps README §9 groups 1–13 to concrete tests that already
// exist elsewhere (Task 6.3 living checklist). Prefer pointing at dedicated
// suites over re-embedding behaviour here.
type section9Evidence struct {
	id      string
	title   string
	pkg     string // module-relative, e.g. "./client"
	test    string // exact TestXxx name
	softGap string // optional note when coverage is intentionally thin
}

var section9Checklist = []section9Evidence{
	// §9-1: milestone-0 probes → formal suites (probe/ deleted in 1.17).
	{id: "§9-1a", title: "OpenStream before response headers", pkg: "./transport/http2", test: "TestOpenStreamReturnsBeforeResponseHeaders"},
	{id: "§9-1b", title: "HTTP/1 OpenStream before response headers", pkg: "./transport/http1", test: "TestOpenStreamReturnsBeforeResponseHeaders"},
	{id: "§9-1c", title: "gRPC interop smoke (h2c unary)", pkg: "./transport/grpc", test: "TestH2CUnaryEcho"},
	{id: "§9-1d", title: "sequential pool one dial per endpoint", pkg: "./internal/sessionpool", test: "TestOpenCallSequentialEmptyPoolOwnDial"},
	{id: "§9-1e", title: "server AcceptCall loop survives handler error", pkg: "./server", test: "TestHandlerErrorDoesNotEndLoop"},
	{id: "§9-1f", title: "Shutdown wakes idle AcceptCall", pkg: "./server", test: "TestShutdownIdleConnExitsQuickly"},
	{id: "§9-1g", title: "OpenFilter short-circuit never dials", pkg: "./client", test: "TestOpenFilterShortCircuitNeverDials"},
	{id: "§9-1h", title: "UDP transport dial serve round-trip", pkg: "./transport/udp", test: "TestDialServeRoundTrip"},

	// §9-2: architecture invariants + transitive neutrality.
	{id: "§9-2a", title: "§3 / §3.1 dependency table", pkg: ".", test: "TestInvariantDependencyTable"},
	{id: "§9-2b", title: "resp+tcp transitive no gRPC", pkg: ".", test: "TestInvariantTransitiveRespTCPNoGRPC"},
	{id: "§9-2c", title: "transport/udp transitive no genproto", pkg: ".", test: "TestInvariantTransitiveTransportUDPNoGenproto"},
	{id: "§9-2d", title: "httpunary+http1 transitive no gRPC", pkg: ".", test: "TestInvariantTransitiveHTTPUnaryHTTP1NoGRPC"},

	// §9-3: Conn/Carrier matrix + shape rejects (thin combo gate).
	{id: "§9-3a", title: "Conn/Carrier interface matrix (5 transports)", pkg: "./transport", test: "TestConnCarrierMatrix"},
	{id: "§9-3b", title: "gRPC AcceptCall reject returns finishable call", pkg: "./transport/grpc", test: "TestAcceptCallRejectReturnsFinishableCall"},
	{id: "§9-3c", title: "httpunary rejects non-unary before handler", pkg: "./transport/httpunary", test: "TestAcceptRejectsNonUnary"},
	{id: "§9-3d", title: "narrow-interface setup error path", pkg: "./client", test: "TestNarrowInterfaceAssertStaysSetupError"},
	{id: "§9-3e", title: "server has no concrete Framing type-switch", pkg: "./server", test: "TestNoConcreteFramingTypeSwitch"},

	// §9-4: example / wire behaviour (non-gRPC framing).
	{id: "§9-4a", title: "resp wire round-trip", pkg: "./example/resp", test: "TestWireRoundTrip"},
	{id: "§9-4b", title: "resp HELLO once per session", pkg: "./example/resp", test: "TestHELLOOncePerSession"},
	{id: "§9-4c", title: "resp SET/GET same connection", pkg: "./example/resp", test: "TestSetGetSameConnection"},
	{id: "§9-4d", title: "gRPC LPM rejects oversize without alloc", pkg: "./transport/grpc", test: "TestReadLPMLimitedRejectsOversizeWithoutAlloc"},

	// §9-5: state machine / lifecycle.
	// A Client has no Close (it owns nothing releasable) and a Server has no
	// Close (it stops when Run's ctx is canceled), so the entries here point at
	// what those lifecycles became: the ctx that ends a call, the goroutines
	// call churn leaves behind, and the axis ownership the composition layer
	// must not take over.
	{id: "§9-5a", title: "client Open refuses a done ctx", pkg: "./client", test: "TestOpenRefusesDoneContext"},
	{id: "§9-5b", title: "client call churn leaves no goroutines", pkg: "./client", test: "TestClosedCallsLeaveNoGoroutines"},
	{id: "§9-5c", title: "server stop leaves the axis usable", pkg: "./server", test: "TestStopLeavesAxisUsable"},
	{id: "§9-5d", title: "httpunary early rejection keeps response readable", pkg: "./transport/httpunary", test: "TestEarlyRejectionKeepsResponseReadable"},
	{id: "§9-5e", title: "resp SendHeaders unimplemented next call works", pkg: "./example/resp", test: "TestSendHeadersUnimplementedNextCallWorks"},
	{id: "§9-5f", title: "client leaves a shared axis to its owner", pkg: "./client", test: "TestClientLeavesAxisToItsOwner"},
	{id: "§9-5g", title: "server stop does not kill an in-flight handler", pkg: "./server", test: "TestAcceptCancelDoesNotKillInFlight"},
	{id: "§9-5j", title: "aborted server start leaves axes open", pkg: "./server", test: "TestStartFailureLeavesAxesOpen"},

	// §9-6: gRPC interop formal gate.
	{id: "§9-6a", title: "interop h2c unary echo", pkg: "./transport/grpc", test: "TestH2CUnaryEcho"},
	{id: "§9-6b", title: "interop TLS/ALPN unary echo", pkg: "./transport/grpc", test: "TestTLSALPNUnaryEcho"},
	{id: "§9-6c", title: "interop metadata binary + trailers", pkg: "./transport/grpc", test: "TestInteropMetadata_BinaryAndTrailers"},
	{id: "§9-6d", title: "interop trailers-only", pkg: "./transport/grpc", test: "TestInteropTrailersOnly"},
	{id: "§9-6e", title: "interop 4-shape × h2c/TLS", pkg: "./transport/grpc", test: "TestInteropOK_Shapes"},
	{id: "§9-6f", title: "interop 17 codes × h2c/TLS unary", pkg: "./transport/grpc", test: "TestInteropStatusCodes_Unary"},
	{id: "§9-6g", title: "interop zero-message client-stream × TLS", pkg: "./transport/grpc", test: "TestInteropZeroMessageClientStream"},
	{id: "§9-6h", title: "interop half-close timing × TLS", pkg: "./transport/grpc", test: "TestInteropHalfCloseTiming"},
	{id: "§9-6i", title: "gRPC MaxMessageSize boundary", pkg: "./transport/grpc", test: "TestMaxMessageSizeBoundary"},
	{id: "§9-6j", title: "gRPC compression bomb MaxMessageSize", pkg: "./transport/grpc", test: "TestCompressionBombMaxMessageSize", softGap: "custom Compressor↔grpc-go adapter and details conflict/corrupt remain framing-level only; not re-duplicated as full interop cells"},

	// §9-7: Filter / OpenFilter / CallMetadata.
	{id: "§9-7a", title: "OpenFilter short-circuit", pkg: "./filter", test: "TestOpenFilterShortCircuit"},
	{id: "§9-7b", title: "OpenFilter wrap order", pkg: "./filter", test: "TestOpenFilterWrapOrder"},
	{id: "§9-7c", title: "OpenFilter must not clear failure", pkg: "./filter", test: "TestOpenFilterMustNotClearFailure"},
	{id: "§9-7d", title: "metadata freeze + SendHeaders", pkg: "./metadata", test: "TestSendHeadersFreezesAndSecondIsAlreadySent"},
	{id: "§9-7e", title: "metadata concurrent race", pkg: "./metadata", test: "TestConcurrentAddAndGettersRace"},

	// §9-8: HTTP/1 commit / SendHeaders unimplemented.
	{id: "§9-8a", title: "Send then Finish commits error not 200", pkg: "./transport/httpunary", test: "TestFinishAfterSendCommitsErrorNot200"},
	{id: "§9-8b", title: "SendHeaders Unimplemented no HTTP 200", pkg: "./transport/httpunary", test: "TestSendHeadersUnimplementedNoCommit"},
	{id: "§9-8c", title: "http1 WriteResponse after buffered send can be error", pkg: "./transport/http1", test: "TestWriteResponseAfterBufferedSendCanBeError"},
	{id: "§9-8d", title: "UDP SendHeaders unsupported (datagram)", pkg: "./metadata", test: "TestSendHeadersReturnsUnimplementedNoFreeze"},

	// §9-9: budget / admission / §6.1 defaults.
	{id: "§9-9a", title: "§6.1 defaults match", pkg: ".", test: "TestDefaultsMatchSection61"},
	{id: "§9-9b", title: "budget product conflict lists fields", pkg: ".", test: "TestBudgetProductConflict"},
	{id: "§9-9c", title: "client admission exhausted", pkg: "./client", test: "TestAdmissionExhausted"},
	{id: "§9-9d", title: "slice alias no double charge", pkg: "./budget", test: "TestSliceAliasNoDoubleCharge"},

	// §9-10: codegen + echo example.
	{id: "§9-10a", title: "echo multi-transport unary", pkg: "./example/echo", test: "TestClientEchoTransports"},
	{id: "§9-10b", title: "echo server-streaming Watch", pkg: "./example/echo", test: "TestWatchStreaming"},

	// §9-11: session / connection lifecycle.
	{id: "§9-11a", title: "sessionpool sequential reuse", pkg: "./internal/sessionpool", test: "TestSequentialReuseKeepsConnUntilPoolClose"},
	{id: "§9-11b", title: "sessionpool concurrent keep-alive until last", pkg: "./internal/sessionpool", test: "TestConcurrentKeepAliveUntilLastRelease"},
	{id: "§9-11c", title: "sessionpool cap exhausted non-blocking", pkg: "./internal/sessionpool", test: "TestCapExhaustedNonBlocking"},
	{id: "§9-11d", title: "not-reusable closed on release", pkg: "./internal/sessionpool", test: "TestNotReusableOnReleaseClosedNotRelent"},
	{id: "§9-11e", title: "inbound conn idle closes Accept", pkg: "./server", test: "TestInboundConnIdleClosesAccept"},

	// §9-12: six-shape assemblability gate.
	{id: "§9-12a", title: "echo shapes (grpc/httpunary)", pkg: "./example/echo", test: "TestClientEchoTransports"},
	{id: "§9-12b", title: "resp×tcp SET/GET same connection", pkg: "./example/resp", test: "TestSetGetSameConnection"},
	{id: "§9-12c", title: "synth×tcp greeting before call", pkg: "./example/synth", test: "TestGreetingReceivedBeforeCall"},
	{id: "§9-12d", title: "composition layer no concrete protocol names", pkg: ".", test: "TestInvariantCompositionNoConcreteProtocolNames"},
	{id: "§9-12e", title: "core packages no example import", pkg: ".", test: "TestInvariantCorePackagesNoExampleImport"},

	// §9-13: repo-level verify gate.
	{id: "§9-13a", title: "Makefile verify = §13.1 full set", pkg: ".", test: "TestSection9VerifyGate"},
	{id: "§9-13b", title: "tcp Addr() after Serve", pkg: "./transport/tcp", test: "TestAddrAfterServe"},
	{id: "§9-13c", title: "udp Addr() after Serve", pkg: "./transport/udp", test: "TestAddrAfterServe"},
}

// TestSection9Checklist is Task 6.3: living §9 map. Each subtest fails clearly
// when a required evidence TestXxx is missing from go test -list.
func TestSection9Checklist(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)

	byGroup := map[string][]section9Evidence{}
	for _, e := range section9Checklist {
		g := section9Group(e.id)
		byGroup[g] = append(byGroup[g], e)
	}
	for g := 1; g <= 13; g++ {
		key := fmt.Sprintf("§9-%d", g)
		items := byGroup[key]
		if len(items) == 0 {
			t.Fatalf("checklist missing any evidence for %s", key)
		}
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			for _, e := range items {
				e := e
				t.Run(e.id+"_"+sanitizeRunName(e.title), func(t *testing.T) {
					t.Parallel()
					if !testExists(t, root, e.pkg, e.test) {
						msg := fmt.Sprintf("%s (%s): missing evidence test %s in package %s",
							e.id, e.title, e.test, e.pkg)
						if e.softGap != "" {
							msg += " [" + e.softGap + "]"
						}
						t.Fatal(msg)
					}
					if e.softGap != "" {
						t.Logf("soft gap noted: %s", e.softGap)
					}
				})
			}
		})
	}
}

// TestSection9VerifyGate asserts Makefile verify equals the §13.1 full set
// (Task 6.1 / §9-13): lint + test + test-race + accept + test-generate +
// test-integration + test-deps.
func TestSection9VerifyGate(t *testing.T) {
	t.Parallel()
	root := moduleRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	mk := string(data)
	requiredTargets := []string{
		"lint",
		"test",
		"test-race",
		"accept",
		"test-generate",
		"test-integration",
		"test-deps",
	}
	// verify: must list all of the above as prerequisites (order may vary).
	verifyLine := ""
	for _, line := range strings.Split(mk, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "verify:") {
			verifyLine = line
			break
		}
	}
	if verifyLine == "" {
		t.Fatal("Makefile missing verify: target (§9-13)")
	}
	for _, want := range requiredTargets {
		if !strings.Contains(verifyLine, want) {
			t.Errorf("verify prerequisites missing %q (line=%q)", want, verifyLine)
		}
		// Each must also be defined as a recipe target.
		if !strings.Contains(mk, "\n"+want+":") && !strings.HasPrefix(mk, want+":") {
			t.Errorf("Makefile missing standalone target %q:", want)
		}
	}
}

func section9Group(id string) string {
	// "§9-12a" → "§9-12"
	id = strings.TrimPrefix(id, "§9-")
	n := 0
	for _, r := range id {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return fmt.Sprintf("§9-%d", n)
}

func sanitizeRunName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ' || r == '-' || r == '/' || r == '+':
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		return "item"
	}
	return out
}

func testExists(t *testing.T, root, pkg, name string) bool {
	t.Helper()
	// Fuzz targets appear in -list as FuzzXxx.
	pattern := "^" + name + "$"
	cmd := exec.Command("go", "test", pkg, "-list", pattern)
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go test %s -list %s: %v\n%s", pkg, pattern, err, stderr.String())
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == name {
			return true
		}
	}
	return false
}
