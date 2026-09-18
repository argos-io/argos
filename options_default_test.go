package argos

import (
	"testing"
)

// The process default is shared mutable state, so nothing in this file calls
// t.Parallel and every test puts back what it found. Go holds paused parallel
// tests until the serial pass is over, which is what keeps the rest of the
// package from ever observing the temporary value — adding t.Parallel here
// would break that and make the whole package flaky.

// tempDefault hands back the live process default with a restore registered.
func tempDefault(t *testing.T) *Options {
	t.Helper()
	old := *DefaultOptions()
	t.Cleanup(func() { *DefaultOptions() = old })
	return DefaultOptions()
}

// TestDefaultOptionsIsLive is the point of handing out a pointer: a program
// tunes the default once at start-up and every later unconfigured constructor
// picks it up.
func TestDefaultOptionsIsLive(t *testing.T) {
	const tuned = 128
	tempDefault(t).MaxConcurrentCalls = tuned

	client, err := ClientOptions()
	if err != nil {
		t.Fatal(err)
	}
	if client.MaxConcurrentCalls != tuned {
		t.Errorf("ClientOptions MaxConcurrentCalls = %d, want %d", client.MaxConcurrentCalls, tuned)
	}
	server, err := ServerOptions()
	if err != nil {
		t.Fatal(err)
	}
	if server.MaxConcurrentCalls != tuned {
		t.Errorf("ServerOptions MaxConcurrentCalls = %d, want %d", server.MaxConcurrentCalls, tuned)
	}

	// Defaults is the built-in floor, not a view of the tuned default.
	if got := Defaults().MaxConcurrentCalls; got == tuned {
		t.Errorf("Defaults moved with the process default: MaxConcurrentCalls = %d", got)
	}
}

// TestConstructorsCopyTheProcessDefault: the returned Options must not be a
// second handle on the live default, or one Client's tuning would land on
// every later one.
func TestConstructorsCopyTheProcessDefault(t *testing.T) {
	d := tempDefault(t)
	d.MaxConcurrentCalls = 128

	cfg, err := ClientOptions()
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConcurrentCalls = 32
	if d.MaxConcurrentCalls != 128 {
		t.Fatalf("writing the returned Options reached the process default: %d", d.MaxConcurrentCalls)
	}
}

// TestWithClientOptionsBeatsProcessDefault: naming a base is how a call site opts out
// of process-wide tuning entirely.
func TestWithClientOptionsBeatsProcessDefault(t *testing.T) {
	tempDefault(t).MaxConcurrentCalls = 128

	cfg, err := ClientOptions(WithClientOptions(&Options{}))
	if err != nil {
		t.Fatal(err)
	}
	if want := Defaults().MaxConcurrentCalls; cfg.MaxConcurrentCalls != want {
		t.Errorf("MaxConcurrentCalls = %d, want the built-in %d", cfg.MaxConcurrentCalls, want)
	}
}
