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
	const tuned = 2 * miB
	tempDefault(t).MaxMessageSize = tuned

	client, err := ClientOptions()
	if err != nil {
		t.Fatal(err)
	}
	if client.MaxMessageSize != tuned {
		t.Errorf("ClientOptions MaxMessageSize = %d, want %d", client.MaxMessageSize, tuned)
	}
	server, err := ServerOptions()
	if err != nil {
		t.Fatal(err)
	}
	if server.MaxMessageSize != tuned {
		t.Errorf("ServerOptions MaxMessageSize = %d, want %d", server.MaxMessageSize, tuned)
	}

	// Defaults is the built-in floor, not a view of the tuned default.
	if got := Defaults().MaxMessageSize; got == tuned {
		t.Errorf("Defaults moved with the process default: MaxMessageSize = %d", got)
	}
}

// TestConstructorsCopyTheProcessDefault: the returned Options must not be a
// second handle on the live default, or one Client's tuning would land on
// every later one.
func TestConstructorsCopyTheProcessDefault(t *testing.T) {
	d := tempDefault(t)
	d.MaxMessageSize = 2 * miB

	cfg, err := ClientOptions()
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxMessageSize = 1 * miB
	if d.MaxMessageSize != 2*miB {
		t.Fatalf("writing the returned Options reached the process default: %d", d.MaxMessageSize)
	}
}

// TestWithClientOptionsBeatsProcessDefault: naming a base is how a call site opts out
// of process-wide tuning entirely.
func TestWithClientOptionsBeatsProcessDefault(t *testing.T) {
	tempDefault(t).MaxMessageSize = 2 * miB

	cfg, err := ClientOptions(WithClientOptions(&Options{}))
	if err != nil {
		t.Fatal(err)
	}
	if want := Defaults().MaxMessageSize; cfg.MaxMessageSize != want {
		t.Errorf("MaxMessageSize = %d, want the built-in %d", cfg.MaxMessageSize, want)
	}
}
