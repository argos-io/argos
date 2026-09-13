package cmd_test

import (
	"testing"

	"github.com/argos-io/argos/internal/cmd"
)

func TestAppRootCommand(t *testing.T) {
	app := cmd.App()
	if app.Name != "argos" {
		t.Fatalf("Name = %q", app.Name)
	}
	if len(app.Commands) != 2 {
		t.Fatalf("Commands len = %d, want 2", len(app.Commands))
	}
	names := map[string]bool{}
	for _, c := range app.Commands {
		names[c.Name] = true
	}
	for _, want := range []string{"generate", "frontend"} {
		if !names[want] {
			t.Fatalf("missing subcommand %q", want)
		}
	}
}
