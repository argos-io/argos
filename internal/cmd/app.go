// Package cmd defines the argos CLI command tree.
package cmd

import (
	"github.com/urfave/cli/v3"

	"github.com/argos-io/argos/internal/cmd/frontend"
	"github.com/argos-io/argos/internal/cmd/generate"
)

// App returns the root argos CLI command.
func App() *cli.Command {
	return &cli.Command{
		Name:  "argos",
		Usage: "argos toolchain",
		Commands: []*cli.Command{
			generate.Command(),
			frontend.Command(),
		},
	}
}
