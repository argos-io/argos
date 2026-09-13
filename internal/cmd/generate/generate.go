// Package generate defines argos generate subcommands.
package generate

import "github.com/urfave/cli/v3"

// Command is the generate command group.
func Command() *cli.Command {
	return &cli.Command{
		Name:  "generate",
		Usage: "generate artifacts from IDL",
		Commands: []*cli.Command{
			StubCommand(),
		},
	}
}
