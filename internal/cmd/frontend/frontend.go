// Package frontend defines argos frontend inspection subcommands.
package frontend

import "github.com/urfave/cli/v3"

// Command is the frontend command group.
func Command() *cli.Command {
	return &cli.Command{
		Name:  "frontend",
		Usage: "inspect IDL frontends",
		Commands: []*cli.Command{
			ListCommand(),
		},
	}
}
