package generate

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"

	"github.com/argos-io/argos/internal/codegen/stub"
)

// StubCommand generates or checks *.argos.go service stubs.
func StubCommand() *cli.Command {
	return &cli.Command{
		Name:      "stub",
		Usage:     "generate *.argos.go service stubs",
		ArgsUsage: "[files...]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "from",
				Value: "proto",
				Usage: "built-in frontend: proto | ir",
			},
			&cli.StringFlag{
				Name:  "plugin",
				Usage: "external frontend command (runs: {plugin} emit-ir -- files...)",
			},
			&cli.StringSliceFlag{
				Name:  "proto-path",
				Usage: "import path for --from proto (repeatable)",
				Value: []string{"."},
			},
			&cli.StringFlag{
				Name:  "out",
				Usage: "output directory (default: next to input, source_relative)",
			},
			&cli.StringFlag{
				Name:  "check",
				Usage: "verify mode: path of one file generation would write; every generated file is diffed on disk",
			},
		},
		Action: stubAction,
	}
}

func stubAction(ctx context.Context, cmd *cli.Command) error {
	if cmd.String("plugin") != "" && cmd.IsSet("from") {
		return fmt.Errorf("use either --plugin or --from, not both")
	}
	opts := stub.Options{
		From:        cmd.String("from"),
		Plugin:      cmd.String("plugin"),
		ImportPaths: cmd.StringSlice("proto-path"),
		OutDir:      cmd.String("out"),
		Check:       cmd.String("check"),
	}
	return stub.Run(ctx, opts, cmd.Args().Slice())
}
