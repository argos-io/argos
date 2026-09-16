package frontend

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"

	codegenfrontend "github.com/argos-io/argos/internal/codegen/frontend"
)

// ListCommand lists built-in and plugin frontend conventions.
func ListCommand() *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list built-in frontends",
		Action: func(context.Context, *cli.Command) error {
			fmt.Println("Built-in frontends:")
			for _, name := range codegenfrontend.BuiltinNames() {
				fmt.Printf("  %s\n", name)
			}
			fmt.Println("\nExternal plugins: any command implementing:")
			fmt.Println("  {plugin} emit-ir -- <files...>  → JSON IR on stdout")
			fmt.Println("\nIR fields for stub generation (JSON):")
			fmt.Println("  services          — required; service and method descriptors for *.argos.go")
			fmt.Println("  message_model     — protobuf | none | <custom> (default: infer from payload)")
			fmt.Println("  message_files     — with custom model: [{name, source, symbols}, ...] pre-rendered Go")
			fmt.Println("  file_descriptor*  — with message_model protobuf: proto descriptor bytes")
			fmt.Println("\nUse: argos generate stub --plugin {plugin} -- files...")
			return nil
		},
	}
}
