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
			return nil
		},
	}
}
