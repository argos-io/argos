// Package frontend converts IDL inputs into codegen IR.
package frontend

import (
	"context"

	"github.com/argos-io/argos/internal/codegen/ir"
)

// Frontend parses inputs into IR files.
type Frontend interface {
	Name() string
	Parse(ctx context.Context, inputs []string) ([]ir.File, error)
}

// BuiltinNames returns built-in frontend identifiers for --from.
func BuiltinNames() []string {
	return []string{"proto", "ir"}
}
