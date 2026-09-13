// Package gen renders ir.File into Go source (stub re-export).
package gen

import (
	"github.com/argos-io/argos/internal/codegen/gen/stubgen"
	"github.com/argos-io/argos/internal/codegen/ir"
)

// Generate returns Go source for one IR file's *.argos.go.
func Generate(file ir.File) ([]byte, error) {
	return stubgen.Generate(file)
}
