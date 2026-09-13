// Package irjson loads codegen IR from JSON files (handwritten or plugin output).
package irjson

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/argos-io/argos/internal/codegen/frontend"
	"github.com/argos-io/argos/internal/codegen/ir"
)

// Frontend implements frontend.Frontend for JSON IR files.
type Frontend struct{}

func (Frontend) Name() string { return "ir" }

// Parse reads one or more JSON files. Each file is either one ir.File or []ir.File.
func (Frontend) Parse(_ context.Context, inputs []string) ([]ir.File, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("ir frontend: no input files")
	}
	var out []ir.File
	for _, path := range inputs {
		blob, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("ir frontend: read %s: %w", path, err)
		}
		files, err := decodeFiles(blob)
		if err != nil {
			return nil, fmt.Errorf("ir frontend: decode %s: %w", path, err)
		}
		for i := range files {
			files[i].Normalize(false)
		}
		out = append(out, files...)
	}
	return out, nil
}

func decodeFiles(blob []byte) ([]ir.File, error) {
	var many []ir.File
	if err := json.Unmarshal(blob, &many); err == nil && len(many) > 0 {
		return many, nil
	}
	var one ir.File
	if err := json.Unmarshal(blob, &one); err != nil {
		return nil, err
	}
	if one.GoPackage == "" {
		return nil, fmt.Errorf("invalid IR JSON")
	}
	return []ir.File{one}, nil
}

var _ frontend.Frontend = Frontend{}
