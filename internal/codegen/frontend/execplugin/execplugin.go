// Package execplugin runs external IDL frontends that emit JSON IR on stdout.
package execplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/argos-io/argos/internal/codegen/frontend"
	"github.com/argos-io/argos/internal/codegen/ir"
)

// Frontend invokes: {Command} emit-ir -- {inputs...}
type Frontend struct {
	Command string
}

func (f Frontend) Name() string {
	return "plugin:" + f.Command
}

// Parse runs the plugin and decodes stdout as IR JSON ([]ir.File or ir.File).
func (f Frontend) Parse(ctx context.Context, inputs []string) ([]ir.File, error) {
	if f.Command == "" {
		return nil, fmt.Errorf("exec plugin: empty command")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("exec plugin: no input files")
	}
	args := append([]string{"emit-ir", "--"}, inputs...)
	cmd := exec.CommandContext(ctx, f.Command, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("exec plugin %s: %s", f.Command, msg)
	}
	files, err := decodeIR(stdout.Bytes())
	if err != nil {
		return nil, err
	}
	for i := range files {
		files[i].Normalize(true)
		if err := files[i].Validate(true); err != nil {
			return nil, err
		}
	}
	return files, nil
}

func decodeIR(blob []byte) ([]ir.File, error) {
	var many []ir.File
	if err := json.Unmarshal(blob, &many); err == nil && len(many) > 0 {
		return many, nil
	}
	var one ir.File
	if err := json.Unmarshal(blob, &one); err != nil {
		return nil, fmt.Errorf("decode IR: %w", err)
	}
	if one.GoPackage == "" {
		return nil, fmt.Errorf("decode IR: missing go_package")
	}
	return []ir.File{one}, nil
}

var _ frontend.Frontend = Frontend{}
