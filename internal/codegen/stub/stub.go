// Package stub generates or checks *.argos.go and message files from IR.
package stub

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/argos-io/argos/internal/codegen/check"
	"github.com/argos-io/argos/internal/codegen/frontend"
	"github.com/argos-io/argos/internal/codegen/frontend/execplugin"
	"github.com/argos-io/argos/internal/codegen/frontend/irjson"
	"github.com/argos-io/argos/internal/codegen/frontend/proto"
	"github.com/argos-io/argos/internal/codegen/gen/message"
	"github.com/argos-io/argos/internal/codegen/gen/stubgen"
	"github.com/argos-io/argos/internal/codegen/ir"
)

// Options configures stub generation.
type Options struct {
	From        string   // proto | ir
	Plugin      string   // external emit-ir command
	ImportPaths []string // proto import paths
	OutDir      string   // default: directory of each input
	Check       string   // existing file to diff against (handwritten fallback)
	Write       bool     // write files (default true unless Check set)
}

// Run parses inputs and generates or checks output files.
func Run(ctx context.Context, opts Options, inputs []string) error {
	if len(inputs) == 0 && opts.Check == "" {
		return fmt.Errorf("stub: no input files")
	}
	fromPlugin := opts.Plugin != ""
	fe, err := resolveFrontend(opts)
	if err != nil {
		return err
	}
	var files []ir.File
	if len(inputs) > 0 {
		files, err = fe.Parse(ctx, inputs)
		if err != nil {
			return err
		}
		for i := range files {
			files[i].Normalize(fromPlugin)
			if err := files[i].Validate(fromPlugin); err != nil {
				return err
			}
		}
	}
	if opts.Check != "" {
		return checkFiles(opts.Check, files)
	}
	for i, file := range files {
		if err := writeFileOutputs(opts, inputs, file); err != nil {
			return err
		}
		_ = i
	}
	return nil
}

func resolveFrontend(opts Options) (frontend.Frontend, error) {
	switch {
	case opts.Plugin != "":
		return execplugin.Frontend{Command: opts.Plugin}, nil
	case opts.From == "" || opts.From == "proto":
		return proto.Frontend{ImportPaths: opts.ImportPaths}, nil
	case opts.From == "ir":
		return irjson.Frontend{}, nil
	default:
		return nil, fmt.Errorf("stub: unknown frontend %q", opts.From)
	}
}

func writeFileOutputs(opts Options, inputs []string, file ir.File) error {
	if file.GenerateMessages() {
		msgSrc, err := message.Generate(file)
		if err != nil {
			return err
		}
		msgPath, err := outputPath(opts, inputs, file, file.MessagesName())
		if err != nil {
			return err
		}
		if err := writeFile(msgPath, msgSrc); err != nil {
			return err
		}
	}
	stubSrc, err := stubgen.Generate(file)
	if err != nil {
		return err
	}
	stubPath, err := outputPath(opts, inputs, file, file.StubName())
	if err != nil {
		return err
	}
	return writeFile(stubPath, stubSrc)
}

func writeFile(path string, source []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("stub: mkdir %s: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, source, 0o644); err != nil {
		return fmt.Errorf("stub: write %s: %w", path, err)
	}
	return nil
}

func outputPath(opts Options, inputs []string, file ir.File, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("stub: missing output name")
	}
	if opts.OutDir != "" {
		return filepath.Join(opts.OutDir, name), nil
	}
	for _, input := range inputs {
		base := strings.TrimSuffix(filepath.Base(input), filepath.Ext(input))
		if strings.HasPrefix(name, base+".") {
			return filepath.Join(filepath.Dir(input), name), nil
		}
	}
	if len(inputs) == 1 {
		return filepath.Join(filepath.Dir(inputs[0]), name), nil
	}
	return name, nil
}

func checkFiles(path string, files []ir.File) error {
	if len(files) != 1 {
		return fmt.Errorf("stub: --check expects exactly one IR file, got %d", len(files))
	}
	file := files[0]
	stubPath, msgPath := companionPaths(path, file)
	if err := checkOne(stubPath, func() ([]byte, error) { return stubgen.Generate(file) }); err != nil {
		return err
	}
	if file.GenerateMessages() {
		if err := checkOne(msgPath, func() ([]byte, error) { return message.Generate(file) }); err != nil {
			return err
		}
	}
	return nil
}

func companionPaths(path string, file ir.File) (stubPath, msgPath string) {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	base = strings.TrimSuffix(base, ".argos")
	base = strings.TrimSuffix(base, ".pb")
	base = strings.TrimSuffix(base, ".msg")
	dir := filepath.Dir(path)
	if strings.HasSuffix(path, ".argos.go") || file.StubName() == filepath.Base(path) {
		stubPath = path
		msgPath = filepath.Join(dir, file.MessagesName())
		if file.MessagesName() == "" {
			msgPath = filepath.Join(dir, base+".pb.go")
		}
		return stubPath, msgPath
	}
	msgPath = path
	stubPath = filepath.Join(dir, file.StubName())
	if file.StubName() == "" {
		stubPath = filepath.Join(dir, base+".argos.go")
	}
	return stubPath, msgPath
}

func checkOne(path string, generate func() ([]byte, error)) error {
	got, err := generate()
	if err != nil {
		return err
	}
	want, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("stub: read %s: %w", path, err)
	}
	if !bytes.Equal(check.Normalize(got), check.Normalize(want)) {
		return fmt.Errorf("stub: %s differs from generated output (handwritten fallback or update IR)", path)
	}
	return nil
}
