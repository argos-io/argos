// Package stub generates or checks *.argos.go and message files from IR.
package stub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
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
}

type generatedOutput struct {
	path   string
	source []byte
}

// Run parses inputs and generates or checks output files.
func Run(ctx context.Context, opts Options, inputs []string) error {
	if ctx == nil {
		return fmt.Errorf("stub: nil context")
	}
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
	var outputs []generatedOutput
	for _, file := range files {
		fileOutputs, err := generateFileOutputs(opts, inputs, file)
		if err != nil {
			return err
		}
		outputs = append(outputs, fileOutputs...)
	}
	if err := validateOutputPaths(outputs); err != nil {
		return err
	}
	for _, output := range outputs {
		if err := writeFile(output.path, output.source); err != nil {
			return err
		}
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
	outputs, err := generateFileOutputs(opts, inputs, file)
	if err != nil {
		return err
	}
	if err := validateOutputPaths(outputs); err != nil {
		return err
	}
	for _, output := range outputs {
		if err := writeFile(output.path, output.source); err != nil {
			return err
		}
	}
	return nil
}

func generateFileOutputs(opts Options, inputs []string, file ir.File) ([]generatedOutput, error) {
	var outputs []generatedOutput
	if file.GenerateMessages() {
		msgSrc, err := message.Generate(file)
		if err != nil {
			return nil, err
		}
		msgPath, err := outputPath(opts, inputs, file, file.MessagesName())
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, generatedOutput{path: msgPath, source: msgSrc})
	}
	stubSrc, err := stubgen.Generate(file)
	if err != nil {
		return nil, err
	}
	stubPath, err := outputPath(opts, inputs, file, file.StubName())
	if err != nil {
		return nil, err
	}
	outputs = append(outputs, generatedOutput{path: stubPath, source: stubSrc})
	return outputs, nil
}

func validateOutputPaths(outputs []generatedOutput) error {
	seen := make(map[string]string, len(outputs))
	for _, output := range outputs {
		key := filepath.Clean(output.path)
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if previous, exists := seen[key]; exists {
			return fmt.Errorf("stub: output path %q is generated more than once (same as %q)", output.path, previous)
		}
		seen[key] = output.path
	}
	return nil
}

func writeFile(path string, source []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("stub: mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".argos-*"+filepath.Ext(path)+".tmp")
	if err != nil {
		return fmt.Errorf("stub: create temporary output for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	n, err := tmp.Write(source)
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("stub: write temporary output for %s: %w", path, err)
	}
	if n != len(source) {
		_ = tmp.Close()
		return fmt.Errorf("stub: write temporary output for %s: %w", path, io.ErrShortWrite)
	}
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("stub: chmod temporary output for %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("stub: close temporary output for %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		// Windows does not replace an existing destination with Rename. The
		// temporary file is complete and closed at this point, so only the
		// final replacement remains. POSIX platforms keep the atomic rename.
		if !errors.Is(err, fs.ErrExist) && !os.IsExist(err) {
			return fmt.Errorf("stub: rename temporary output to %s: %w", path, err)
		}
		if removeErr := os.Remove(path); removeErr != nil {
			return fmt.Errorf("stub: replace %s: %w (remove existing: %v)", path, err, removeErr)
		}
		if renameErr := os.Rename(tmpName, path); renameErr != nil {
			return fmt.Errorf("stub: rename temporary output to %s: %w", path, renameErr)
		}
	}
	return nil
}

func outputPath(opts Options, inputs []string, file ir.File, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("stub: missing output name")
	}
	if opts.OutDir != "" {
		return safeOutputPath(opts.OutDir, name)
	}
	for _, input := range inputs {
		base := strings.TrimSuffix(filepath.Base(input), filepath.Ext(input))
		if strings.HasPrefix(name, base+".") {
			return safeOutputPath(filepath.Dir(input), name)
		}
	}
	if len(inputs) == 1 {
		return safeOutputPath(filepath.Dir(inputs[0]), name)
	}
	return safeOutputPath(".", name)
}

func safeOutputPath(dir, name string) (string, error) {
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("stub: output path %q must be relative", name)
	}
	base, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("stub: resolve output directory %q: %w", dir, err)
	}
	candidate := filepath.Join(base, filepath.Clean(name))
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return "", fmt.Errorf("stub: resolve output path %q: %w", name, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("stub: output path %q escapes output directory %q", name, dir)
	}
	return candidate, nil
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
