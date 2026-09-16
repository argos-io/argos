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
	"path"
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
	var outputs []generatedOutput
	for _, file := range files {
		fileOutputs, err := generateFileOutputs(opts, inputs, file)
		if err != nil {
			return err
		}
		outputs = append(outputs, fileOutputs...)
	}
	// Check mode verifies this exact slice, so --check can only ever compare
	// the files generation would write for the same inputs and --out.
	if opts.Check != "" {
		return checkFiles(opts.Check, outputs)
	}
	if err := validateOutputPaths(outputs); err != nil {
		return err
	}
	return writeAll(outputs)
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
	return writeAll(outputs)
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

// writeAll replaces every output or none of them.
//
// A run typically emits both a message file and a stub file. Writing them one
// after another left a mixed revision on disk when the second write failed -
// new stub next to stale messages, which breaks the build in a way that is
// hard to trace back. Stage every temporary file first, then rename.
func writeAll(outputs []generatedOutput) error {
	staged := make([]stagedFile, 0, len(outputs))
	defer func() {
		// Anything still staged belongs to a failed run.
		for _, st := range staged {
			os.Remove(st.tmp)
		}
	}()
	for _, output := range outputs {
		st, err := stageFile(output.path, output.source)
		if err != nil {
			return err
		}
		staged = append(staged, st)
	}
	for i, st := range staged {
		if err := commitFile(st); err != nil {
			return err
		}
		staged[i].tmp = "" // committed; nothing to clean up
	}
	return nil
}

// writeFile replaces one output atomically.
func writeFile(path string, source []byte) error {
	return writeAll([]generatedOutput{{path: path, source: source}})
}

// stagedFile is a complete, closed temporary file waiting to replace path.
type stagedFile struct {
	path string
	tmp  string
}

func stageFile(path string, source []byte) (stagedFile, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return stagedFile{}, fmt.Errorf("stub: mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".argos-*"+filepath.Ext(path)+".tmp")
	if err != nil {
		return stagedFile{}, fmt.Errorf("stub: create temporary output for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	n, err := tmp.Write(source)
	if err != nil {
		_ = tmp.Close()
		os.Remove(tmpName)
		return stagedFile{}, fmt.Errorf("stub: write temporary output for %s: %w", path, err)
	}
	if n != len(source) {
		_ = tmp.Close()
		os.Remove(tmpName)
		return stagedFile{}, fmt.Errorf("stub: write temporary output for %s: %w", path, io.ErrShortWrite)
	}
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		os.Remove(tmpName)
		return stagedFile{}, fmt.Errorf("stub: chmod temporary output for %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return stagedFile{}, fmt.Errorf("stub: close temporary output for %s: %w", path, err)
	}
	return stagedFile{path: path, tmp: tmpName}, nil
}

func commitFile(st stagedFile) error {
	path, tmpName := st.path, st.tmp
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
	// IR can come from a different host than the generator. Treat both slash
	// styles as path separators while validating so a Windows traversal cannot
	// become a literal filename on Unix (or vice versa).
	normalized := strings.ReplaceAll(name, `\`, "/")
	if filepath.IsAbs(name) || path.IsAbs(normalized) || hasWindowsVolume(normalized) {
		return "", fmt.Errorf("stub: output path %q must be relative", name)
	}
	base, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("stub: resolve output directory %q: %w", dir, err)
	}
	candidate := filepath.Join(base, filepath.FromSlash(path.Clean(normalized)))
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return "", fmt.Errorf("stub: resolve output path %q: %w", name, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("stub: output path %q escapes output directory %q", name, dir)
	}
	return candidate, nil
}

func hasWindowsVolume(name string) bool {
	return len(name) >= 2 && name[1] == ':'
}

// checkFiles diffs outputs (the files generation would write) against what is
// on disk. The --check path identifies which artifact the caller means, but
// never moves the comparison: taking it verbatim let `--check <some other
// directory>` verify an unrelated copy while the shipped artifact was stale.
func checkFiles(checkPath string, outputs []generatedOutput) error {
	if len(outputs) == 0 {
		return fmt.Errorf("stub: --check has nothing to compare: no inputs were given")
	}
	if err := matchCheckPath(checkPath, outputs); err != nil {
		return err
	}
	for _, output := range outputs {
		if err := checkOne(output.path, output.source); err != nil {
			return err
		}
	}
	return nil
}

// matchCheckPath rejects a --check path that this invocation does not generate.
// A caller could otherwise point it at a stale copy, a handwritten file, or a
// message file generation does not produce, and read the resulting pass as
// "the generated artifact is up to date".
func matchCheckPath(checkPath string, outputs []generatedOutput) error {
	checked, err := filepath.Abs(checkPath)
	if err != nil {
		return fmt.Errorf("stub: resolve --check path %q: %w", checkPath, err)
	}
	generated := make([]string, 0, len(outputs))
	for _, output := range outputs {
		if filepath.Clean(checked) == filepath.Clean(output.path) {
			return nil
		}
		generated = append(generated, output.path)
	}
	return fmt.Errorf("stub: --check %s is not a file this invocation generates; it generates %s", checkPath, strings.Join(generated, ", "))
}

func checkOne(path string, got []byte) error {
	want, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("stub: read %s: %w", path, err)
	}
	if !bytes.Equal(check.Normalize(got), check.Normalize(want)) {
		return fmt.Errorf("stub: %s differs from generated output (handwritten fallback or update IR)", path)
	}
	return nil
}
