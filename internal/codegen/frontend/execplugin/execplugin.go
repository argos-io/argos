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
	if ctx == nil {
		return nil, fmt.Errorf("exec plugin: nil context")
	}
	if f.Command == "" {
		return nil, fmt.Errorf("exec plugin: empty command")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("exec plugin: no input files")
	}
	args := append([]string{"emit-ir", "--"}, inputs...)
	cmd := exec.CommandContext(ctx, f.Command, args...)
	// A plugin is a subprocess we do not control. Capturing its output without
	// a ceiling let a buggy or hostile one exhaust memory in CI.
	stdout := &cappedBuffer{limit: maxPluginStdout, what: "stdout"}
	stderr := &cappedBuffer{limit: maxPluginStderr, what: "stderr"}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if stdout.overflow != nil {
			return nil, fmt.Errorf("exec plugin %s: %w", f.Command, stdout.overflow)
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("exec plugin %s: %s", f.Command, msg)
	}
	if stdout.overflow != nil {
		return nil, fmt.Errorf("exec plugin %s: %w", f.Command, stdout.overflow)
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
	if err := json.Unmarshal(blob, &many); err == nil {
		if len(many) == 0 {
			return nil, fmt.Errorf("decode IR: no files")
		}
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

const (
	// maxPluginStdout bounds the IR a plugin may emit. Generous next to any
	// real descriptor set, small enough that a runaway plugin fails instead of
	// taking the machine down with it.
	maxPluginStdout = 64 << 20
	// maxPluginStderr bounds the diagnostics kept for the error message.
	maxPluginStderr = 1 << 20
)

// cappedBuffer accumulates output up to limit and then records an error
// instead of growing. Writes keep succeeding so the subprocess is not blocked
// on a broken pipe; the overflow is reported once the command finishes.
type cappedBuffer struct {
	limit    int
	what     string
	buf      bytes.Buffer
	overflow error
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.overflow != nil {
		return len(p), nil
	}
	if room := c.limit - c.buf.Len(); len(p) > room {
		if room > 0 {
			c.buf.Write(p[:room])
		}
		c.overflow = fmt.Errorf("plugin %s exceeded %d bytes", c.what, c.limit)
		return len(p), nil
	}
	return c.buf.Write(p)
}

func (c *cappedBuffer) Bytes() []byte  { return c.buf.Bytes() }
func (c *cappedBuffer) String() string { return c.buf.String() }
