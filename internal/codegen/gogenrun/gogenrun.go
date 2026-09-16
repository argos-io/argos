// Package gogenrun compiles and runs generated code from within a test.
//
// Source-text assertions cannot see a generated file that does not compile, nor
// a type bound to the wrong descriptor. The scratch package is created inside
// the repository module so it resolves through the same go.mod/go.sum; a
// leading dot keeps it out of ./... .
package gogenrun

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// DirName returns a scratch directory name unique to the calling test's
// process, so parallel packages do not collide.
func DirName(prefix string) string {
	return fmt.Sprintf(".%s-%d", prefix, os.Getpid())
}

// WriteFiles replaces root/dir with the given files (relative path -> content).
func WriteFiles(root, dir string, files map[string]string) error {
	full := filepath.Join(root, dir)
	if err := os.RemoveAll(full); err != nil {
		return err
	}
	for rel, content := range files {
		path := filepath.Join(full, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// Run executes `go run ./dir` with root as the working directory and returns
// its combined output.
func Run(root, dir string) ([]byte, error) {
	cmd := exec.Command("go", "run", "./"+dir)
	cmd.Dir = root
	return cmd.CombinedOutput()
}

// Remove deletes root/dir. Safe to call when it does not exist.
func Remove(root, dir string) error {
	return os.RemoveAll(filepath.Join(root, dir))
}
