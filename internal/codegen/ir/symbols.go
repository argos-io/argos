package ir

import "fmt"

// CheckStubMessageCollision reports the first package-level name declared in both
// stub and message generator output.
func CheckStubMessageCollision(stubSymbols, messageSymbols []string) error {
	seen := make(map[string]string, len(stubSymbols))
	for _, name := range stubSymbols {
		seen[name] = "stub"
	}
	for _, name := range messageSymbols {
		if previous, ok := seen[name]; ok {
			return fmt.Errorf("codegen: generated message %q conflicts with %s", name, previous)
		}
	}
	return nil
}
