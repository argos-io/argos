// Command argos is the argos toolchain entrypoint.
package main

import (
	"context"
	"log/slog"
	"os"

	argoscmd "github.com/argos-io/argos/internal/cmd"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	if err := argoscmd.App().Run(context.Background(), os.Args); err != nil {
		slog.Error("argos", "err", err)
		os.Exit(1)
	}
}
