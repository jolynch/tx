//go:build !linux

package harness

import (
	"context"
	"io"
)

func runSuiteCLI(_ context.Context, _ []string, _, stderr io.Writer) int {
	return reportErr(stderr, usageErrorf("suite requires Linux"))
}
