// Command tx-bench benchmarks tx between two machines. See docs/bench.
package main

import (
	"os"

	"github.com/jolynch/tx/internal/bench/harness"
)

func main() {
	os.Exit(harness.Main(os.Args[1:], os.Stdout, os.Stderr))
}
