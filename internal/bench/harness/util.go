package harness

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/cliflags"
)

const (
	defaultBenchDir = "./tx-bench-src" // not ./tx-bench, which is the binary
	defaultDstDir   = "./tx-bench-dst"
	defaultPort     = "3453"
)

// Exit codes shared by every command.
const (
	exitOK         = 0
	exitFailedRuns = 1
	exitUsage      = 2
	exitCorruption = 3
)

// usageError is reported with exit code 2.
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usageErrorf(format string, args ...any) error {
	return usageError{msg: fmt.Sprintf(format, args...)}
}

// exitError carries a specific exit code.
type exitError struct {
	code int
	err  error
}

func (e exitError) Error() string { return e.err.Error() }
func (e exitError) Unwrap() error { return e.err }

func reportErr(stderr io.Writer, err error) int {
	fmt.Fprintf(stderr, "tx-bench: %v\n", err)
	var ue usageError
	if errors.As(err, &ue) {
		return exitUsage
	}
	var ee exitError
	if errors.As(err, &ee) {
		return ee.code
	}
	return exitFailedRuns
}

// parseFlags parses args, allowing options after positional arguments
// (tx-bench prep BENCH_DIR --check); positionals are left in cf.Args(). done
// is true when the command should exit.
func parseFlags(cf *cliflags.Flags, args []string) (code int, done bool) {
	var positional []string
	for {
		if err := cf.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return exitOK, true
			}
			return exitUsage, true
		}
		rest := cf.Args()
		if len(rest) == 0 {
			break
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
	// Re-parse only the positionals so cf.Args() returns them.
	_ = cf.FlagSet().Parse(append([]string{"--"}, positional...))
	return 0, false
}

// splitPassThrough separates tx-bench's own arguments from everything after
// "--", which goes to the forked tx command untouched.
func splitPassThrough(args []string) (own, tx []string) {
	for i, a := range args {
		if a == "--" {
			return args[:i], append([]string(nil), args[i+1:]...)
		}
	}
	return args, nil
}

func positionalDir(args []string, def, name string) (string, error) {
	switch len(args) {
	case 0:
		return filepath.Abs(def)
	case 1:
		return filepath.Abs(args[0])
	}
	return "", usageErrorf("at most one positional argument: %s (got %d)", name, len(args))
}

// Formatting shared with the report.
var (
	commas     = report.Commas
	humanBytes = report.HumanBytes
)

func roundDur(d time.Duration) time.Duration {
	switch {
	case d >= 10*time.Second:
		return d.Round(time.Second)
	case d >= time.Second:
		return d.Round(100 * time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(time.Millisecond)
	}
	return d.Round(time.Microsecond)
}

func selfName() string {
	if len(os.Args) == 0 {
		return "tx-bench"
	}
	name := os.Args[0]
	if !strings.Contains(name, "/") {
		return name
	}
	if rel, err := filepath.Rel(mustCwd(), name); err == nil && !strings.HasPrefix(rel, "..") {
		if !strings.Contains(rel, "/") {
			return "./" + rel
		}
		return rel
	}
	return name
}

func mustCwd() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "/"
	}
	return cwd
}

// displayPath shows p relative to the working directory when it is inside it.
func displayPath(p string) string {
	if rel, err := filepath.Rel(mustCwd(), p); err == nil && !strings.HasPrefix(rel, "..") {
		return "./" + rel
	}
	return p
}

func shellQuote(s string) string {
	if s != "" && strings.IndexFunc(s, func(r rune) bool {
		return !(r == '/' || r == '.' || r == '-' || r == '_' || r == ':' || r == '=' || r == '%' || r == '+' || r == '@' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	}) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
