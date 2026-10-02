// Package harness implements the tx-bench commands: prep, remote send-tree,
// remote recv-copy, local, and report. It forks real tx processes, supervises
// them, and verifies their results; it never speaks FTCP itself. See
// docs/bench/OVERVIEW.md.
package harness

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// Main runs tx-bench with args (excluding the program name).
func Main(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		// The first interrupt starts a clean wrap-up; restoring the default
		// handler lets a second one kill tx-bench outright.
		<-ctx.Done()
		stop()
	}()
	return run(ctx, args, stdout, stderr)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return exitUsage
	}
	switch args[0] {
	case "prep":
		return RunPrepCLI(ctx, args[1:], stdout, stderr)
	case "remote":
		return runRemote(ctx, args[1:], stdout, stderr)
	case "local":
		return RunLocalCLI(ctx, args[1:], stdout, stderr)
	case "report":
		return RunReportCLI(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printUsage(stderr)
		return exitOK
	}
	fmt.Fprintf(stderr, "unknown command: %s\n", args[0])
	printUsage(stderr)
	return exitUsage
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `usage: tx-bench <command> [options]

Commands:
  prep       Generate or import a dataset, check it, and set its page cache
  remote     Benchmark tx between two hosts (send-tree, recv-copy)
  local      Run remote send-tree and recv-copy against each other on this host
  report     Print the benchmark report from recv-copy metrics and traces

Run 'tx-bench <command> --help' for command-specific options.
`)
}

func runRemote(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	usage := func(w io.Writer) {
		fmt.Fprint(w, `usage: tx-bench remote <command> [options]

Benchmark tx between two hosts, forking the matching tx command on each.

Commands:
  send-tree  Prepare a dataset and serve it with tx send tree
  recv-copy  Pull it repeatedly with tx recv copy, verify, and report

Run 'tx-bench remote <command> --help' for command-specific options.
`)
	}
	if len(args) == 0 {
		usage(stderr)
		return exitUsage
	}
	switch args[0] {
	case "send-tree":
		return RunSendTreeCLI(ctx, args[1:], stdout, stderr)
	case "recv-copy":
		return RunRecvCopyCLI(ctx, args[1:], stdout, stderr)
	case "-h", "--help", "help":
		usage(stderr)
		return exitOK
	}
	fmt.Fprintf(stderr, "unknown command: remote %s\n", args[0])
	usage(stderr)
	return exitUsage
}
