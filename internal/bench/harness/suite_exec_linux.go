//go:build linux

package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// suiteKillGrace is how long a harness child may wrap up after SIGTERM before
// the suite kills its process group.
const suiteKillGrace = 2 * time.Minute

// errSuiteKilled reports a child killed by a second interrupt.
var errSuiteKilled = errors.New("killed by a second interrupt")

// suiteCommand is one child process of the suite.
type suiteCommand struct {
	args []string
	env  map[string]string
	// harness children (tx-bench prep and local) handle SIGTERM through their
	// context and wait for their tx processes, so cancellation asks them to
	// stop before killing them. Other commands may fork compilers or test
	// binaries; cancellation kills their process group at once.
	harness bool
	// workspace children hold the workspace locks while they run, so a run
	// that outlives the suite still keeps the next run from reclaiming its
	// directories.
	workspace bool
}

// suiteEscalation closes its channel on the second SIGINT or SIGTERM. Main
// cancels the suite's context on the first and then restores the default
// action; this registration keeps a second signal from killing the suite
// while its children, in their own process groups, keep running.
func suiteEscalation() (<-chan struct{}, func()) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	escalate, done := make(chan struct{}), make(chan struct{})
	go func() {
		for seen := 0; ; {
			select {
			case <-signals:
				if seen++; seen == 2 {
					close(escalate)
				}
			case <-done:
				return
			}
		}
	}()
	return escalate, func() {
		signal.Stop(signals)
		close(done)
	}
}

// run runs one child with its output in log. It returns ctx's error when ctx
// ends first, after the child and its process group have exited.
func (w *suiteWorkspace) run(ctx context.Context, log *os.File, sc suiteCommand) error {
	if _, err := fmt.Fprintf(log, "\n$ %s\n", suiteShellCommand(sc.args, sc.env)); err != nil {
		return err
	}
	cmd := exec.Command(sc.args[0], sc.args[1:]...)
	cmd.Env = suiteCommandEnv(sc.env)
	cmd.Stdout, cmd.Stderr = log, log
	stop := unix.SIGKILL
	if sc.harness {
		stop = unix.SIGTERM
	}
	// Pdeathsig covers a suite killed outright. It follows the thread that
	// forked the child; the Go runtime does not retire threads that are not
	// locked to a goroutine.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: stop}
	if sc.workspace {
		cmd.ExtraFiles = w.locks
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	killGroup := func() { _ = unix.Kill(-cmd.Process.Pid, unix.SIGKILL) }

	var stopped error
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		stopped = ctx.Err()
	case <-w.escalate:
		stopped = errSuiteKilled
	}
	if !sc.harness || stopped == errSuiteKilled {
		killGroup()
		<-done
		return stopped
	}
	_ = cmd.Process.Signal(unix.SIGTERM)
	grace := time.NewTimer(w.killGrace())
	defer grace.Stop()
	select {
	case <-done:
		// The group may be empty now, and its ID free for reuse: don't
		// signal it after the leader is reaped.
		return stopped
	case <-grace.C:
		fmt.Fprintf(log, "\n%s did not stop within %s of SIGTERM; killing its process group\n", sc.args[0], w.killGrace())
	case <-w.escalate:
		stopped = errSuiteKilled
	}
	killGroup()
	<-done
	return stopped
}

func (w *suiteWorkspace) killGrace() time.Duration {
	if w.grace > 0 {
		return w.grace
	}
	return suiteKillGrace
}

func suiteCommandEnv(overrides map[string]string) []string {
	env := os.Environ()
	env = slices.DeleteFunc(env, func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "GOFLAGS", "GOWORK", "MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "MAKELEVEL", "GNUMAKEFLAGS",
			"TX_AUTH_TOKEN", txBinEnv:
			return true
		}
		return strings.HasPrefix(key, "TX_VERIFY_") || strings.HasPrefix(key, "TX_BENCH_") ||
			strings.HasPrefix(key, "TX_FAULTS_")
	})
	env = append(env, "GOFLAGS=", "GOWORK=off")
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}
