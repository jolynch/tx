package harness

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// TxBinary is the tx executable tx-bench forks.
type TxBinary struct {
	Path string `json:"path"`
	Hash string `json:"xxh128"`
}

// Short abbreviates the hash for terminal output.
func (b TxBinary) Short() string {
	h := strings.TrimPrefix(b.Hash, "xxh128:")
	if len(h) < 6 {
		return b.Hash
	}
	return "xxh128:" + h[:4] + "…" + h[len(h)-2:]
}

// txBinEnv names the environment variable that selects the tx binary when
// --tx is not given.
const txBinEnv = "TX_BIN"

// resolveTx finds the tx binary: the --tx flag, else $TX_BIN, else tx next
// to tx-bench, else tx on $PATH.
func resolveTx(flag string) (TxBinary, error) {
	path := flag
	if path == "" {
		path = os.Getenv(txBinEnv)
	}
	if path == "" {
		if self, err := os.Executable(); err == nil {
			if cand := filepath.Join(filepath.Dir(self), "tx"); isExecutable(cand) {
				path = cand
			}
		}
	}
	if path == "" {
		found, err := exec.LookPath("tx")
		if err != nil {
			return TxBinary{}, usageErrorf("no tx binary next to tx-bench or on $PATH; pass --tx or set $TX_BIN")
		}
		path = found
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return TxBinary{}, err
	}
	if !isExecutable(abs) {
		return TxBinary{}, usageErrorf("tx binary %s (from --tx or $TX_BIN) is not an executable file", path)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return TxBinary{}, err
	}
	return TxBinary{Path: abs, Hash: encoding.FormatXXH128HashToken(xxh3.Hash128(data))}, nil
}

func isExecutable(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Mode().IsRegular() && st.Mode().Perm()&0o111 != 0
}

// proc is one forked tx process.
type proc struct {
	cmd *exec.Cmd
	// lifeline is the write end of the child's stdin, never written. When
	// tx-bench exits for any reason the kernel closes it, and a tx started
	// with --exit-with stdin shuts down. Holding it here also keeps the
	// garbage collector from closing it early.
	lifeline io.WriteCloser
	logPath  string
	done     chan struct{}
	state    *os.ProcessState
	waitErr  error
	started  time.Time
}

// startProc forks bin with args, sending stdout and stderr to logPath (or
// discarding them when empty). With lifeline, the child's stdin is a pipe
// held open until the child exits; see proc.lifeline.
func startProc(bin string, args []string, logPath string, lifeline bool) (*proc, error) {
	cmd := exec.Command(bin, args...)
	// Its own process group keeps a terminal Ctrl-C away from the tx:
	// tx-bench stops it deliberately, after recording what it needs.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var out io.Writer = io.Discard
	var logFile *os.File
	if logPath != "" {
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		logFile = f
		out = f
	}
	cmd.Stdout, cmd.Stderr = out, out
	p := &proc{cmd: cmd, logPath: logPath, done: make(chan struct{}), started: time.Now()}
	if lifeline {
		w, err := cmd.StdinPipe()
		if err != nil {
			if logFile != nil {
				logFile.Close()
			}
			return nil, err
		}
		p.lifeline = w
	}
	if err := cmd.Start(); err != nil {
		if logFile != nil {
			logFile.Close()
		}
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	go func() {
		p.waitErr = cmd.Wait()
		p.state = cmd.ProcessState
		if logFile != nil {
			logFile.Close()
		}
		close(p.done)
	}()
	return p, nil
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// stop sends SIGTERM and waits; after grace it kills.
func (p *proc) stop(grace time.Duration) {
	if p.exited() {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(grace):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

func (p *proc) exitCode() int {
	if p.state == nil {
		return -1
	}
	return p.state.ExitCode()
}

// rusage converts the process's wait4 rusage.
func (p *proc) rusage() report.Rusage {
	if p.state == nil {
		return report.Rusage{}
	}
	ru, ok := p.state.SysUsage().(*syscall.Rusage)
	if !ok {
		return report.Rusage{}
	}
	return report.Rusage{
		UserNS:  ru.Utime.Nano(),
		SysNS:   ru.Stime.Nano(),
		MaxRSS:  ru.Maxrss * 1024, // Linux reports KiB
		MinFlt:  ru.Minflt,
		MajFlt:  ru.Majflt,
		InBlock: ru.Inblock,
		OuBlock: ru.Oublock,
		NVCSw:   ru.Nvcsw,
		NIVCSw:  ru.Nivcsw,
		WallNS:  int64(time.Since(p.started)),
	}
}

// logExcerpt returns a whole short log, or the first and last lines of a
// long one: a flag error comes first and a runtime failure last.
func logExcerpt(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) <= 20 {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[:5], "\n") + "\n  ...\n" + strings.Join(lines[len(lines)-10:], "\n")
}

func firstLine(s string) string {
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			return l
		}
	}
	return ""
}

func tailFile(path string, n int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// waitListening polls addr until it accepts a connection, the process exits,
// or ctx ends.
func waitListening(ctx context.Context, addr string, p *proc, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		if p.exited() {
			return fmt.Errorf("tx exited (code %d) before listening on %s", p.exitCode(), addr)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("tx did not listen on %s within %s", addr, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// readRSS returns VmRSS of pid in bytes.
func readRSS(pid int) (int64, error) {
	f, err := os.Open("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "VmRSS:"); ok {
			fields := strings.Fields(rest)
			if len(fields) == 0 {
				break
			}
			kib, err := strconv.ParseInt(fields[0], 10, 64)
			return kib * 1024, err
		}
	}
	return 0, errors.New("VmRSS missing")
}

// lineTailer reads complete lines appended to a file since the last call.
type lineTailer struct {
	path    string
	offset  int64
	partial []byte
}

func (t *lineTailer) next() ([]string, error) {
	f, err := os.Open(t.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(t.offset, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	t.offset += int64(len(data))
	data = append(t.partial, data...)
	var lines []string
	for {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		lines = append(lines, string(data[:i]))
		data = data[i+1:]
	}
	t.partial = append([]byte(nil), data...)
	return lines, nil
}
