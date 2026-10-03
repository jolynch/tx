package harness

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// The acceptance suite runs the real tx-bench binaries against a dataset of
// TX_BENCH_ACCEPTANCE_SIZE (make bench-acceptance). Without that variable it
// is skipped, so go test ./... stays fast.
const (
	envSize    = "TX_BENCH_ACCEPTANCE_SIZE"
	envOut     = "TX_BENCH_ACCEPTANCE_OUT"
	envDir     = "TX_BENCH_ACCEPTANCE_DIR"
	envMaxRSS  = "TX_BENCH_ACCEPTANCE_MAX_RSS"
	envMaxDial = "TX_BENCH_ACCEPTANCE_MAX_DIALS"
)

var (
	txBenchOnce sync.Once
	txBenchBin  string
	txBenchErr  error
)

// buildTxBench builds tx-bench next to the test tx binary, so it also finds
// that tx by default.
func buildTxBench(t *testing.T) string {
	t.Helper()
	txBenchOnce.Do(func() {
		root, err := exec.Command("go", "env", "GOMOD").Output()
		if err != nil {
			txBenchErr = err
			return
		}
		txBenchBin = filepath.Join(filepath.Dir(txBin), "tx-bench")
		build := exec.Command("go", "build", "-o", txBenchBin, "./cmd/tx-bench")
		build.Dir = filepath.Dir(strings.TrimSpace(string(root)))
		if out, err := build.CombinedOutput(); err != nil {
			txBenchErr = fmt.Errorf("build tx-bench: %v\n%s", err, out)
		}
	})
	if txBenchErr != nil {
		t.Fatal(txBenchErr)
	}
	return txBenchBin
}

// syncBuffer is a bytes.Buffer safe for a child's output and the test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// benchProc is one forked tx-bench command.
type benchProc struct {
	name string
	cmd  *exec.Cmd
	out  *syncBuffer
	done chan struct{}
	code int
}

func startBench(t *testing.T, name string, args ...string) *benchProc {
	t.Helper()
	p := &benchProc{name: name, out: &syncBuffer{}, done: make(chan struct{})}
	p.cmd = exec.Command(buildTxBench(t), args...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	go func() {
		_ = p.cmd.Wait()
		p.code = p.cmd.ProcessState.ExitCode()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	})
	return p
}

// wait returns the exit code, failing the test after d.
func (p *benchProc) wait(t *testing.T, d time.Duration) int {
	t.Helper()
	select {
	case <-p.done:
		return p.code
	case <-time.After(d):
		t.Fatalf("%s still running after %s:\n%s", p.name, d, p.out.String())
		return -1
	}
}

var txPIDLine = regexp.MustCompile(`pid (\d+)  serving tx://`)

// startSender starts remote send-tree on a free loopback port and waits
// until it serves. It returns the process, its address, and the pid of the
// tx send tree it forked.
func startSender(t *testing.T, bench string, extra ...string) (*benchProc, string, int) {
	t.Helper()
	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	args := append([]string{"remote", "send-tree", "--tx", txBin, "-l", addr}, extra...)
	p := startBench(t, "send-tree", append(args, bench)...)
	deadline := time.Now().Add(30 * time.Minute) // generation can take a while
	for {
		if m := txPIDLine.FindStringSubmatch(p.out.String()); m != nil {
			pid, _ := strconv.Atoi(m[1])
			return p, addr, pid
		}
		select {
		case <-p.done:
			t.Fatalf("send-tree exited (%d) before serving:\n%s", p.code, p.out.String())
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("send-tree not serving after 30m:\n%s", p.out.String())
		}
	}
}

func recvArgs(addr, dst string, flags ...string) []string {
	args := append([]string{"remote", "recv-copy", "--tx", txBin}, flags...)
	return append(args, addr, dst, "--", "--progress=false")
}

func envBytes(t *testing.T, name string, def int64) int64 {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	n, err := encoding.ParseByteSize(raw)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, raw, err)
	}
	return n
}

func pidAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

func portOpen(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func waitGone(t *testing.T, what string, pid int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for pidAlive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("%s (pid %d) still running after %s", what, pid, d)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func acceptanceDirs(t *testing.T) (work, out string) {
	t.Helper()
	work = os.Getenv(envDir)
	if work == "" {
		work = t.TempDir()
	} else {
		var err error
		if work, err = os.MkdirTemp(work, "tx-bench-acceptance-"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(work) })
	}
	out = os.Getenv(envOut)
	if out == "" {
		out = t.TempDir()
	}
	out, err := filepath.Abs(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	return work, out
}

// TestBenchAcceptance runs send-tree and recv-copy as separate processes and
// checks correctness, liveness and shutdown, and resource budgets.
func TestBenchAcceptance(t *testing.T) {
	sizeRaw := os.Getenv(envSize)
	if sizeRaw == "" {
		t.Skip(envSize + " is not set (make bench-acceptance)")
	}
	size, err := encoding.ParseByteSize(sizeRaw)
	if err != nil || size <= 0 {
		t.Fatalf("%s=%q: %v", envSize, sizeRaw, err)
	}
	maxRSS := envBytes(t, envMaxRSS, 4<<30)
	work, out := acceptanceDirs(t)

	// The dataset and one receiver copy live here at once.
	fs, err := dataset.StatFS(work)
	if err != nil {
		t.Fatal(err)
	}
	if need := size * 5 / 2; fs.Available < need {
		t.Fatalf("%s has %s free; a %s run needs about %s (dataset, one copy, and headroom)",
			work, report.HumanBytes(fs.Available), report.HumanBytes(size), report.HumanBytes(need))
	}
	bench, dst := filepath.Join(work, "src"), filepath.Join(work, "dst")

	t.Run("correctness", func(t *testing.T) {
		send, addr, txPID := startSender(t, bench, "-s", sizeRaw, "--profile", "mixed", "-c", "0%")
		b, err := dataset.ReadBenchJSON(bench)
		if err != nil {
			t.Fatal(err)
		}
		metrics := filepath.Join(out, "metrics.jsonl")
		removeStale(t, metrics)
		recv := startBench(t, "recv-copy", recvArgs(addr, dst, "-w", "1", "-n", "3", "-e", b.Fingerprint, "-o", metrics, "--stop-sender")...)
		if code := recv.wait(t, 30*time.Minute); code != 0 {
			t.Fatalf("recv-copy exit %d:\n%s", code, recv.out.String())
		}
		if code := send.wait(t, 30*time.Second); code != 0 || tableRow(send.out.String(), "stop") == nil {
			t.Fatalf("send-tree after --stop-sender: exit %d:\n%s", code, send.out.String())
		}
		waitGone(t, "tx send tree", txPID, 10*time.Second)

		m, err := report.ReadMetrics(metrics)
		if err != nil {
			t.Fatal(err)
		}
		if len(m.Runs) != 4 || m.Summary.Measured != 3 || m.Summary.Failed != 0 {
			t.Fatalf("runs=%d summary=%+v", len(m.Runs), m.Summary)
		}
		for _, r := range m.Runs {
			if r.Status != report.StatusOK || r.Verify.Mode != "full" || r.Verify.Mismatches != 0 ||
				r.Verify.Files != b.Files || r.Files != int64(b.Files) || r.Bytes != b.Bytes {
				t.Errorf("run %s: status=%s verify=%+v files=%d bytes=%d; dataset has %d files, %d bytes",
					r.Label, r.Status, r.Verify, r.Files, r.Bytes, b.Files, b.Bytes)
			}
			if r.Sender == nil {
				t.Errorf("run %s has no sender metrics", r.Label)
				continue
			}
			if r.Client.MaxRSS > maxRSS || r.Sender.Rusage.MaxRSS > maxRSS {
				t.Errorf("run %s RSS client %s sender %s exceeds %s", r.Label,
					report.HumanBytes(r.Client.MaxRSS), report.HumanBytes(r.Sender.Rusage.MaxRSS), report.HumanBytes(maxRSS))
			}
			t.Logf("run %-2s wall %s  dials %d  client rss %s  sender rss %s", r.Label,
				time.Duration(r.WallNS).Round(time.Millisecond), r.Dials,
				report.HumanBytes(r.Client.MaxRSS), report.HumanBytes(r.Sender.Rusage.MaxRSS))
		}
		if _, err := os.Stat(filepath.Join(dst, "data")); !os.IsNotExist(err) {
			t.Errorf("DST/data left after the last run: %v", err)
		}
	})
	if t.Failed() {
		return
	}

	t.Run("corruption", func(t *testing.T) {
		d, err := dataset.Load(bench)
		if err != nil {
			t.Fatal(err)
		}
		var victim dataset.Entry
		for _, e := range d.Entries {
			if e.Type == dataset.TypeFile && e.Size > 0 {
				victim = e
				break
			}
		}
		path := filepath.Join(d.DataRoot(), victim.Path)
		flip := func() {
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			var b [1]byte
			if _, err := f.ReadAt(b[:], 0); err != nil {
				t.Fatal(err)
			}
			b[0] ^= 1
			if _, err := f.WriteAt(b[:], 0); err != nil {
				t.Fatal(err)
			}
			mt := time.Unix(0, victim.MtimeNS)
			if err := os.Chtimes(path, mt, mt); err != nil {
				t.Fatal(err)
			}
		}
		flip()
		defer flip() // restore for the later phases

		send, addr, _ := startSender(t, bench)
		recv := startBench(t, "recv-copy", recvArgs(addr, dst, "-w", "0", "-n", "1", "-o", "", "--stop-sender")...)
		if code := recv.wait(t, 30*time.Minute); code != exitCorruption {
			t.Fatalf("recv-copy exit %d, want %d:\n%s", code, exitCorruption, recv.out.String())
		}
		if !strings.Contains(recv.out.String(), victim.Path) {
			t.Errorf("corruption report does not name %s:\n%s", victim.Path, recv.out.String())
		}
		if _, err := os.Stat(filepath.Join(dst, "data", victim.Path)); err != nil {
			t.Errorf("DST not kept for inspection: %v", err)
		}
		if code := send.wait(t, 30*time.Second); code != 0 {
			t.Fatalf("send-tree exit %d:\n%s", code, send.out.String())
		}
	})

	t.Run("lifeline", func(t *testing.T) {
		send, addr, txPID := startSender(t, bench)
		if err := send.cmd.Process.Kill(); err != nil { // SIGKILL: no cleanup runs
			t.Fatal(err)
		}
		<-send.done
		waitGone(t, "orphaned tx send tree", txPID, 10*time.Second)
		if portOpen(addr) {
			t.Fatalf("%s still accepts connections after tx-bench was killed", addr)
		}
	})

	t.Run("forever", func(t *testing.T) {
		send, addr, txPID := startSender(t, bench)
		stream := filepath.Join(out, "forever.jsonl")
		removeStale(t, stream) // stale records would trigger the interrupt early
		recv := startBench(t, "recv-copy", recvArgs(addr, dst, "--forever", "-w", "0", "-o", stream, "--stop-sender")...)
		// A run's record is written when the next run starts, so two records
		// mean the third run is in flight when the interrupt lands.
		deadline := time.Now().Add(30 * time.Minute)
		for countRunRecords(stream) < 2 {
			select {
			case <-recv.done:
				t.Fatalf("forever run exited (%d) on its own:\n%s", recv.code, recv.out.String())
			case <-time.After(200 * time.Millisecond):
			}
			if time.Now().After(deadline) {
				t.Fatalf("fewer than 2 runs after 30m:\n%s", recv.out.String())
			}
		}
		if err := recv.cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		if code := recv.wait(t, 5*time.Minute); code != 0 {
			t.Fatalf("forever run after interrupt: exit %d:\n%s", code, recv.out.String())
		}
		data, err := os.ReadFile(stream)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if !strings.HasPrefix(lines[len(lines)-1], `{"summary":`) {
			t.Errorf("stream does not end with a summary: %s", lines[len(lines)-1])
		}
		if code := send.wait(t, 30*time.Second); code != 0 {
			t.Fatalf("send-tree exit %d:\n%s", code, send.out.String())
		}
		waitGone(t, "tx send tree", txPID, 10*time.Second)
		if portOpen(addr) {
			t.Errorf("%s still accepts connections after the forever run", addr)
		}
	})
}

// removeStale deletes a previous invocation's output, since TX_BENCH_ACCEPTANCE_OUT
// persists between runs.
func removeStale(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func countRunRecords(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), `{"run":`) {
			n++
		}
	}
	return n
}

// TestBenchAcceptanceDialBudget checks the dials of the main acceptance run.
// It fails until the client stops dialing a connection per request (make
// bench-acceptance-dials; CI allows the failure).
func TestBenchAcceptanceDialBudget(t *testing.T) {
	outDir := os.Getenv(envOut)
	if outDir == "" {
		t.Skip(envOut + " is not set (make bench-acceptance-dials)")
	}
	m, err := report.ReadMetrics(filepath.Join(outDir, "metrics.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		t.Fatal("no metrics from the main acceptance run; run make bench-acceptance first")
	}
	if err != nil {
		t.Fatal(err)
	}
	budget := int64(1000)
	if raw := os.Getenv(envMaxDial); raw != "" {
		if budget, err = strconv.ParseInt(raw, 10, 64); err != nil {
			t.Fatalf("%s=%q: %v", envMaxDial, raw, err)
		}
	}
	for _, r := range m.Runs {
		if r.Measured && r.Dials > budget {
			t.Errorf("run %s dialed %d connections (%d sync fallbacks, %d reuses); budget %d",
				r.Label, r.Dials, r.SyncFallbacks, r.Reuses, budget)
		}
	}
}
