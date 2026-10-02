package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"

	"golang.org/x/sys/unix"
)

// txBin is a freshly built tx, shared by every test in the package.
var txBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "tx-bench-test-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	txBin = filepath.Join(dir, "tx")
	root, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "go env GOMOD:", err)
		os.Exit(1)
	}
	build := exec.Command("go", "build", "-o", txBin, "./cmd/tx")
	build.Dir = filepath.Dir(strings.TrimSpace(string(root)))
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build tx: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// tinyMix avoids Silesia so tests never download the corpus.
const tinyMix = "rand=60%@64KiB,rand=40%@1KiB..16KiB"

type localResult struct {
	code           int
	stdout, stderr string
}

func runLocal(t *testing.T, args ...string) localResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := run(ctx, append([]string{"local", "--tx", txBin}, args...), &stdout, &stderr)
	return localResult{code, stdout.String(), stderr.String()}
}

func TestLocalEndToEnd(t *testing.T) {
	dir := t.TempDir()
	bench, dst := filepath.Join(dir, "bench"), filepath.Join(dir, "dst")
	metrics := filepath.Join(dir, "m.json")
	res := runLocal(t, "-s", "1MiB", "-m", tinyMix, "-c", "0%", "-w", "1", "-n", "2", "-o", metrics,
		"--send-metrics", filepath.Join(dir, "send.jsonl"), bench, dst, "--", "--progress=false")
	if res.code != 0 {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
	for _, label := range []string{"w1", "1", "2"} {
		if f := tableRow(res.stdout, label); len(f) != 8 || f[7] != "ok" {
			t.Errorf("run row %s = %q:\n%s", label, f, res.stdout)
		}
	}
	for _, want := range []string{"dataset   1.0MiB", "data-cold", "2 measured, 0 failed, 1 warmup", "fingerprint ok"} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("report lacks %q:\n%s", want, res.stdout)
		}
	}
	assertAligned(t, res.stdout, "run ", "status", true)
	assertAligned(t, res.stdout, "run ", "rate", false)
	assertAligned(t, res.stdout, "  metric", "unit", true)
	assertAligned(t, res.stdout, "  metric", "max", false)
	if f := tableRow(res.stdout, "wall"); len(f) != 5 {
		t.Errorf("wall row = %q", f)
	}
	if f := tableRow(res.stderr, "prep"); len(f) < 9 || f[1] != "1" || f[2] != "#2" {
		t.Errorf("first prep row = %q:\n%s", f, res.stderr)
	}
	if f := tableRow(res.stderr, "flush"); len(f) < 9 || f[1] != "4" {
		t.Errorf("flush row = %q", f)
	}
	assertAligned(t, res.stderr, "event", "cpu", false)
	assertAligned(t, res.stderr, "event", "tid", true)
	m, err := report.ReadMetrics(metrics)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Runs) != 3 || m.Summary.Measured != 2 || m.Summary.Failed != 0 {
		t.Fatalf("metrics runs=%d summary=%+v", len(m.Runs), m.Summary)
	}
	tids := map[string]bool{}
	for _, r := range m.Runs {
		if r.Status != report.StatusOK || r.TID == "" || r.Sender == nil || r.Bytes != 1<<20 || r.Verify.Mode != "full" ||
			r.Verify.Mismatches != 0 || r.Client.MaxRSS == 0 || r.Sender.Rusage.MaxRSS == 0 || r.Cache.Warm != "0%" {
			t.Fatalf("run %s: %+v sender %+v", r.Label, r, r.Sender)
		}
		if tids[r.TID] {
			t.Fatalf("tid %s reused across runs", r.TID)
		}
		tids[r.TID] = true
	}
	if m.Header.Dataset.Files != m.Runs[0].Verify.Files || m.Header.TxHash != m.Header.SenderTx {
		t.Fatalf("header %+v", m.Header)
	}
	// Every metrics format re-renders the same report.
	var want bytes.Buffer
	report.RenderText(&want, m)
	for _, ext := range []string{".jsonl", ".txt"} {
		p := filepath.Join(dir, "m"+ext)
		if err := report.WriteMetrics(p, m); err != nil {
			t.Fatal(err)
		}
		var got, stderr bytes.Buffer
		if code := run(context.Background(), []string{"report", p}, &got, &stderr); code != 0 {
			t.Fatalf("report %s: exit %d %s", ext, code, stderr.String())
		}
		if ext == ".jsonl" && got.String() != want.String() {
			t.Fatalf("report %s differs:\n%s\nwant:\n%s", ext, got.String(), want.String())
		}
		if ext == ".txt" && !strings.Contains(got.String(), "2 measured, 0 failed") {
			t.Fatalf("report %s:\n%s", ext, got.String())
		}
	}
	// The sender appended one record per run served.
	data, err := os.ReadFile(filepath.Join(dir, "send.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(strings.TrimSpace(string(data)), "\n") + 1; n != 3 {
		t.Fatalf("sender metrics has %d records, want 3", n)
	}
	// DST is cleaned after the last run but stays marked.
	if _, err := os.Stat(filepath.Join(dst, "data")); !os.IsNotExist(err) {
		t.Fatalf("DST/data left behind: %v", err)
	}
	if !dataset.IsMarked(dst, dataset.DstMarker) {
		t.Fatal("DST lost its marker")
	}

	// A second invocation reuses the dataset, and --skip-write turns the
	// oracle off.
	res = runLocal(t, "-w", "0", "-n", "1", "-o", "", bench, dst, "--", "--skip-write", "--progress=false")
	if f := tableRow(res.stdout, "1"); res.code != 0 || len(f) < 8 || f[len(f)-1] != "(unverified)" {
		t.Fatalf("skip-write run: exit %d\n%s\n%s", res.code, res.stdout, res.stderr)
	}
	if strings.Contains(res.stderr, "generating") {
		t.Fatal("second run regenerated the dataset")
	}
}

func TestLocalReadOnlyImportedDirectoryStartsFreshRuns(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so the read-only case cannot occur")
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	readOnly := filepath.Join(source, "readonly")
	if err := os.MkdirAll(readOnly, 0o755); err != nil {
		t.Fatal(err)
	}
	const size = 56 << 10
	if err := os.WriteFile(filepath.Join(readOnly, "payload"), bytes.Repeat([]byte("x"), size), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(readOnly, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o755) })

	bench, dst := filepath.Join(dir, "bench"), filepath.Join(dir, "dst")
	metrics := filepath.Join(dir, "metrics.json")
	res := runLocal(t, "--in", source, "-w", "0", "-n", "2", "-o", metrics, bench, dst, "--", "--progress=false")
	if res.code != 0 {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
	m, err := report.ReadMetrics(metrics)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Runs) != 2 || m.Summary.Failed != 0 {
		t.Fatalf("runs=%d summary=%+v", len(m.Runs), m.Summary)
	}
	for _, run := range m.Runs {
		if run.Status != report.StatusOK || run.LogicalBytes != size || run.Sender == nil || run.Sender.Overlap {
			t.Fatalf("run %s reused a destination: status=%s logical=%d sender=%+v", run.Label, run.Status, run.LogicalBytes, run.Sender)
		}
	}
	if _, err := os.Stat(filepath.Join(dst, "data")); !os.IsNotExist(err) {
		t.Fatalf("final DST/data cleanup: %v", err)
	}
}

func TestLocalCorruptionExitsThree(t *testing.T) {
	dir := t.TempDir()
	bench, dst := filepath.Join(dir, "bench"), filepath.Join(dir, "dst")
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"prep", "-s", "256KiB", "-m", tinyMix, bench}, &stdout, &stderr); code != 0 {
		t.Fatalf("prep: %d %s", code, stderr.String())
	}
	d, err := dataset.Load(bench)
	if err != nil {
		t.Fatal(err)
	}
	// Flip one byte of one file, keeping its size and mtime, so only the
	// content oracle can notice.
	var victim string
	for _, e := range d.Entries {
		if e.Type == dataset.TypeFile && e.Size > 0 {
			victim = e.Path
			p := filepath.Join(d.DataRoot(), e.Path)
			b, _ := os.ReadFile(p)
			b[0] ^= 1
			if err := os.WriteFile(p, b, 0o644); err != nil {
				t.Fatal(err)
			}
			mt := time.Unix(0, e.MtimeNS)
			if err := os.Chtimes(p, mt, mt); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	res := runLocal(t, "-w", "0", "-n", "2", "-o", "", bench, dst, "--", "--progress=false")
	if res.code != exitCorruption {
		t.Fatalf("exit %d, want %d\n%s\n%s", res.code, exitCorruption, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stderr, "CORRUPTION in run 1") || !strings.Contains(res.stderr, victim) {
		t.Fatalf("corruption report lacks the run or path %s:\n%s", victim, res.stderr)
	}
	if strings.Contains(res.stdout, "run    2/2") {
		t.Fatal("benchmark continued after corruption")
	}
	if _, err := os.Stat(filepath.Join(dst, "data", victim)); err != nil {
		t.Fatalf("DST not kept for inspection: %v", err)
	}
}

func TestLocalRefusesUnmarkedDst(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "precious")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dst, "keep.txt")
	if err := os.WriteFile(keep, []byte("mine"), 0o644); err != nil {
		t.Fatal(err)
	}
	res := runLocal(t, "-s", "64KiB", "-m", tinyMix, filepath.Join(dir, "bench"), dst)
	if res.code != exitUsage || !strings.Contains(res.stderr, "not created by tx-bench") {
		t.Fatalf("exit %d\n%s", res.code, res.stderr)
	}
	if data, err := os.ReadFile(keep); err != nil || string(data) != "mine" {
		t.Fatalf("unmarked DST was touched: %v %q", err, data)
	}
	if _, err := os.Stat(filepath.Join(dir, "bench")); !os.IsNotExist(err) {
		t.Fatal("the sender started before DST was checked")
	}
}

func TestLocalBadPassThroughFlagFailsFast(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	res := runLocal(t, "-s", "64KiB", "-m", tinyMix, "-n", "3", "-o", "", filepath.Join(dir, "b"), filepath.Join(dir, "d"), "--", "--no-such-flag")
	if res.code != exitUsage || !strings.Contains(res.stderr, "no-such-flag") {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", res.code, res.stdout, res.stderr)
	}
	if tableRow(res.stdout, "2") != nil || time.Since(start) > time.Minute {
		t.Fatal("a bad flag became a string of failed runs")
	}
	res = runLocal(t, "-s", "64KiB", "-m", tinyMix, filepath.Join(dir, "b"), filepath.Join(dir, "d"), "--", "--stats", "x")
	if res.code != exitUsage || !strings.Contains(res.stderr, "set by tx-bench") {
		t.Fatalf("managed flag accepted: exit %d %s", res.code, res.stderr)
	}
}

func TestParsePassThrough(t *testing.T) {
	pt := parsePassThrough([]string{"--compress", "zstd", "-t=abc", "--skip-write", "--fsync-interval", "-1", "-k", "/keys", "--encrypt=auto", "-v"})
	if got := pt.values("t", "auth-token"); !slices.Equal(got, []string{"abc"}) {
		t.Fatalf("t = %v", got)
	}
	if !pt.boolSet("skip-write") || pt.boolSet("clean") {
		t.Fatal("bool flags misread")
	}
	if got := pt.values("fsync-interval"); !slices.Equal(got, []string{"-1"}) {
		t.Fatalf("negative value misread: %v", got)
	}
	if got := pt.fetchArgs(); !slices.Equal(got, []string{"--t=abc", "--k=/keys", "--encrypt=auto"}) {
		t.Fatalf("fetchArgs = %v", got)
	}
	if pt.setting("compress", "adapt") != "zstd" || pt.setting("encrypt", "none") != "auto" {
		t.Fatal("settings misread")
	}
	if err := parsePassThrough([]string{"-p", "x"}).checkManaged(recvManaged(true)); err == nil {
		t.Fatal("-p accepted while tracing")
	}
	if err := parsePassThrough([]string{"-p", "x", "-f", "json"}).checkManaged(recvManaged(false)); err != nil {
		t.Fatalf("-p rejected without tracing: %v", err)
	}
	if err := parsePassThrough([]string{"--listen=:1"}).checkManaged(sendManaged(false)); err == nil {
		t.Fatal("--listen accepted")
	}
}

func TestParseServerArg(t *testing.T) {
	cases := map[string][2]string{
		"10.0.4.17:3453":           {"10.0.4.17:3453", "/"},
		"host":                     {"host:3453", "/"},
		"host:99/srv/bench/":       {"host:99", "/srv/bench"},
		"tx://[::1]:7/b":           {"[::1]:7", "/b"},
		"127.0.0.1:3453/":          {"127.0.0.1:3453", "/"},
		"":                         {"127.0.0.1:3453", "/"},
		"host/with/nested/bench":   {"host:3453", "/with/nested/bench"},
		"[2001:db8::1]/bench/path": {"[2001:db8::1]:3453", "/bench/path"},
	}
	for in, want := range cases {
		hp, br, err := parseServerArg(in)
		if err != nil || hp != want[0] || br != want[1] {
			t.Errorf("parseServerArg(%q) = %q %q %v; want %v", in, hp, br, err, want)
		}
	}
}

func TestLocalTraceBothSides(t *testing.T) {
	dir := t.TempDir()
	client, sender := filepath.Join(dir, "c.jsonl"), filepath.Join(dir, "s.txt")
	res := runLocal(t, "-s", "2MiB", "-m", "rand=50%@512KiB,rand=50%@4KiB..32KiB", "-w", "1", "-n", "1", "-o", "",
		"--trace", client, "--send-trace", sender, "--trace-rss-interval", "5ms",
		filepath.Join(dir, "b"), filepath.Join(dir, "d"), "--", "--progress=false")
	if res.code != 0 {
		t.Fatalf("exit %d\n%s\n%s", res.code, res.stdout, res.stderr)
	}
	h, recs, err := report.ReadTrace(client)
	if err != nil {
		t.Fatal(err)
	}
	if h.Side != "c" || h.ClockErrNS <= 0 {
		t.Fatalf("client header %+v", h)
	}
	count := func(recs []report.TraceRecord) map[string]int {
		n := map[string]int{}
		for _, r := range recs {
			n[r.Ev]++
		}
		return n
	}
	evs := count(recs)
	for _, ev := range []string{"run_start", "run_end", "probe", "manifest_start", "manifest_end", "conn_dial",
		"req_start", "req_end", "file_start", "file_done", "window", "fsync", "ack", "rss", "verify_start", "verify_end"} {
		if evs[ev] == 0 {
			t.Errorf("client trace has no %s events (%v)", ev, evs)
		}
	}
	runs := map[string]bool{}
	for _, r := range recs {
		runs[r.Run] = true
	}
	if !runs["w1"] || !runs["1"] || len(runs) != 2 {
		t.Fatalf("client runs %v", runs)
	}
	_, server, err := report.ReadTrace(report.SiblingPath(client, "server"))
	if err != nil {
		t.Fatal(err)
	}
	sevs := count(server)
	for _, ev := range []string{"accept", "cmd_start", "cmd_end", "file_open", "window", "file_done", "transfer_done", "ack"} {
		if sevs[ev] == 0 {
			t.Errorf("fetched sender trace has no %s events (%v)", ev, sevs)
		}
	}
	sh, sendTrace, err := report.ReadTrace(sender)
	if err != nil {
		t.Fatalf("sender --trace (text): %v", err)
	}
	if sh.Side != "s" || count(sendTrace)["prep_start"] != 3 || count(sendTrace)["prep_end"] != 3 || count(sendTrace)["window"] < sevs["window"] {
		t.Fatalf("sender --trace header %+v events %v", sh, count(sendTrace))
	}

	a, err := report.AnalyzeTrace(client, report.AnalysisOptions{Top: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Runs) != 1 || a.Runs[0].Run != "1" || a.Runs[0].ClientWindows == 0 || a.Runs[0].Joined != a.Runs[0].ClientWindows ||
		len(a.Runs[0].Slowest) != 3 || !a.Runs[0].Slowest[0].HasSender {
		t.Fatalf("analysis %+v", a.Runs)
	}
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"report", "--run", "w1", "--top", "2", client}, &out, &errOut); code != 0 ||
		!strings.Contains(out.String(), "run w1") || !strings.Contains(out.String(), "slowest files") {
		t.Fatalf("report on trace: %d\n%s%s", code, out.String(), errOut.String())
	}
}

func TestResolveTxPrecedence(t *testing.T) {
	dir := t.TempDir()
	mk := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(name), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	fromFlag, fromEnv := mk("tx-flag"), mk("tx-env")
	t.Setenv("TX_BIN", fromEnv)
	if b, err := resolveTx(fromFlag); err != nil || b.Path != fromFlag {
		t.Fatalf("--tx should win over $TX_BIN: %+v %v", b, err)
	}
	if b, err := resolveTx(""); err != nil || b.Path != fromEnv {
		t.Fatalf("$TX_BIN should be used without --tx: %+v %v", b, err)
	}
	t.Setenv("TX_BIN", filepath.Join(dir, "missing"))
	if _, err := resolveTx(""); err == nil || !strings.Contains(err.Error(), "TX_BIN") {
		t.Fatalf("a bad $TX_BIN should be reported: %v", err)
	}
}

// tableRow returns the fields of the first output line whose first field is
// label, or nil.
func tableRow(out, label string) []string {
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) > 0 && f[0] == label {
			return f
		}
	}
	return nil
}

// assertAligned checks that a fixed-width table's rows put a column where its
// header does: rows up to the next blank line must start (left-aligned) or
// end (right-aligned) the column at the header's offset.
func assertAligned(t *testing.T, out, headerPrefix, col string, left bool) {
	t.Helper()
	lines := strings.Split(out, "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, headerPrefix) || !strings.Contains(line, col) {
			continue
		}
		start := strings.Index(line, col)
		end := start + len(col)
		for _, row := range lines[i+1:] {
			switch {
			case strings.TrimSpace(row) == "":
				return
			case strings.HasPrefix(row, "warning"), strings.HasPrefix(row, "note"):
				continue
			case left && (len(row) <= start || row[start-1] != ' ' || row[start] == ' '):
				t.Errorf("column %s does not start at %d:\n%s\n%s", col, start, line, row)
			case !left && (len(row) < end || row[end-1] == ' ' || (len(row) > end && row[end] != ' ')):
				t.Errorf("column %s does not end at %d:\n%s\n%s", col, end, line, row)
			}
		}
		return
	}
	t.Errorf("no table header starting %q with %q in:\n%s", headerPrefix, col, out)
}

func TestStopSenderEndsSendTree(t *testing.T) {
	dir := t.TempDir()
	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	var sendOut, sendErr bytes.Buffer
	sendDone := make(chan int, 1)
	go func() {
		sendDone <- run(context.Background(), []string{"remote", "send-tree", "--tx", txBin, "-l", addr,
			"-s", "128KiB", "-m", tinyMix, filepath.Join(dir, "b")}, &sendOut, &sendErr)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var out, errOut bytes.Buffer
	deadline := time.Now().Add(30 * time.Second)
	code := -1
	for time.Now().Before(deadline) {
		out.Reset()
		errOut.Reset()
		code = run(ctx, []string{"remote", "recv-copy", "--tx", txBin, "-w", "0", "-n", "1", "-o", "", "--stop-sender",
			addr, filepath.Join(dir, "d"), "--", "--progress=false"}, &out, &errOut)
		if code == 0 || !strings.Contains(errOut.String(), "connection refused") {
			break
		}
		time.Sleep(100 * time.Millisecond) // send-tree is still generating
	}
	if code != 0 {
		t.Fatalf("recv-copy exit %d\n%s\n%s", code, out.String(), errOut.String())
	}
	select {
	case c := <-sendDone:
		if c != 0 {
			t.Fatalf("send-tree exit %d\n%s\n%s", c, sendOut.String(), sendErr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("send-tree still running after --stop-sender\n%s", sendOut.String())
	}
	if f := tableRow(sendOut.String(), "stop"); f == nil {
		t.Fatalf("no stop row:\n%s", sendOut.String())
	}
	// The last run's sender side was recorded before the sender left.
	m := strings.Contains(out.String(), "sender conns")
	if !m {
		t.Fatalf("report lacks sender metrics:\n%s", out.String())
	}
}

func TestForeverStreamsMetricsToFIFO(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "metrics.jsonl") // a FIFO; the name picks JSON lines
	if err := unix.Mkfifo(fifo, 0o644); err != nil {
		t.Skip("mkfifo:", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out, errOut bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"local", "--tx", txBin, "-s", "128KiB", "-m", tinyMix, "-w", "0", "--forever",
			"-o", fifo, filepath.Join(dir, "b"), filepath.Join(dir, "d"), "--", "--progress=false"}, &out, &errOut)
	}()
	// Attach the reader late: records written meanwhile must be kept.
	time.Sleep(2 * time.Second)
	f, err := os.Open(fifo)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var kinds []string
	runs := 0
	for runs < 3 {
		var rec map[string]json.RawMessage
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("decode after %v: %v\n%s\n%s", kinds, err, out.String(), errOut.String())
		}
		for k := range rec {
			kinds = append(kinds, k)
			if k == "run" {
				runs++
			}
		}
	}
	cancel() // the interrupt: wrap up, flush, report
	for {
		var rec map[string]json.RawMessage
		if err := dec.Decode(&rec); err != nil {
			break
		}
		for k := range rec {
			kinds = append(kinds, k)
		}
	}
	code := <-done
	if code != 0 {
		t.Fatalf("forever run exit %d\n%s\n%s", code, out.String(), errOut.String())
	}
	if kinds[0] != "header" || kinds[len(kinds)-1] != "summary" {
		t.Fatalf("stream records %v", kinds)
	}
	if !strings.Contains(out.String(), "measured, 0 failed") {
		t.Fatalf("report after interrupt:\n%s", out.String())
	}
}

// TestLifelineEndsOrphanedSender closes the lifeline the way the kernel
// does when tx-bench dies, and checks tx send tree shuts down cleanly.
func TestLifelineEndsOrphanedSender(t *testing.T) {
	dir := t.TempDir()
	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	stats := filepath.Join(dir, "stats.jsonl")
	p, err := startProc(txBin, []string{"send", "tree", "--exit-after", "never", "--keys", dir,
		"--listen", fmt.Sprintf("127.0.0.1:%d", port), "--stats", stats, "--exit-with", "stdin", dir}, filepath.Join(dir, "log"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitListening(context.Background(), fmt.Sprintf("127.0.0.1:%d", port), p, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	p.lifeline.Close()
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		p.stop(time.Second)
		t.Fatal("tx send tree outlived its lifeline")
	}
	if p.exitCode() != 0 {
		t.Fatalf("exit code %d\n%s", p.exitCode(), tailFile(filepath.Join(dir, "log"), 10))
	}
	if data, _ := os.ReadFile(stats); !strings.Contains(string(data), `"exit":"exit-with:stdin"`) {
		t.Fatalf("stats lack the exit-with:stdin exit:\n%s", data)
	}
}
