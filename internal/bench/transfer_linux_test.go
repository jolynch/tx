//go:build linux

// Linux only: the in-memory source is a memfd (unix.MemfdCreate), which other
// platforms lack. The _linux file name already implies this constraint; the
// line above states it where a reader sees it.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/store"
)

// The in-memory transfer runs a real ftcp server and tx.Client over loopback
// TCP, with no filesystem on either side. The sender serves every file from
// one memfd the size of the largest file, so memory does not grow with the
// number of files or total bytes; the receiver discards what it reads. Only
// file open, create, and metadata syscalls are left out, which makes the
// per-file cost of tx itself (protocol, scheduling, store, hashing) visible.

// memDeps serves file content from src instead of opening paths. GetFileRef
// still runs, so the store's per-file lookup is part of the measurement.
type memDeps struct {
	ftcp.Deps
	src *os.File
}

func (d memDeps) GetFile(txferID string, fileID uint64, fullPath string) (*os.File, ftcp.FileRef, error) {
	ref, err := d.GetFileRef(txferID, fileID, fullPath)
	if err != nil {
		return nil, ftcp.FileRef{}, err
	}
	fd, err := unix.Dup(int(d.src.Fd()))
	if err != nil {
		return nil, ftcp.FileRef{}, err
	}
	return os.NewFile(uintptr(fd), fullPath), ref, nil
}

// memTransferConfig is one in-memory copy: files of fileSize adding up to
// totalBytes.
type memTransferConfig struct {
	fileSize   int64
	totalBytes int64
}

func (c memTransferConfig) files() int {
	return int(max(1, c.totalBytes/c.fileSize))
}

// memTransferResult is what a set of copies cost.
type memTransferResult struct {
	copies  int
	files   int64
	bytes   int64
	wall    time.Duration
	cpuNS   int64 // user plus system, both sides
	maxHeap uint64
}

func (r memTransferResult) coreSecPerGiB() float64 {
	return float64(r.cpuNS) / 1e9 / (float64(r.bytes) / (1 << 30))
}

func (r memTransferResult) mibPerSec() float64 {
	return float64(r.bytes) / (1 << 20) / r.wall.Seconds()
}

func (r memTransferResult) filesPerSec() float64 {
	return float64(r.files) / r.wall.Seconds()
}

// memTransferEnv is a server and a client sharing one process.
type memTransferEnv struct {
	store   *store.Store
	client  *tx.Client
	cfg     memTransferConfig
	conc    int
	plan    tx.BatchSizePlan
	sendBuf int64 // server socket write buffer, which sets the batch floor
	close   func()
}

// memTransferLinkMbps is the link speed the batch plan assumes. Loopback has
// no meaningful link, and a measured one would vary the plan between runs.
const memTransferLinkMbps = 10_000

// startMemTransfer starts the server and client. It plans concurrency and
// batch size as tx recv copy does, for a server with GOMAXPROCS CPUs on a
// memTransferLinkMbps link.
func startMemTransfer(tb testing.TB, cfg memTransferConfig) *memTransferEnv {
	tb.Helper()
	fd, err := unix.MemfdCreate("tx-bench-transfer", 0)
	if err != nil {
		tb.Fatalf("memfd_create: %v", err)
	}
	src := os.NewFile(uintptr(fd), "tx-bench-transfer")
	// Seeded random content: incompressible, so adaptive compression stays on
	// identity frames, and the same on every run.
	content := make([]byte, cfg.fileSize)
	_, _ = rand.NewChaCha8([32]byte{}).Read(content)
	if _, err := src.Write(content); err != nil {
		tb.Fatal(err)
	}

	logOut := log.Writer()
	log.SetOutput(io.Discard) // the server logs every transfer's progress
	st := store.NewStore()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	served := make(chan error, 1)
	go func() {
		served <- ftcp.Serve(ln, ftcp.ServerOptions{Deps: memDeps{Deps: ftcp.NewRuntimeDeps(st), src: src}})
	}()

	client := tx.NewClient(ln.Addr().String())
	probe, err := client.ProbeLink(context.Background(), tx.ProbeRequest{ProbeBytes: 1})
	if err != nil {
		tb.Fatalf("probe: %v", err)
	}
	conc := tx.LocalSuggestedConcurrency(runtime.GOMAXPROCS(0), probe.ServerIODepth)
	client.Close()
	client = tx.NewClient(ln.Addr().String(), tx.WithConcurrency(conc))
	plan := tx.ExplainBatchMaxBytes(conc, client.WindowConcurrency, client.FileRequestWindowBytes, probe.ServerSendBufBytes, memTransferLinkMbps)

	return &memTransferEnv{
		store:   st,
		client:  client,
		cfg:     cfg,
		conc:    conc,
		plan:    plan,
		sendBuf: probe.ServerSendBufBytes,
		close: func() {
			client.Close()
			_ = ln.Close()
			<-served
			st.Close()
			_ = src.Close()
			log.SetOutput(logOut)
		},
	}
}

const memTransferRoot = "/tx-bench"

// copyOnce registers a fresh transfer of cfg.files() files with the store,
// as TXFER would, and downloads all of it.
func (e *memTransferEnv) copyOnce(tb testing.TB) {
	tb.Helper()
	n := e.cfg.files()
	tr, err := e.store.NewTransfer(memTransferRoot, 0, 0, store.WithRequestPath(memTransferRoot))
	if err != nil {
		tb.Fatal(err)
	}
	updates := make(chan store.TransferFileStateUpdate, 1024)
	done := e.store.RegisterTransferFileState(tr.ID, updates, store.TransferStateStarted)
	manifest := &tx.Manifest{
		TransferID:  tr.ID,
		Root:        memTransferRoot,
		Mode:        tx.LoadStrategyFast,
		Concurrency: e.conc,
		Entries:     make([]tx.ManifestEntry, 0, n),
	}
	updates <- store.TransferFileStateUpdate{
		FileID:    encoding.RootFileID,
		EntryType: encoding.EntryTypeDir,
		PathHash:  xxh3.Hash128([]byte(memTransferRoot)),
	}
	for i := 1; i <= n; i++ {
		rel := fmt.Sprintf("d%03d/f%07d", i%256, i)
		updates <- store.TransferFileStateUpdate{
			FileID:    uint64(i),
			EntryType: encoding.EntryTypeFile,
			PathHash:  xxh3.Hash128([]byte(path.Join(memTransferRoot, rel))),
			FileSize:  e.cfg.fileSize,
		}
		manifest.Entries = append(manifest.Entries, tx.ManifestEntry{
			ID:    uint64(i),
			Type:  encoding.EntryTypeFile,
			Path:  rel,
			Size:  e.cfg.fileSize,
			Mtime: 1,
			Mode:  0o644,
		})
	}
	close(updates)
	<-done
	e.store.ClipTransfer(tr.ID)
	defer e.store.DeleteTransfer(tr.ID)

	resp, err := e.client.StartFromManifest(context.Background(), tx.StartFromManifestRequest{
		Manifest:           manifest,
		OutputWriter:       discardOutput,
		Concurrency:        e.conc,
		BatchMaxBytes:      e.plan.BatchMaxBytes,
		SplitWindowWorkers: e.plan.SplitWindowWorkers,
	})
	if err != nil {
		tb.Fatal(err)
	}
	if len(resp.Errors) > 0 || resp.Downloaded != n {
		tb.Fatalf("downloaded %d of %d files; first error: %v", resp.Downloaded, n, firstErr(resp.Errors))
	}
}

func discardOutput(tx.ManifestEntry, int64) (io.WriteCloser, func() error, error) {
	return nopWriteCloser{io.Discard}, nil, nil
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func firstErr(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

func processCPUNS() int64 {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return ru.Utime.Nano() + ru.Stime.Nano()
}

// memTransferSizes covers small files through files large enough to split
// into windows.
var memTransferSizes = []struct {
	name string
	size int64
}{
	{"4KiB", 4 << 10},
	{"16KiB", 16 << 10},
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
	{"16MiB", 16 << 20},
}

// BenchmarkTransferInMemory copies 64 MiB per op at each file size. Run it
// with -cpu to plan for that many server CPUs. core-s/GiB covers both sides.
func BenchmarkTransferInMemory(b *testing.B) {
	for _, sz := range memTransferSizes {
		b.Run(sz.name, func(b *testing.B) {
			cfg := memTransferConfig{fileSize: sz.size, totalBytes: 64 << 20}
			env := startMemTransfer(b, cfg)
			defer env.close()
			env.copyOnce(b) // warm the connection pools
			b.SetBytes(int64(cfg.files()) * cfg.fileSize)
			cpu0 := processCPUNS()
			b.ResetTimer()
			for b.Loop() {
				env.copyOnce(b)
			}
			b.StopTimer()
			gib := float64(b.N) * float64(cfg.files()) * float64(cfg.fileSize) / (1 << 30)
			b.ReportMetric(float64(processCPUNS()-cpu0)/1e9/gib, "core-s/GiB")
			b.ReportMetric(float64(b.N)*float64(cfg.files())/b.Elapsed().Seconds(), "files/s")
		})
	}
}

// TestTransferCostRatio compares what tx spends to copy the same bytes as
// small files and as 16 MiB files, for each server CPU count in
// TX_BENCH_THROUGHPUT_CPUS. Each size's cost ratio is its CPU per GiB over the
// 16 MiB files' CPU per GiB at the same CPU count, measured in the same run, so
// it holds across machines of different speed. The goal is 1.0 everywhere:
// many small files cost no more than one large file. The test fails when any
// ratio exceeds TX_BENCH_THROUGHPUT_MAX_RATIO, a bar against regressions
// rather than the goal. It is skipped unless TX_BENCH_THROUGHPUT_SIZE is set
// (make bench-throughput).
const (
	envTPSize     = "TX_BENCH_THROUGHPUT_SIZE"
	envTPCPUs     = "TX_BENCH_THROUGHPUT_CPUS"
	envTPMaxRatio = "TX_BENCH_THROUGHPUT_MAX_RATIO"
	envTPMinWall  = "TX_BENCH_THROUGHPUT_MIN_WALL"
	envTPOut      = "TX_BENCH_THROUGHPUT_OUT"
)

// throughputCPUCounts returns the server CPU counts the grid covers: by
// default, powers of two from 2 up to the host's CPU count, then the count
// itself, so the last row is the whole machine. Counts in
// TX_BENCH_THROUGHPUT_CPUS above the host's CPUs are dropped, since a row
// with more Go threads than CPUs would measure oversubscription.
func throughputCPUCounts(t *testing.T) []int {
	t.Helper()
	hostCPUs := runtime.NumCPU()
	var def []int
	for c := 2; c < hostCPUs; c *= 2 {
		def = append(def, c)
	}
	def = append(def, hostCPUs)
	var counts []int
	for _, c := range envInts(t, envTPCPUs, def) {
		if c > hostCPUs {
			t.Logf("skipping %d CPUs: the host has %d", c, hostCPUs)
			continue
		}
		counts = append(counts, c)
	}
	if len(counts) == 0 {
		t.Fatalf("%s leaves no CPU count at or below the host's %d", envTPCPUs, hostCPUs)
	}
	return counts
}

// defaultTPMaxRatio is the regression bar for the worst cost ratio. It must
// hold on any CI host, and hosts differ: 4 KiB files measured 9.5-15 on a
// Ryzen AI 9 HX 370 (2-24 CPUs) but 17.8-21.8 on a GitHub Actions EPYC 9V45
// (2-4 CPUs), where bulk byte work is relatively cheaper. Before small frames
// skipped zero-copy and manifest lookups were indexed, the Ryzen measured
// 2.5x its current ratio.
const defaultTPMaxRatio = 40.0

// The largest size is the reference every ratio divides by.
var memTransferReference = memTransferSizes[len(memTransferSizes)-1]

func TestTransferCostRatio(t *testing.T) {
	sizeRaw := os.Getenv(envTPSize)
	if sizeRaw == "" {
		t.Skip(envTPSize + " is not set (make bench-throughput)")
	}
	total, err := encoding.ParseByteSize(sizeRaw)
	if err != nil || total <= 0 {
		t.Fatalf("%s=%q: %v", envTPSize, sizeRaw, err)
	}
	cpuCounts := throughputCPUCounts(t)
	maxRatio := envFloat(t, envTPMaxRatio, defaultTPMaxRatio)
	minWall := 5 * time.Second
	if raw := os.Getenv(envTPMinWall); raw != "" {
		if minWall, err = time.ParseDuration(raw); err != nil {
			t.Fatalf("%s=%q: %v", envTPMinWall, raw, err)
		}
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))

	rep := newThroughputReport(total, maxRatio)
	t.Logf("host: %s, kernel %s, %d CPUs, %s; %s per copy, ratio bar %.1f",
		rep.Host.CPUModel, rep.Host.Kernel, rep.Host.NumCPU, rep.Host.GoVersion, encoding.HumanBytes(total), maxRatio)
	for _, cpus := range cpuCounts {
		runtime.GOMAXPROCS(cpus)
		rep.Rows = append(rep.Rows, measureThroughputRow(t, cpus, total, minWall))
	}
	logThroughputGrids(t, rep)

	worst, at := 0.0, ""
	for _, row := range rep.Rows {
		for _, sz := range row.Sizes {
			if sz.CostRatio > worst {
				worst, at = sz.CostRatio, fmt.Sprintf("%s files at %d CPUs", encoding.HumanBytes(sz.FileSize), row.CPUs)
			}
		}
	}
	rep.WorstRatio = worst
	rep.Pass = worst <= maxRatio
	writeThroughputReport(t, rep)
	if !rep.Pass {
		t.Errorf("copying %s as %s costs %.2fx the CPU of %s files, over the %.2f bar (%s)",
			encoding.HumanBytes(total), at, worst, memTransferReference.name, maxRatio, envTPMaxRatio)
	}
}

// measureThroughputRow measures every size with GOMAXPROCS already set to
// cpus, so the server and client plan for that many CPUs.
func measureThroughputRow(t *testing.T, cpus int, total int64, minWall time.Duration) throughputRow {
	t.Helper()
	envs := make([]*memTransferEnv, len(memTransferSizes))
	for i, sz := range memTransferSizes {
		envs[i] = startMemTransfer(t, memTransferConfig{fileSize: sz.size, totalBytes: total})
		envs[i].copyOnce(t) // warm the connection pools
	}
	results := measureInterleaved(t, envs, 3, minWall)
	row := throughputRow{
		CPUs:         cpus,
		Concurrency:  envs[0].conc,
		BatchBytes:   envs[0].plan.BatchMaxBytes,
		SendBufBytes: envs[0].sendBuf,
	}
	for _, env := range envs {
		env.close()
	}
	ref := results[len(results)-1].coreSecPerGiB()
	for i, sz := range memTransferSizes {
		r := results[i]
		row.Sizes = append(row.Sizes, throughputSize{
			FileSize:      sz.size,
			FilesPerCopy:  int(r.files / int64(r.copies)),
			Copies:        r.copies,
			MiBPerSec:     r.mibPerSec(),
			FilesPerSec:   r.filesPerSec(),
			CoreSecPerGiB: r.coreSecPerGiB(),
			CoreUSPerFile: float64(r.cpuNS) / 1e3 / float64(r.files),
			CostRatio:     r.coreSecPerGiB() / ref,
			HeapBytes:     r.maxHeap,
		})
	}
	return row
}

// measureInterleaved copies every size once per round until both minRounds
// and minWall are reached. Interleaving keeps machine drift (clock speed,
// background load) from landing on one size. Each copy ends with a GC inside
// its own measurement, so no copy pays for another's garbage.
func measureInterleaved(tb testing.TB, envs []*memTransferEnv, minRounds int, minWall time.Duration) []memTransferResult {
	tb.Helper()
	results := make([]memTransferResult, len(envs))
	var ms runtime.MemStats
	runtime.GC()
	start := time.Now()
	for round := 0; round < minRounds || time.Since(start) < minWall; round++ {
		for i, env := range envs {
			cpu0, wall0 := processCPUNS(), time.Now()
			env.copyOnce(tb)
			runtime.ReadMemStats(&ms)
			runtime.GC()
			r := &results[i]
			r.wall += time.Since(wall0)
			r.cpuNS += processCPUNS() - cpu0
			r.copies++
			r.files += int64(env.cfg.files())
			r.bytes += int64(env.cfg.files()) * env.cfg.fileSize
			r.maxHeap = max(r.maxHeap, ms.HeapInuse)
		}
	}
	return results
}

// logThroughputGrids logs one grid per metric: a row per CPU count, a column
// per file size.
func logThroughputGrids(t *testing.T, rep throughputReport) {
	t.Helper()
	grids := []struct {
		title string
		cell  func(throughputSize) string
	}{
		{"cost ratio vs " + memTransferReference.name + " files (goal 1.00)", func(s throughputSize) string { return fmt.Sprintf("%.2f", s.CostRatio) }},
		{"CPU, core-s/GiB", func(s throughputSize) string { return fmt.Sprintf("%.2f", s.CoreSecPerGiB) }},
		{"rate, MiB/s", func(s throughputSize) string { return fmt.Sprintf("%.0f", s.MiBPerSec) }},
	}
	for _, g := range grids {
		var b strings.Builder
		fmt.Fprintf(&b, "\n%s\n%6s", g.title, "CPUs")
		for _, sz := range memTransferSizes {
			fmt.Fprintf(&b, " %8s", sz.name)
		}
		fmt.Fprintf(&b, " %18s", "plan")
		for _, row := range rep.Rows {
			fmt.Fprintf(&b, "\n%6d", row.CPUs)
			for _, sz := range row.Sizes {
				fmt.Fprintf(&b, " %8s", g.cell(sz))
			}
			fmt.Fprintf(&b, " %18s", fmt.Sprintf("conc %d, batch %s", row.Concurrency, encoding.HumanBytes(row.BatchBytes)))
		}
		t.Log(b.String())
	}
}

// throughputReport is the JSON written to TX_BENCH_THROUGHPUT_OUT, so CI can
// keep each run's grid alongside the host it came from.
type throughputReport struct {
	Host struct {
		CPUModel  string `json:"cpu_model"`
		Kernel    string `json:"kernel"`
		NumCPU    int    `json:"num_cpu"`
		GoVersion string `json:"go_version"`
	} `json:"host"`
	CopyBytes  int64           `json:"copy_bytes"`
	LinkMbps   int64           `json:"link_mbps"`
	Rows       []throughputRow `json:"rows"`
	WorstRatio float64         `json:"worst_ratio"`
	MaxRatio   float64         `json:"max_ratio"`
	Pass       bool            `json:"pass"`
}

// throughputRow is one CPU count: the plan tx made for it and every size.
type throughputRow struct {
	CPUs         int              `json:"cpus"`
	Concurrency  int              `json:"concurrency"`
	BatchBytes   int64            `json:"batch_bytes"`
	SendBufBytes int64            `json:"send_buf_bytes"`
	Sizes        []throughputSize `json:"sizes"`
}

type throughputSize struct {
	FileSize      int64   `json:"file_size"`
	FilesPerCopy  int     `json:"files_per_copy"`
	Copies        int     `json:"copies"`
	MiBPerSec     float64 `json:"mib_per_sec"`
	FilesPerSec   float64 `json:"files_per_sec"`
	CoreSecPerGiB float64 `json:"core_sec_per_gib"`
	CoreUSPerFile float64 `json:"core_us_per_file"`
	CostRatio     float64 `json:"cost_ratio"`
	HeapBytes     uint64  `json:"heap_bytes"`
}

func newThroughputReport(total int64, maxRatio float64) throughputReport {
	var rep throughputReport
	rep.Host.CPUModel = cpuModel()
	var uts unix.Utsname
	if unix.Uname(&uts) == nil {
		rep.Host.Kernel = unix.ByteSliceToString(uts.Release[:])
	}
	rep.Host.NumCPU = runtime.NumCPU()
	rep.Host.GoVersion = runtime.Version()
	rep.CopyBytes = total
	rep.LinkMbps = memTransferLinkMbps
	rep.MaxRatio = maxRatio
	return rep
}

func cpuModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	for line := range strings.Lines(string(data)) {
		if key, val, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "model name" {
			return strings.TrimSpace(val)
		}
	}
	return "unknown"
}

func writeThroughputReport(t *testing.T, rep throughputReport) {
	t.Helper()
	dir := os.Getenv(envTPOut)
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "throughput.json")
	if err := os.WriteFile(out, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", out)
}

func envFloat(t *testing.T, name string, def float64) float64 {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v < 0 {
		t.Fatalf("%s=%q: must be a non-negative number", name, raw)
	}
	return v
}

// envInts parses a comma-separated list of positive integers.
func envInts(t *testing.T, name string, def []int) []int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	var out []int
	for field := range strings.SplitSeq(raw, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || v <= 0 {
			t.Fatalf("%s=%q: must be a comma-separated list of positive integers", name, raw)
		}
		out = append(out, v)
	}
	return out
}
