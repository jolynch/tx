//go:build linux

// Linux only: both sides read a memfd (unix.MemfdCreate), which other
// platforms lack.

package filexfercli

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/benchio"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/store"
)

// The in-memory verification runs the copy's data verification against a
// real FTCP server over loopback, with file contents held in memfds. Files
// are served from a set of identical memfds, file i from memfd i mod n, on
// both sides, and the run measures what verification itself costs: request
// batching, overlapping local and remote hashing, and both sides' per-file
// work. Several memfds keep concurrent reads of "different" small files
// from all contending for one page, which no real tree does.

// verifyMemfdBytes caps the memory the memfds of one file size hold.
const verifyMemfdBytes = 64 << 20

// verifyMemDeps serves CXSUM file i from srcs[i mod len(srcs)]. Lookups still
// resolve through the store, so path checks are part of the measurement.
type verifyMemDeps struct {
	intftcp.Deps
	srcs []*os.File
}

func (d verifyMemDeps) GetFile(txferID string, fileID uint64, fullPath string) (*os.File, intftcp.FileRef, error) {
	refs, errs := d.GetFileRefs(txferID, []intftcp.FileLookup{{FileID: fileID, Path: fullPath}})
	if errs[0] != nil {
		return nil, intftcp.FileRef{}, errs[0]
	}
	fd, err := unix.Dup(int(d.srcs[fileID%uint64(len(d.srcs))].Fd()))
	if err != nil {
		return nil, intftcp.FileRef{}, err
	}
	return os.NewFile(uintptr(fd), refs[0].Path), refs[0], nil
}

const verifyMemRoot = "/tx-verify-bench"

// verifyBusy totals, from both sides' event timelines, the time spent in
// each half of verification.
type verifyBusy struct {
	hash   atomic.Int64 // receiver: local hashing (verify_hash)
	server atomic.Int64 // server: CXSUM commands (cmd_end)
}

func (v *verifyBusy) reset() {
	v.hash.Store(0)
	v.server.Store(0)
}

// verifyMemEnv is a server and one registered transfer of files of one
// size. Clients connect with newClient.
type verifyMemEnv struct {
	addr       string
	transferID string
	entries    []tx.ManifestEntry
	localPaths []string // the memfds, as hashItems opens them
	clientSink *events.Sink
	busy       *verifyBusy
	ioLatency  *verifyIOLatency
	ioDepth    int
}

func startVerifyMemEnv(tb testing.TB, fileSize, totalBytes int64) *verifyMemEnv {
	tb.Helper()
	depth := tx.DefaultTargetIODepth
	if raw := os.Getenv("TX_VERIFY_IO_DEPTH"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			tb.Fatalf("invalid TX_VERIFY_IO_DEPTH %q", raw)
		}
		depth = n
	}
	n := int(max(1, totalBytes/fileSize))
	content := make([]byte, fileSize)
	_, _ = rand.NewChaCha8([32]byte{}).Read(content)
	srcs := make([]*os.File, min(n, 256, int(max(1, verifyMemfdBytes/fileSize))))
	tb.Cleanup(func() {
		for _, src := range srcs {
			if src != nil {
				_ = src.Close()
			}
		}
	})
	localPaths := make([]string, len(srcs))
	for i := range srcs {
		fd, err := unix.MemfdCreate("tx-verify-bench", 0)
		if err != nil {
			tb.Fatalf("memfd_create: %v", err)
		}
		srcs[i] = os.NewFile(uintptr(fd), "tx-verify-bench")
		if _, err := srcs[i].Write(content); err != nil {
			tb.Fatal(err)
		}
		localPaths[i] = fmt.Sprintf("/proc/self/fd/%d", fd)
	}

	busy := &verifyBusy{}
	latency := newVerifyIOLatency(tb, os.Getenv("TX_VERIFY_IO_LATENCY"))
	if latency != nil {
		tb.Cleanup(benchio.SetReadLatency(latency.wait))
	}
	serverSink, clientSink := events.NewSink(), events.NewSink()
	serverSink.Subscribe(func(e events.Event) {
		if e.Name == "cmd_end" && e.Str("verb") == "CXSUM" {
			busy.server.Add(e.Int("dur"))
		}
	})
	clientSink.Subscribe(func(e events.Event) {
		if e.Name == "verify_hash" {
			busy.hash.Add(e.Int("dur"))
		}
	})

	logOut := log.Writer()
	tb.Cleanup(func() { log.SetOutput(logOut) })
	log.SetOutput(io.Discard)
	st := store.NewStore()
	tb.Cleanup(st.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	served := make(chan error, 1)
	tb.Cleanup(func() {
		_ = ln.Close()
		if err := <-served; err != nil {
			tb.Error(err)
		}
	})
	go func() {
		served <- intftcp.Serve(ln, intftcp.ServerOptions{
			Deps:             verifyMemDeps{Deps: intftcp.NewRuntimeDeps(st), srcs: srcs},
			KeepAliveTimeout: time.Minute,
			TargetIODepth:    depth,
			Events:           serverSink,
		})
	}()

	tr, err := st.NewTransfer(verifyMemRoot, 0, 0)
	if err != nil {
		tb.Fatal(err)
	}
	updates := make(chan store.TransferFileStateUpdate, 1024)
	done := st.RegisterTransferFileState(tr.ID, updates, store.TransferStateDone)
	updates <- store.TransferFileStateUpdate{FileID: encoding.RootFileID, EntryType: encoding.EntryTypeDir, PathHash: xxh3.Hash128([]byte(verifyMemRoot))}
	entries := make([]tx.ManifestEntry, 0, n)
	for i := 1; i <= n; i++ {
		rel := fmt.Sprintf("d%03d/f%07d", i%256, i)
		updates <- store.TransferFileStateUpdate{
			FileID:    uint64(i),
			EntryType: encoding.EntryTypeFile,
			PathHash:  xxh3.Hash128([]byte(path.Join(verifyMemRoot, rel))),
			FileSize:  fileSize,
		}
		entries = append(entries, tx.ManifestEntry{ID: uint64(i), Type: encoding.EntryTypeFile, Path: rel, Size: fileSize, Mtime: 1, Mode: 0o644})
	}
	close(updates)
	<-done
	st.ClipTransfer(tr.ID)

	return &verifyMemEnv{
		addr:       ln.Addr().String(),
		transferID: tr.ID,
		entries:    entries,
		localPaths: localPaths,
		clientSink: clientSink,
		busy:       busy,
		ioLatency:  latency,
		ioDepth:    depth,
	}
}

// newClient requests conc data connections. PROBE applies the client pool
// minimum, as tx recv copy --concurrency conc does.
func (e *verifyMemEnv) newClient(conc int) *tx.Client {
	return tx.NewClient(e.addr, tx.WithConcurrency(conc), tx.WithEventSink(e.clientSink))
}

// run verifies every 4 MiB slot of every file once with check, on the
// workers a copy over conc connections uses.
func (e *verifyMemEnv) run(tb testing.TB, conc int, check func(context.Context, []verifyItem) error) {
	tb.Helper()
	opts := verifyOptions{pct: 100, frameSize: defaultVerifyFrameSize, seed: 1}
	files, totals := newVerifyFiles(verifyMemRoot, e.entries, opts, func(entry tx.ManifestEntry) (string, string) {
		return path.Join(verifyMemRoot, entry.Path), e.localPaths[entry.ID%uint64(len(e.localPaths))]
	})
	// As verifyCopyDataSamples sizes it.
	result, err := runVerify(verifyRun{files: files, totals: totals, workers: conc, check: check})
	if err != nil {
		tb.Fatal(err)
	}
	if result.partial || result.bytes != result.totalBytes {
		tb.Fatalf("verified %s", result.coverage())
	}
}

// BenchmarkVerifyInMemory verifies 256 MiB per op at each file size, with all
// of every file sampled. Client and server share the process CPU budget;
// cpus/2 models the server CPU count only for connection planning. At each budget:
//
//   - local hashes only the receiver's side at the planned concurrency;
//   - c=1 is the copy's real verification at requested concurrency one;
//     PROBE creates the client's minimum pool of two data connections;
//   - c=N is the same over the concurrency tx recv copy plans for a server
//     of cpus/2 CPUs.
//
// hash-busy and server-busy are the receiver's local hashing and the
// server's CXSUM handling, each summed over every worker or connection and
// divided by wall time: the average number in progress. These include IO waits,
// scheduling, parsing, and output; neither measures CPU utilization.
// core-s/GiB covers both sides.
func BenchmarkVerifyInMemory(b *testing.B) {
	total := int64(256 << 20)
	if raw := os.Getenv("TX_VERIFY_BYTES"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 16<<20 {
			b.Fatalf("TX_VERIFY_BYTES must be at least 16777216: %q", raw)
		}
		total = n
	}
	var pinnedConcurrency int
	if raw := os.Getenv("TX_VERIFY_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			b.Fatalf("invalid TX_VERIFY_CONCURRENCY %q", raw)
		}
		pinnedConcurrency = n
	}
	for _, size := range []struct {
		name  string
		bytes int64
	}{
		{"4KiB", 4 << 10},
		{"16KiB", 16 << 10},
		{"64KiB", 64 << 10},
		{"1MiB", 1 << 20},
		{"4MiB", 4 << 20},
		{"16MiB", 16 << 20},
	} {
		b.Run(size.name, func(b *testing.B) {
			env := startVerifyMemEnv(b, size.bytes, total)
			for _, cpus := range []int{2, 4, 8, 12} {
				if cpus > runtime.NumCPU() {
					continue
				}
				planned := tx.LocalSuggestedConcurrency(cpus/2, env.ioDepth)
				b.Run(fmt.Sprintf("cpus=%d", cpus), func(b *testing.B) {
					defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(cpus))
					b.Run("local", func(b *testing.B) {
						benchVerify(b, env, len(env.entries), size.bytes, func() {
							env.run(b, planned, func(ctx context.Context, items []verifyItem) error {
								_, err := hashItems(ctx, items, false)
								return err
							})
						})
					})
					concurrencies := []int{1, planned}
					if pinnedConcurrency > 0 {
						concurrencies = []int{pinnedConcurrency}
					}
					for _, conc := range concurrencies {
						b.Run(fmt.Sprintf("c=%d", conc), func(b *testing.B) {
							client := env.newClient(conc)
							defer client.Close()
							probe, err := client.ProbeLink(context.Background(), tx.ProbeRequest{ProbeBytes: 1})
							if err != nil {
								b.Fatal(err)
							}
							workers := min(conc, max(1, probe.WarmConnectionPoolSize))
							check := func(ctx context.Context, items []verifyItem) error {
								return verifyCopyItems(ctx, client, env.clientSink, env.transferID, items)
							}
							env.run(b, workers, check) // warm the connection pool
							benchVerify(b, env, len(env.entries), size.bytes, func() {
								env.run(b, workers, check)
							})
							b.ReportMetric(float64(probe.WarmConnectionPoolSize), "data-conns")
							b.ReportMetric(float64(probe.ServerIODepth), "io-depth")
						})
					}
				})
			}
		})
	}
}

// benchVerify times op, one full verification, and reports its rate, CPU,
// and how busy each side was.
func benchVerify(b *testing.B, env *verifyMemEnv, files int, size int64, op func()) {
	b.SetBytes(int64(files) * size)
	b.ReportAllocs()
	var cpuNS int64
	env.busy.reset()
	if env.ioLatency != nil {
		env.ioLatency.reset()
	}
	b.ResetTimer()
	for b.Loop() {
		cpu0 := verifyProcessCPUNS()
		op()
		cpuNS += verifyProcessCPUNS() - cpu0
	}
	wall := float64(b.Elapsed().Nanoseconds())
	gib := float64(b.N) * float64(files) * float64(size) / (1 << 30)
	b.ReportMetric(float64(cpuNS)/1e9/gib, "core-s/GiB")
	b.ReportMetric(float64(cpuNS)/wall, "cpu-cores")
	b.ReportMetric(100*float64(cpuNS)/wall/float64(runtime.GOMAXPROCS(0)), "cpu-percent")
	b.ReportMetric(float64(b.N)*float64(files)/b.Elapsed().Seconds(), "files/s")
	b.ReportMetric(float64(env.busy.hash.Load())/wall, "hash-busy")
	b.ReportMetric(float64(env.busy.server.Load())/wall, "server-busy")
	if env.ioLatency != nil {
		env.ioLatency.report(b)
	}
}

func verifyProcessCPUNS() int64 {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return ru.Utime.Nano() + ru.Stime.Nano()
}

// verifyIOLatency is a queue-free synthetic cloud SSD read-service model.
// gp3 and i4i use the Netflix service-capacity-modeling beta fit.
type verifyIOLatency struct {
	minimum, spread time.Duration
	alpha, beta     float64
	bytesPerSecond  float64
	client, server  verifyIOStats
}

type verifyIOStats struct {
	sequence, count, requested, actual atomic.Int64
	histogram                          [512]atomic.Int64 // 32 us bins; final bin includes larger waits.
}

// Fits: Netflix-Skunkworks/service-capacity-modeling@c56c0dd7861a,
// stats.py _beta_dist_from_interval; shapes/aws/{manual_drives,auto_i4i}.json.
// The larger-read bandwidth terms remain independent simulation assumptions.
func newVerifyIOLatency(tb testing.TB, profile string) *verifyIOLatency {
	switch profile {
	case "":
		return nil
	case "io2":
		return &verifyIOLatency{minimum: 100 * time.Microsecond, spread: 1400 * time.Microsecond, bytesPerSecond: 4e9}
	case "gp3":
		return &verifyIOLatency{minimum: 400 * time.Microsecond, spread: 9600 * time.Microsecond, alpha: 13.904287927818451, beta: 191.45134915996172, bytesPerSecond: 1e9}
	case "i4i", "ephem":
		return &verifyIOLatency{minimum: 50 * time.Microsecond, spread: 1950 * time.Microsecond, alpha: 17.546493426147688, beta: 438.6623356536922, bytesPerSecond: 4e9}
	default:
		tb.Fatalf("invalid TX_VERIFY_IO_LATENCY %q", profile)
		return nil
	}
}

// Integer-shape gamma sampling gives Beta(2,8) without a shared RNG lock.
func verifyBeta28(sequence uint64) float64 {
	rng := rand.NewPCG(sequence, 0x8e6f239bc12d40a7)
	var a, b float64
	for i := 0; i < 10; i++ {
		u := (float64(rng.Uint64()>>11) + .5) / (1 << 53)
		value := -math.Log(u)
		if i < 2 {
			a += value
		} else {
			b += value
		}
	}
	return a / (a + b)
}

// Marsaglia–Tsang gamma sampling; fitted profile shapes are greater than one.
func verifyGamma(rng *rand.Rand, shape float64) float64 {
	d := shape - 1.0/3
	c := 1 / math.Sqrt(9*d)
	for {
		x := rng.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := rng.Float64()
		if u < 1-.0331*x*x*x*x || math.Log(u) < .5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}

func (m *verifyIOLatency) wait(ctx context.Context, side string, bytes int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stats := &m.client
	salt := uint64(0)
	if side == "server" {
		stats = &m.server
		salt = 1 << 63
	}
	sequence := uint64(stats.sequence.Add(1)) + salt
	beta := verifyBeta28(sequence)
	if m.alpha > 0 {
		rng := rand.New(rand.NewPCG(sequence, 0x8e6f239bc12d40a7))
		a, b := verifyGamma(rng, m.alpha), verifyGamma(rng, m.beta)
		beta = a / (a + b)
	}
	delay := m.minimum + time.Duration(beta*float64(m.spread))
	// Published latency guidance is for 16 KiB IO. Larger application reads
	// add a size term; this is not an aggregate throughput or queueing limit.
	delay += time.Duration(float64(max(0, bytes-(16<<10))) * 1e9 / m.bytesPerSecond)
	start := time.Now()
	// Native blocking waits release the Go P like a blocking file read and
	// avoid rounding sub-millisecond service times to Go timer wakeups.
	for remaining := delay; remaining > 0; remaining = delay - time.Since(start) {
		if err := ctx.Err(); err != nil {
			return err
		}
		ts := unix.NsecToTimespec(int64(remaining))
		if err := unix.Nanosleep(&ts, nil); err != nil && err != unix.EINTR {
			return err
		}
	}
	actual := time.Since(start)
	stats.count.Add(1)
	stats.requested.Add(int64(delay))
	stats.actual.Add(int64(actual))
	bin := min(len(stats.histogram)-1, int(actual/(32*time.Microsecond)))
	stats.histogram[bin].Add(1)
	return ctx.Err()
}

func (m *verifyIOLatency) reset() {
	for _, s := range []*verifyIOStats{&m.client, &m.server} {
		s.count.Store(0)
		s.requested.Store(0)
		s.actual.Store(0)
		for i := range s.histogram {
			s.histogram[i].Store(0)
		}
	}
}

func (m *verifyIOLatency) report(b *testing.B) {
	for side, s := range map[string]*verifyIOStats{"client": &m.client, "server": &m.server} {
		n := max(1, s.count.Load())
		b.ReportMetric(float64(s.requested.Load())/float64(n)/1000, side+"-io-request-us")
		b.ReportMetric(float64(s.actual.Load())/float64(n)/1000, side+"-io-wait-us")
		b.ReportMetric(float64(s.count.Load())/float64(b.N), side+"-reads/op")
		var seen int64
		for i := range s.histogram {
			seen += s.histogram[i].Load()
			if seen*100 >= n*99 {
				b.ReportMetric(float64(i*32+16), side+"-io-p99-us")
				break
			}
		}
	}
}

func TestVerifyBetaLatencyModel(t *testing.T) {
	var sum float64
	for i := uint64(1); i <= 10000; i++ {
		value := verifyBeta28(i)
		if value <= 0 || value >= 1 || value != verifyBeta28(i) {
			t.Fatal("invalid or nondeterministic beta sample")
		}
		sum += value
	}
	if mean := sum / 10000; math.Abs(mean-.2) > .01 {
		t.Fatalf("mean=%f", mean)
	}
	for _, profile := range []string{"gp3", "i4i"} {
		m := newVerifyIOLatency(t, profile)
		var sum float64
		for i := uint64(1); i <= 10000; i++ {
			rng := rand.New(rand.NewPCG(i, 42))
			a, b := verifyGamma(rng, m.alpha), verifyGamma(rng, m.beta)
			sum += a / (a + b)
		}
		if math.Abs(sum/10000-m.alpha/(m.alpha+m.beta)) > .001 {
			t.Fatalf("%s mean mismatch", profile)
		}
	}
	model := newVerifyIOLatency(t, "io2")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := model.wait(ctx, "client", 16384); err != context.Canceled || model.client.count.Load() != 0 {
		t.Fatalf("err=%v", err)
	}
}

func TestVerifyMemEnvCleanup(t *testing.T) {
	before := log.Writer()
	var env *verifyMemEnv
	if !t.Run("fixture", func(t *testing.T) {
		env = startVerifyMemEnv(t, 4096, 16384)
		client := env.newClient(1)
		defer client.Close()
		if _, err := client.ProbeLink(context.Background(), tx.ProbeRequest{ProbeBytes: 1}); err != nil {
			t.Fatal(err)
		}
	}) {
		return
	}
	for _, path := range env.localPaths {
		if target, err := os.Readlink(path); err == nil && strings.Contains(target, "memfd:tx-verify-bench") {
			t.Fatalf("memfd still open: %s", path)
		}
	}
	if conn, err := net.DialTimeout("tcp", env.addr, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("listener still open")
	}
	if log.Writer() != before {
		t.Fatal("log output not restored")
	}
}
