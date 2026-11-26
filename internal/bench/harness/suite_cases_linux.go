//go:build linux

package harness

import (
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/bench/dataset"
)

const suiteMiB = 1 << 20
const suiteGiB = 1 << 30

const suiteSSDRepeats = 3

// suiteMaxHotPct is the most of an SSD source that may stay cached before a
// copy. The harness evicts every page it can, but fadvise is advisory.
const suiteMaxHotPct = 10.0

// Measurement revisions by case kind. Bump one when harness code changes what
// that kind measures or how it times it, so the report starts a new segment
// instead of showing the change as a tx regression.
const (
	suiteGoRevision   = 1
	suiteGridRevision = 1
	suiteSSDRevision  = 1
)

var suiteID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)

type suiteManifest struct {
	Version int         `json:"version"`
	Profile string      `json:"profile"`
	Cases   []suiteCase `json:"cases"`
}

type suiteCase struct {
	ID       string            `json:"id"`
	Kind     string            `json:"kind"`
	Label    string            `json:"label,omitempty"`
	Argv     []string          `json:"argv,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
	Series   []string          `json:"series,omitempty"`
	Repeats  int               `json:"repeats,omitempty"`
	Revision int               `json:"revision,omitempty"`
	// Identity holds the settings that define every series in the case, not
	// the list of series, so adding a series keeps the others' history.
	Identity map[string]any `json:"identity"`
	Bytes    int64          `json:"bytes,omitempty"`
	Recipe   *suiteRecipe   `json:"recipe,omitempty"`
	// SendBWLimit caps an SSD copy's sender with tx send tree --bwlimit, so
	// the link rather than the sender's CPU limits the copy.
	SendBWLimit string       `json:"send_bwlimit,omitempty"`
	Charts      []suiteChart `json:"charts,omitempty"`

	Samples suiteSamples      `json:"samples,omitempty"`
	Details *suiteCopyDetails `json:"details,omitempty"`

	gridSize, gridCPUs, gridModes string
}

type suiteChart struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	// Metrics maps each charted metric to the direction that counts as
	// better: "inc" or "dec".
	Metrics map[string]string `json:"metrics"`
	Prefix  string            `json:"prefix,omitempty"`
}

// suiteBetter is the direction that counts as better for each metric the
// suite charts. A chart that judges a metric the other way sets its own entry.
var suiteBetter = map[string]string{
	"MiB/s": "inc", "files/s": "inc", "comp-ratio": "inc",
	"core-s/GiB": "dec", "ratio": "dec", "allocs/op": "dec", "B/op": "dec",
	"ns/op": "dec", "ns/ack": "dec", "copy_ns": "dec", "verify_ns": "dec",
}

func chart(id, title string, metrics ...string) suiteChart {
	c := suiteChart{ID: id, Title: title, Metrics: make(map[string]string, len(metrics))}
	for _, metric := range metrics {
		c.Metrics[metric] = suiteBetter[metric]
	}
	return c
}

type suiteRecipe struct {
	Size     string `json:"size"`
	Mix      string `json:"mix"`
	Seed     int64  `json:"seed"`
	Revision int    `json:"revision"`
}

type suiteProfile struct {
	CPUs        int
	Repeats     int
	BenchTime   string
	GridSize    string
	GridCPUs    string
	GridEncrypt string
	GridMinWall string
	VerifyBytes int64
	VerifyConc  int
	// CompressedBytes is what each op of the compressed-transfer case copies.
	CompressedBytes int64
	SSD             []suiteCase
}

func ssdCase(id, label, size, mix string, bytes int64) suiteCase {
	return suiteCase{
		ID: id, Kind: "ssd", Label: label, Bytes: bytes, Repeats: suiteSSDRepeats, Revision: suiteSSDRevision,
		Recipe: &suiteRecipe{Size: size, Mix: mix, Seed: dataset.DefaultSeed, Revision: dataset.GeneratorRevision},
		Identity: map[string]any{
			"size": size, "mix": mix, "seed": dataset.DefaultSeed, "generator": dataset.GeneratorRevision,
			"bytes": bytes, "repeats": suiteSSDRepeats,
		},
		Charts: []suiteChart{
			chart("ssd-throughput", "SSD copies", "MiB/s"),
			chart("ssd-timings", "SSD copy and verification times", "copy_ns", "verify_ns"),
		},
	}
}

// suiteCompressibleBWLimit caps the compressible copy's sender. Over
// loopback, sending uncompressed is the fastest choice, so an uncapped copy
// never compresses; under this cap compression pays, and the copy measures
// the adaptive codec path.
const suiteCompressibleBWLimit = "50MiB"

// linkLimited caps a copy's sender at bwlimit and records the cap in the
// case's identity. It returns a copy and leaves c's identity unchanged.
func (c suiteCase) linkLimited(bwlimit string) suiteCase {
	c.SendBWLimit = bwlimit
	c.Identity = maps.Clone(c.Identity)
	if c.Identity == nil {
		c.Identity = map[string]any{}
	}
	c.Identity["send_bwlimit"] = bwlimit
	return c
}

func suiteSettings(profile string) suiteProfile {
	const silesiaSizes = "128MiB..1GiB"
	if profile == "smoke" {
		return suiteProfile{
			CPUs: 2, Repeats: 1, BenchTime: "1x",
			GridSize: "16MiB", GridCPUs: "2", GridEncrypt: "none", GridMinWall: "1ms",
			VerifyBytes: 16 * suiteMiB, VerifyConc: 1, CompressedBytes: 64 * suiteMiB,
			SSD: []suiteCase{
				ssdCase("ssd-small", "Small files", "16MiB", "rand=100%@16KiB", 16*suiteMiB),
				ssdCase("ssd-large", "Large files", "32MiB", "rand=100%@16MiB", 32*suiteMiB),
				ssdCase("ssd-mixed", "Mixed files", "512MiB", "rand=25%@16KiB,rand=25%@32MiB,silesia=50%@128MiB..160MiB", 512*suiteMiB),
				ssdCase("ssd-compressible", "Compressible files", "512MiB", "silesia=100%@128MiB..160MiB", 512*suiteMiB).linkLimited(suiteCompressibleBWLimit),
			},
		}
	}
	return suiteProfile{
		CPUs: 8, Repeats: 3, BenchTime: "1s",
		GridSize: "256MiB", GridCPUs: "4,16", GridEncrypt: "none,aes,chacha20", GridMinWall: "5s",
		VerifyBytes: 256 * suiteMiB, VerifyConc: 4, CompressedBytes: suiteGiB,
		SSD: []suiteCase{
			ssdCase("ssd-small", "Small files", "2GiB", "rand=100%@16KiB", 2*suiteGiB),
			ssdCase("ssd-large", "Large files", "40GiB", "rand=100%@2GiB", 40*suiteGiB),
			ssdCase("ssd-mixed", "Mixed files", "32GiB", "rand=25%@4KiB..256KiB,rand=25%@1GiB,silesia=50%@"+silesiaSizes, 32*suiteGiB),
			ssdCase("ssd-compressible", "Compressible files", "12GiB", "silesia=100%@"+silesiaSizes, 12*suiteGiB).linkLimited(suiteCompressibleBWLimit),
		},
	}
}

func (p suiteProfile) cases() []suiteCase {
	cpus, conc := strconv.Itoa(p.CPUs), strconv.Itoa(p.VerifyConc)
	bench := func(id, pkg, filter string, env map[string]string, chart suiteChart, names ...string) suiteCase {
		return p.goCase(p.CPUs, id, pkg, filter, env, chart, names...)
	}
	verifySuffix := "/cpus=" + cpus + "/c=" + conc
	gridEnv := map[string]string{"TX_BENCH_THROUGHPUT_MIN_WALL": p.GridMinWall}
	grid := suiteCase{
		ID: "transfer", Kind: "grid", Env: gridEnv, Revision: suiteGridRevision,
		Argv: []string{"make", "bench-throughput", "THROUGHPUT_SIZE=" + p.GridSize,
			"THROUGHPUT_CPUS=" + p.GridCPUs, "THROUGHPUT_ENCRYPT=" + p.GridEncrypt},
		Identity: map[string]any{"size": p.GridSize, "min_wall": p.GridMinWall},
		gridSize: p.GridSize, gridCPUs: p.GridCPUs, gridModes: p.GridEncrypt,
	}
	modeTitles := map[string]string{"none": "plaintext", "aes": "AES-GCM", "chacha20": "ChaCha20-Poly1305"}
	for _, mode := range strings.Split(p.GridEncrypt, ",") {
		page := chart("transfer-"+mode, "Transfer, "+modeTitles[mode], "MiB/s", "core-s/GiB", "ratio")
		page.Prefix = mode + "/"
		grid.Charts = append(grid.Charts, page)
	}
	compressed := p.compressedTransferCase(min(runtime.NumCPU(), runtime.GOMAXPROCS(0)))
	return append([]suiteCase{
		grid,
		compressed,
		bench("verify", "./internal/cmd/filexfercli",
			"^BenchmarkVerifyInMemory$/^(4KiB|1MiB|16MiB)$/^cpus="+cpus+"$/^c="+conc+"$",
			map[string]string{
				"TX_VERIFY_BYTES": strconv.FormatInt(p.VerifyBytes, 10), "TX_VERIFY_CONCURRENCY": conc,
				"TX_VERIFY_IO_DEPTH": "4", "TX_VERIFY_IO_LATENCY": "",
			},
			chart("verification", "Verification", "MiB/s", "core-s/GiB"),
			"BenchmarkVerifyInMemory/4KiB"+verifySuffix, "BenchmarkVerifyInMemory/1MiB"+verifySuffix,
			"BenchmarkVerifyInMemory/16MiB"+verifySuffix),
		bench("frames", "./internal/filexfer/ftcp",
			"^BenchmarkSendFramePayload$/^(zerocopy|buffered)$/^(4KiB|1MiB)$", nil,
			chart("sender-frames", "Sender frames", "MiB/s", "allocs/op"),
			"BenchmarkSendFramePayload/zerocopy/4KiB", "BenchmarkSendFramePayload/buffered/4KiB",
			"BenchmarkSendFramePayload/zerocopy/1MiB", "BenchmarkSendFramePayload/buffered/1MiB"),
		bench("cipher", "./internal/aead",
			"^Benchmark(Encrypt|Decrypt)Throughput$/^1MiB$/^(aesgcm|chacha20poly1305)$", nil,
			chart("codecs", "Ciphers and codecs", "MiB/s", "allocs/op"),
			"BenchmarkEncryptThroughput/1MiB/aesgcm", "BenchmarkEncryptThroughput/1MiB/chacha20poly1305",
			"BenchmarkDecryptThroughput/1MiB/aesgcm", "BenchmarkDecryptThroughput/1MiB/chacha20poly1305"),
		bench("handshake", "./internal/aead", "^BenchmarkHandshake$/^(token|age)$/^(aes|chacha20)$", nil,
			chart("handshake", "Handshake", "ns/op", "allocs/op"),
			"BenchmarkHandshake/token/aes", "BenchmarkHandshake/token/chacha20",
			"BenchmarkHandshake/age/aes", "BenchmarkHandshake/age/chacha20"),
		bench("codec", "./internal/bench", "^BenchmarkWrapDecompressedReader$/^(zstd|lz4)$", nil,
			chart("codecs", "Ciphers and codecs", "MiB/s", "allocs/op"),
			"BenchmarkWrapDecompressedReader/zstd", "BenchmarkWrapDecompressedReader/lz4"),
		bench("frame-codec", "./internal/bench", "^BenchmarkAppendCompressedFrame$/^(lz4|zstd)$/^(c1|auto)$", nil,
			chart("codecs", "Ciphers and codecs", "MiB/s", "allocs/op"),
			"BenchmarkAppendCompressedFrame/lz4/c1", "BenchmarkAppendCompressedFrame/lz4/auto",
			"BenchmarkAppendCompressedFrame/zstd/c1", "BenchmarkAppendCompressedFrame/zstd/auto"),
		bench("split", ".", "^BenchmarkSplitEncodedRequests$/^(short-paths|long-paths)$", nil,
			chart("control-path", "Control path", "ns/op", "B/op"),
			"BenchmarkSplitEncodedRequests/short-paths", "BenchmarkSplitEncodedRequests/long-paths"),
		bench("request", "./internal/filexfer/ftcp", "^BenchmarkRequestRecords$/^(short-paths|long-paths)$", nil,
			chart("control-path", "Control path", "ns/op", "B/op"),
			"BenchmarkRequestRecords/short-paths", "BenchmarkRequestRecords/long-paths"),
		bench("ack", "./internal/filexfer/ftcp", "^BenchmarkServePlaintextACKBody$", nil,
			chart("control-path", "Control path", "ns/op", "B/op"),
			"BenchmarkServePlaintextACKBody"),
		bench("ack-store", "./internal/bench", "^BenchmarkTransferStoreAckBatch$", nil,
			chart("control-path", "Control path", "ns/op", "B/op", "ns/ack"),
			"BenchmarkTransferStoreAckBatch"),
	}, p.SSD...)
}

// goCase runs the Go benchmarks matching filter in pkg at cpus CPUs and
// expects the named series, to which it adds the -N CPU suffix.
func (p suiteProfile) goCase(cpus int, id, pkg, filter string, env map[string]string, chart suiteChart, names ...string) suiteCase {
	n := strconv.Itoa(cpus)
	for i := range names {
		names[i] += "-" + n
	}
	return suiteCase{
		ID: id, Kind: "go", Repeats: p.Repeats, Revision: suiteGoRevision, Charts: []suiteChart{chart},
		Argv: []string{"go", "test", "-run=^$", "-cpu=" + n, "-benchmem",
			"-benchtime=" + p.BenchTime, "-count=" + strconv.Itoa(p.Repeats), "-bench=" + filter, pkg},
		Env: env, Series: names,
		Identity: map[string]any{
			"pkg": pkg, "cpu": cpus, "benchtime": p.BenchTime, "count": p.Repeats, "env": env,
		},
	}
}

// compressedTransferCase copies compressible files through an in-memory
// server and client at every CPU the suite may use, so tx plans at least one
// connection per CPU and the copy saturates whichever host runs the suite.
// The CPU count is part of the identity, so hosts of different sizes keep
// separate histories; results compare only between runs on the same host.
func (p suiteProfile) compressedTransferCase(cpus int) suiteCase {
	var series []string
	for _, comp := range []string{"lz4", "zstd", "adapt"} {
		for _, size := range []string{"1MiB", "16MiB"} {
			series = append(series, "BenchmarkTransferInMemoryCompressed/"+comp+"/"+size)
		}
	}
	for _, comp := range []string{"lz4", "zstd"} {
		series = append(series, "BenchmarkTransferInMemoryCompressedFew/"+comp)
	}
	return p.goCase(cpus, "transfer-compressed", "./internal/bench", "^BenchmarkTransferInMemoryCompressed(Few)?$",
		map[string]string{"TX_BENCH_COMPRESSED_BYTES": strconv.FormatInt(p.CompressedBytes, 10)},
		chart("transfer-compressed", "Compressed transfer", "MiB/s", "core-s/GiB", "allocs/op", "comp-ratio"),
		series...)
}

func checkSuiteIDs(cases []suiteCase) error {
	seen := make(map[string]bool, len(cases))
	for _, c := range cases {
		switch c.ID {
		case "build", "suite", "status", "environment":
			return fmt.Errorf("reserved suite case ID %q", c.ID)
		}
		if c.ID == "suite" || !suiteID.MatchString(c.ID) || seen[c.ID] {
			return fmt.Errorf("invalid or duplicate suite case ID %q", c.ID)
		}
		seen[c.ID] = true
	}
	return nil
}

func (c suiteCase) prepArgs(source string) []string {
	return []string{"prep", "--size", c.Recipe.Size, "--mix", c.Recipe.Mix,
		"--seed", strconv.FormatInt(c.Recipe.Seed, 10), "--check", source}
}

func (c suiteCase) copyArgs(tx, fingerprint, metrics, source, dest string) []string {
	args := []string{"local", "--tx", tx, "--auth", "off", "--cache-warm", "0%",
		"--require-cold-cache", strconv.FormatFloat(suiteMaxHotPct, 'g', -1, 64) + "%", "--expect", fingerprint, "--oracle", "full", "-w", "0", "-n", strconv.Itoa(c.Repeats)}
	if c.SendBWLimit != "" {
		args = append(args, "--send-arg=--bwlimit", "--send-arg="+c.SendBWLimit)
	}
	return append(args, "--metrics", metrics, source, dest)
}

func describeSuite(out io.Writer, cases []suiteCase) {
	fmt.Fprintln(out, "GOFLAGS= GOWORK=off go build -o ./tx-bench ./cmd/tx-bench")
	fmt.Fprintln(out, "GOFLAGS= GOWORK=off go build -o ./tx ./cmd/tx")
	fmt.Fprintln(out, "# Set SUITE_OUT to an empty directory for the results.")
	shellVars := strings.NewReplacer(
		"'$SOURCE'", `"$SOURCE"`, "'$DEST'", `"$DEST"`,
		"'$fingerprint'", `"$fingerprint"`,
	)
	fmt.Fprintf(out, "# Worst case %s, including the build.\n", suiteWorstCase(cases))
	for _, c := range cases {
		fmt.Fprintf(out, "\n# %s (worst case %s)\n", c.ID, c.worstCase())
		if c.Kind == "ssd" {
			fmt.Fprintln(out, "# Choose a fresh SOURCE and DEST on the same drive for this case.")
			fmt.Fprintf(out, "./tx-bench %s\n", shellVars.Replace(suiteShellArgs(c.prepArgs("$SOURCE"))))
			fmt.Fprintln(out, "fingerprint=$(jq -er .fingerprint \"$SOURCE/bench.json\")")
			metrics := "$SUITE_OUT/" + c.ID + ".json"
			args := c.copyArgs("./tx", "$fingerprint", metrics, "$SOURCE", "$DEST")
			cmd := strings.ReplaceAll(suiteShellArgs(args), shellQuote(metrics), `"`+metrics+`"`)
			fmt.Fprintf(out, "./tx-bench %s\n", shellVars.Replace(cmd))
			continue
		}
		cmd := suiteShellCommand(c.Argv, c.Env)
		if c.Kind == "grid" {
			cmd = `THROUGHPUT_OUT="$SUITE_OUT/` + c.ID + `" ` + cmd
		}
		fmt.Fprintf(out, "%s >\"$SUITE_OUT/%s.log\" 2>&1\n", cmd, c.ID)
	}
}

func suiteShellArgs(argv []string) string {
	args := make([]string, len(argv))
	for i, arg := range argv {
		args[i] = shellQuote(arg)
	}
	return strings.Join(args, " ")
}

func suiteShellCommand(argv []string, env map[string]string) string {
	assignments := make([]string, 0, len(env))
	for key, val := range env {
		assignments = append(assignments, key+"="+shellQuote(val))
	}
	sort.Strings(assignments)
	return strings.Join(append(assignments, suiteShellArgs(argv)), " ")
}

// suiteGoPackageExists reports whether the tested commit has test files in a
// relative package path such as ./internal/aead.
func suiteGoPackageExists(pkg string) bool {
	matches, _ := filepath.Glob(filepath.Join(filepath.FromSlash(pkg), "*_test.go"))
	return len(matches) > 0
}

// suiteGridExists reports whether the tested commit has the make target and
// test that the transfer grid runs.
func suiteGridExists() bool {
	makefile, err := os.ReadFile("Makefile")
	if err != nil || !strings.Contains("\n"+string(makefile), "\nbench-throughput:") {
		return false
	}
	tests, _ := filepath.Glob(filepath.Join("internal", "bench", "*_test.go"))
	for _, path := range tests {
		if body, err := os.ReadFile(path); err == nil && strings.Contains(string(body), "func TestTransferCostRatio(") {
			return true
		}
	}
	return false
}
