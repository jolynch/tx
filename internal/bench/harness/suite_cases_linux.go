//go:build linux

package harness

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/bench/dataset"
)

const suiteMiB = 1 << 20
const suiteGiB = 1 << 30

type suiteManifest struct {
	Version int         `json:"version"`
	Profile string      `json:"profile"`
	Cases   []suiteCase `json:"cases"`
}

type suiteCase struct {
	ID     string             `json:"id"`
	Kind   string             `json:"kind"`
	Argv   []string           `json:"argv,omitempty"`
	Env    *map[string]string `json:"env,omitempty"`
	Series []string           `json:"series,omitempty"`
	Bytes  int64              `json:"bytes,omitempty"`
	Recipe *suiteRecipe       `json:"recipe,omitempty"`
	Flags  []string           `json:"flags,omitempty"`

	Files         int     `json:"files,omitempty"`
	MeanFileBytes float64 `json:"mean_file_bytes,omitempty"`
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
	SSD         []suiteCase
}

func ssdCase(id, size, mix string, bytes int64) suiteCase {
	return suiteCase{
		ID: id, Kind: "ssd", Bytes: bytes,
		Recipe: &suiteRecipe{Size: size, Mix: mix, Seed: dataset.DefaultSeed, Revision: dataset.GeneratorRevision},
		Flags:  []string{"--compress", "none", "--encrypt", "none"},
	}
}

func suiteSettings(profile string) suiteProfile {
	if profile == "smoke" {
		return suiteProfile{
			CPUs: 2, Repeats: 1, BenchTime: "1x",
			GridSize: "16MiB", GridCPUs: "2", GridEncrypt: "none", GridMinWall: "1ms",
			VerifyBytes: 16 * suiteMiB, VerifyConc: 1,
			SSD: []suiteCase{
				ssdCase("ssd-small", "16MiB", "rand=100%@16KiB", 16*suiteMiB),
				ssdCase("ssd-varied", "32MiB", "rand=100%@128KiB..1324KiB", 32*suiteMiB),
				ssdCase("ssd-large", "32MiB", "rand=100%@16MiB", 32*suiteMiB),
			},
		}
	}
	return suiteProfile{
		CPUs: 8, Repeats: 3, BenchTime: "1s",
		GridSize: "256MiB", GridCPUs: "4,16", GridEncrypt: "none,aes,chacha20", GridMinWall: "5s",
		VerifyBytes: 256 * suiteMiB, VerifyConc: 4,
		SSD: []suiteCase{
			ssdCase("ssd-small", "2GiB", "rand=100%@16KiB", 2*suiteGiB),
			ssdCase("ssd-varied", "128GiB", "rand=100%@128MiB..1324MiB", 128*suiteGiB),
			ssdCase("ssd-large", "128GiB", "rand=100%@2GiB", 128*suiteGiB),
		},
	}
}

func (p suiteProfile) cases() []suiteCase {
	cpus, conc := strconv.Itoa(p.CPUs), strconv.Itoa(p.VerifyConc)
	bench := func(id, pkg, filter string, env map[string]string, names ...string) suiteCase {
		if env == nil {
			env = map[string]string{}
		}
		for i := range names {
			names[i] += "-" + cpus
		}
		return suiteCase{
			ID: id, Kind: "go",
			Argv: []string{"go", "test", "-run=^$", "-cpu=" + cpus, "-benchmem",
				"-benchtime=" + p.BenchTime, "-count=" + strconv.Itoa(p.Repeats), "-bench=" + filter, pkg},
			Env: &env, Series: names,
		}
	}
	verifySuffix := "/cpus=" + cpus + "/c=" + conc
	gridEnv := map[string]string{"TX_BENCH_THROUGHPUT_MIN_WALL": p.GridMinWall}
	return append([]suiteCase{
		{ID: "transfer", Kind: "grid", Env: &gridEnv, Argv: []string{
			"make", "bench-throughput", "THROUGHPUT_SIZE=" + p.GridSize,
			"THROUGHPUT_CPUS=" + p.GridCPUs, "THROUGHPUT_ENCRYPT=" + p.GridEncrypt,
		}},
		bench("verify", "./internal/cmd/filexfercli",
			"^BenchmarkVerifyInMemory$/^(4KiB|1MiB|16MiB)$/^cpus="+cpus+"$/^c="+conc+"$",
			map[string]string{
				"TX_VERIFY_BYTES": strconv.FormatInt(p.VerifyBytes, 10), "TX_VERIFY_CONCURRENCY": conc,
				"TX_VERIFY_IO_DEPTH": "4", "TX_VERIFY_IO_LATENCY": "",
			},
			"BenchmarkVerifyInMemory/4KiB"+verifySuffix, "BenchmarkVerifyInMemory/1MiB"+verifySuffix,
			"BenchmarkVerifyInMemory/16MiB"+verifySuffix),
		bench("frames", "./internal/filexfer/ftcp",
			"^BenchmarkSendFramePayload$/^(zerocopy|buffered)$/^(4KiB|1MiB)$", nil,
			"BenchmarkSendFramePayload/zerocopy/4KiB", "BenchmarkSendFramePayload/buffered/4KiB",
			"BenchmarkSendFramePayload/zerocopy/1MiB", "BenchmarkSendFramePayload/buffered/1MiB"),
		bench("cipher", "./internal/aead",
			"^Benchmark(Encrypt|Decrypt)Throughput$/^1MiB$/^(aesgcm|chacha20poly1305)$", nil,
			"BenchmarkEncryptThroughput/1MiB/aesgcm", "BenchmarkEncryptThroughput/1MiB/chacha20poly1305",
			"BenchmarkDecryptThroughput/1MiB/aesgcm", "BenchmarkDecryptThroughput/1MiB/chacha20poly1305"),
		bench("handshake", "./internal/aead", "^BenchmarkHandshake$/^(token|age)$/^(aes|chacha20)$", nil,
			"BenchmarkHandshake/token/aes", "BenchmarkHandshake/token/chacha20",
			"BenchmarkHandshake/age/aes", "BenchmarkHandshake/age/chacha20"),
		bench("codec", "./internal/bench", "^BenchmarkWrapDecompressedReader$/^(zstd|lz4)$", nil,
			"BenchmarkWrapDecompressedReader/zstd", "BenchmarkWrapDecompressedReader/lz4"),
		bench("split", ".", "^BenchmarkSplitEncodedRequests$/^(short-paths|long-paths)$", nil,
			"BenchmarkSplitEncodedRequests/short-paths", "BenchmarkSplitEncodedRequests/long-paths"),
		bench("request", "./internal/filexfer/ftcp", "^BenchmarkRequestRecords$/^(short-paths|long-paths)$", nil,
			"BenchmarkRequestRecords/short-paths", "BenchmarkRequestRecords/long-paths"),
		bench("ack", "./internal/filexfer/ftcp", "^BenchmarkServePlaintextACKBody$", nil,
			"BenchmarkServePlaintextACKBody"),
		bench("ack-store", "./internal/bench", "^BenchmarkTransferStoreAckBatch$", nil,
			"BenchmarkTransferStoreAckBatch"),
	}, p.SSD...)
}

func (c suiteCase) prepArgs(source string) []string {
	return []string{"prep", "--size", c.Recipe.Size, "--mix", c.Recipe.Mix,
		"--seed", strconv.FormatInt(c.Recipe.Seed, 10), "--check", source}
}

func (c suiteCase) copyArgs(tx, fingerprint, metrics, source, dest string) []string {
	return append([]string{"local", "--tx", tx, "--auth", "off", "--cache-warm", "0%",
		"--expect", fingerprint, "--oracle", "full", "-w", "0", "-n", "1",
		"--metrics", metrics, source, dest, "--"}, c.Flags...)
}

func describeSuite(out io.Writer, cases []suiteCase) {
	fmt.Fprintln(out, "GOFLAGS= GOWORK=off go build -o ./tx-bench ./cmd/tx-bench")
	fmt.Fprintln(out, "GOFLAGS= GOWORK=off go build -o ./tx ./cmd/tx")
	fmt.Fprintln(out, "# Set SUITE_OUT to an empty directory for the results.")
	shellVars := strings.NewReplacer(
		"'$SOURCE'", `"$SOURCE"`, "'$DEST'", `"$DEST"`,
		"'$fingerprint'", `"$fingerprint"`,
	)
	for _, c := range cases {
		fmt.Fprintf(out, "\n# %s\n", c.ID)
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
		cmd := suiteShellCommand(c.Argv, *c.Env)
		if c.Kind == "grid" {
			cmd = `THROUGHPUT_OUT="$SUITE_OUT" ` + cmd
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

// checkSuiteBenchmarks reads go test output after a successful run. A series
// with every repetition is present; one with none is missing from the tested
// commit, since go test exits cleanly when a -bench filter matches nothing.
// A partial or unexpected series is an error.
func checkSuiteBenchmarks(path string, series []string, repeats int) (missing []string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	want := make(map[string]int, len(series))
	for _, name := range series {
		want[name] = repeats
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 || len(fields)%2 != 0 {
			return nil, fmt.Errorf("malformed benchmark result %q", line)
		}
		iterations, err := strconv.Atoi(fields[1])
		if err != nil || iterations < 1 {
			return nil, fmt.Errorf("invalid benchmark iteration count in %q", line)
		}
		name := fields[0]
		if want[name] == 0 {
			return nil, fmt.Errorf("unexpected benchmark result %s", name)
		}
		want[name]--
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, name := range series {
		switch left := want[name]; left {
		case 0:
		case repeats:
			missing = append(missing, name)
		default:
			return nil, fmt.Errorf("%s is missing %d of %d samples", name, left, repeats)
		}
	}
	return missing, nil
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
