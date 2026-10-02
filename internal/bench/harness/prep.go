package harness

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/bench/report"
	"github.com/jolynch/tx/internal/cliflags"
)

// PrepFlags holds the options shared by prep and send-tree.
type PrepFlags struct {
	Size         string
	Fill         string
	Profile      string
	Mix          string
	Seed         int
	In           string
	Regen        bool
	Check        bool
	CacheWarm    string
	WarmSkew     string
	WarmBlock    string
	DropMeta     bool
	Jobs         int
	SilesiaCache string

	// shape records which shape flags were given; any one turns on generate.
	shape []string
}

func (p *PrepFlags) register(cf *cliflags.Flags) {
	cf.StringVar(&p.Size, "s", "size", "", "Dataset size: <bytes>|N%mem|N%disk (default: 10GiB when generating)")
	cf.StringVar(&p.Fill, "", "fill", "", "Size the dataset so its filesystem ends up N% used; replaces --size")
	cf.StringVar(&p.Profile, "", "profile", "", "Preset mix: "+strings.Join(dataset.ProfileNames, "|")+" (default: mixed when generating)")
	cf.StringVar(&p.Mix, "m", "mix", "", "Explicit mix, e.g. rand=40%@1GiB,silesia:osdb=60%@16MiB; overrides --profile")
	cf.IntVar(&p.Seed, "", "seed", dataset.DefaultSeed, "Dataset seed (default: 1 when generating)")
	cf.StringVar(&p.In, "i", "in", "", "Import this directory as the dataset, read-only and in place; excludes the shape flags above")
	cf.BoolVar(&p.Regen, "", "regen", false, "Replace an existing dataset that does not match; never touches an --in directory")
	cf.BoolVar(&p.Check, "", "check", false, "Rehash every file against files.tsv")
	cf.StringVar(&p.CacheWarm, "c", "cache-warm", "", "Page cache to set up: N% of the dataset (0% cold, 100% hot) or N%mem of RAM taken from the dataset; unset leaves the cache alone")
	cf.StringVar(&p.WarmSkew, "", "cache-warm-skew", "1", "Beta(1, skew) warm placement; 1 is uniform, larger is more contiguous")
	cf.StringVar(&p.WarmBlock, "", "cache-warm-block", "1MiB", "Warm and evict granularity")
	cf.BoolVar(&p.DropMeta, "", "cache-drop-meta", false, "Also drop cached dentries and inodes (host-wide drop_caches=2; requires root) so --cache-warm 0% is fully cold")
	cf.IntVar(&p.Jobs, "j", "jobs", 0, "Parallel generate, import, and check workers (0=CPU count)")
	cf.StringVar(&p.SilesiaCache, "", "silesia-cache", "", "Silesia corpus cache directory (default: $XDG_CACHE_HOME/tx-bench/silesia)")
}

// noteGiven records shape flags present on the command line.
func (p *PrepFlags) noteGiven(cf *cliflags.Flags) {
	shapes := map[string]string{"s": "--size", "size": "--size", "fill": "--fill", "profile": "--profile",
		"m": "--mix", "mix": "--mix", "seed": "--seed"}
	cf.Visit(func(f *flag.Flag) {
		if name, ok := shapes[f.Name]; ok {
			p.shape = append(p.shape, name)
		}
	})
}

// Generating reports whether any shape flag turned on the generate step.
func (p *PrepFlags) Generating() bool { return len(p.shape) > 0 }

func (p *PrepFlags) jobs() int {
	if p.Jobs > 0 {
		return p.Jobs
	}
	return runtime.NumCPU()
}

// validate applies the prep usage rules that do not depend on disk state.
func (p *PrepFlags) validate() (dataset.WarmSpec, error) {
	if p.In != "" && p.Generating() {
		return dataset.WarmSpec{}, usageErrorf("--in cannot be combined with %s", strings.Join(p.shape, ", "))
	}
	if p.Regen && !p.Generating() && p.In == "" {
		return dataset.WarmSpec{}, usageErrorf("--regen needs a shape flag (--size, --profile, ...) or --in")
	}
	if p.Size != "" && p.Fill != "" {
		return dataset.WarmSpec{}, usageErrorf("--fill replaces --size; give one")
	}
	w, err := dataset.ParseWarm(p.CacheWarm, p.WarmSkew, p.WarmBlock, p.DropMeta)
	if err != nil {
		return dataset.WarmSpec{}, usageErrorf("%v", err)
	}
	return w, nil
}

// PrepResult is what the startup prep produced.
type PrepResult struct {
	Dataset   *dataset.Dataset
	Generated bool
	Imported  bool
	Duration  time.Duration
	Cache     dataset.CacheResult
}

// runPrep runs every enabled step: generate or import, then check, then cache.
// Residency is always measured.
func runPrep(ctx context.Context, p *PrepFlags, warm dataset.WarmSpec, benchDir string, out io.Writer) (*PrepResult, error) {
	start := time.Now()
	logf := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
	res := &PrepResult{}
	var err error
	switch {
	case p.Generating():
		res.Dataset, res.Generated, err = prepGenerate(ctx, p, benchDir, out, logf)
	case p.In != "":
		res.Dataset, err = prepImport(ctx, p, benchDir, out, logf)
		res.Imported = true
	default:
		res.Dataset, err = prepLoad(benchDir)
		if err == nil {
			printDatasetTable(out, res.Dataset, "existing")
		}
	}
	if err != nil {
		return nil, err
	}
	if p.Check {
		if err := checkDataset(ctx, res.Dataset, p.jobs(), out); err != nil {
			return nil, err
		}
	}
	res.Cache, err = dataset.SetCache(ctx, res.Dataset, warm, seedOf(res.Dataset), p.jobs())
	if err != nil {
		return nil, err
	}
	for _, w := range res.Cache.Warnings {
		logf("warning: %s", w)
	}
	res.Duration = time.Since(start)
	return res, nil
}

func seedOf(d *dataset.Dataset) int64 {
	if d.Bench.Seed != nil {
		return *d.Bench.Seed
	}
	return dataset.DefaultSeed
}

// prepLoad loads an existing dataset as it is; its stat walk must match.
func prepLoad(benchDir string) (*dataset.Dataset, error) {
	if err := dataset.CheckIsDir(benchDir); err != nil {
		return nil, err
	}
	if _, err := os.Stat(benchDir); err == nil && !dataset.IsMarked(benchDir, dataset.BenchMarker) {
		return nil, fmt.Errorf("%s %w", benchDir, dataset.ErrUnmarked)
	}
	d, err := dataset.Load(benchDir)
	if errors.Is(err, dataset.ErrNoDataset) {
		return nil, fmt.Errorf("no dataset in %s; create one with --size 10GiB or --in DIR", benchDir)
	}
	if err != nil {
		return nil, err
	}
	if err := d.CheckStat(); err != nil {
		return nil, err
	}
	return d, nil
}

func prepGenerate(ctx context.Context, p *PrepFlags, benchDir string, out io.Writer, logf func(string, ...any)) (*dataset.Dataset, bool, error) {
	mix, err := dataset.ResolveMix(p.Profile, p.Mix)
	if err != nil {
		return nil, false, usageErrorf("%v", err)
	}
	var size dataset.SizeSpec
	switch {
	case p.Fill != "":
		size, err = dataset.ParseFill(p.Fill)
	case p.Size != "":
		size, err = dataset.ParseSize(p.Size)
	default:
		size, err = dataset.ParseSize(dataset.DefaultSize)
	}
	if err != nil {
		return nil, false, usageErrorf("%v", err)
	}
	if err := dataset.ClaimDir(benchDir, dataset.BenchMarker); err != nil {
		return nil, false, err
	}
	opts := dataset.GenerateOptions{
		BenchDir: benchDir, Seed: int64(p.Seed), Size: size, Mix: mix,
		Jobs: p.jobs(), SilesiaCache: p.SilesiaCache, Logf: logf,
	}
	existing, loadErr := dataset.Load(benchDir)
	var reclaimable int64
	if loadErr == nil && existing.Bench.Generated() {
		reclaimable = existing.Bench.Bytes
	}
	req, err := opts.Resolve(reclaimable)
	if err != nil {
		return nil, false, err
	}
	if loadErr == nil {
		diff := req.Matches(existing.Bench)
		if diff == "" {
			statErr := existing.CheckStat()
			if statErr == nil {
				printDatasetTable(out, existing, "reusing")
				return existing, false, nil
			}
			if !p.Regen {
				return nil, false, statErr
			}
		} else if !p.Regen {
			return nil, false, fmt.Errorf("existing dataset in %s differs: %s; rerun with --regen to replace it", benchDir, diff)
		}
	} else if !errors.Is(loadErr, dataset.ErrNoDataset) && !p.Regen {
		return nil, false, fmt.Errorf("%w; rerun with --regen to replace it", loadErr)
	}

	planned := &dataset.Dataset{Dir: benchDir, Bench: plannedBench(req, opts)}
	printDatasetTable(out, planned, fmt.Sprintf("generating (-j %d)", opts.Jobs))
	start := time.Now()
	b, entries, err := dataset.Generate(ctx, opts, req)
	if err != nil {
		return nil, false, err
	}
	d := &dataset.Dataset{Dir: benchDir, Bench: b, Entries: entries, Fingerprint: b.Fingerprint}
	fmt.Fprintf(out, "generated %s files, %s in %s  fingerprint %s\n",
		commas(int64(b.Files)), humanBytes(b.Bytes), roundDur(time.Since(start)), b.Fingerprint)
	return d, true, nil
}

func plannedBench(req dataset.GenerateRequest, opts dataset.GenerateOptions) *dataset.BenchJSON {
	mix := req.Mix.String()
	b := &dataset.BenchJSON{Seed: &req.Seed, SizeSpec: &opts.Size.Raw, Bytes: req.Bytes, Files: req.NumFiles(), Mix: &mix}
	for _, p := range req.Plans {
		b.Parts = append(b.Parts, dataset.PartJSON{Dir: p.Dir(), Source: p.Part.Source.String(),
			SharePct: p.Part.SharePct, Sizes: p.Part.Sizes(), Files: len(p.Sizes), Bytes: p.Budget})
	}
	return b
}

func prepImport(ctx context.Context, p *PrepFlags, benchDir string, out io.Writer, logf func(string, ...any)) (*dataset.Dataset, error) {
	in, err := filepath.Abs(p.In)
	if err != nil {
		return nil, err
	}
	if err := dataset.CheckNotNested(benchDir, in); err != nil {
		return nil, usageErrorf("%v", err)
	}
	if err := dataset.ClaimDir(benchDir, dataset.BenchMarker); err != nil {
		return nil, err
	}
	if prev, err := dataset.ReadBenchJSON(benchDir); err == nil && prev.Generated() && !p.Regen {
		return nil, fmt.Errorf("%s holds a generated dataset; rerun with --regen to replace it with an import of %s", benchDir, in)
	}
	start := time.Now()
	b, entries, err := dataset.Import(ctx, dataset.ImportOptions{BenchDir: benchDir, In: in, Jobs: p.jobs(), Logf: logf})
	if err != nil {
		return nil, err
	}
	d := &dataset.Dataset{Dir: benchDir, Bench: b, Entries: entries, Fingerprint: b.Fingerprint}
	printDatasetTable(out, d, "imported")
	fmt.Fprintf(out, "imported %s files, %s in %s  fingerprint %s\n",
		commas(int64(b.Files)), humanBytes(b.Bytes), roundDur(time.Since(start)), b.Fingerprint)
	return d, nil
}

func checkDataset(ctx context.Context, d *dataset.Dataset, jobs int, out io.Writer) error {
	res, err := dataset.Verify(ctx, d.DataRoot(), d.Entries, dataset.VerifyOptions{Content: true, Jobs: jobs})
	if err != nil {
		return err
	}
	if !res.OK() {
		var b strings.Builder
		for _, m := range res.Mismatches {
			fmt.Fprintf(&b, "\n  %s", m)
		}
		return fmt.Errorf("check: %d mismatches against files.tsv:%s", res.MismatchCount, b.String())
	}
	fmt.Fprintf(out, "check   %s files, %s rehashed in %s  ok\n", commas(int64(res.Files)), humanBytes(res.Bytes), roundDur(res.Duration))
	return nil
}

// printDatasetTable prints the prep header and one row per part.
func printDatasetTable(out io.Writer, d *dataset.Dataset, action string) {
	b := d.Bench
	if b.In != nil {
		fmt.Fprintf(out, "dataset %s  imported (read-only)  %s files  %s  zstd~%.2f  %s\n",
			b.In.Path, commas(int64(b.Files)), humanBytes(b.Bytes), b.In.ZstdRatioEst, action)
		return
	}
	shape := ""
	if b.Mix != nil {
		profile := ""
		for name, mix := range dataset.Profiles {
			if m, err := dataset.ParseMix(mix); err == nil && m.String() == *b.Mix {
				profile = name
			}
		}
		if profile != "" {
			shape = "profile=" + profile
		} else {
			shape = "mix=" + *b.Mix
		}
	}
	sizeSpec := ""
	if b.SizeSpec != nil {
		sizeSpec = *b.SizeSpec
	}
	fmt.Fprintf(out, "dataset %s  %s size=%s seed=%s  %s\n", filepath.Join(d.Dir, dataset.DataDirName), shape, sizeSpec, ptrStr(b.Seed), action)
	t := report.Table{W: out, Indent: "  ", Cols: []report.Col{
		{Name: "part", Width: 10, Left: true}, {Name: "bytes", Width: 9}, {Name: "files", Width: 11},
		{Name: "sizes", Width: 14, Left: true}, {Name: "corpus", Width: 6, Left: true},
	}}
	t.Header()
	for _, p := range b.Parts {
		corpus := ""
		if src, names, ok := strings.Cut(p.Source, ":"); ok && src == dataset.SourceSilesia {
			corpus = names
		}
		sizes := p.Sizes
		if p.Files == 1 && !strings.Contains(sizes, "..") {
			sizes = humanBytes(p.Bytes) // a part smaller than one file
		}
		t.Row(p.Dir, humanBytes(p.Bytes), commas(int64(p.Files)), sizes, corpus)
	}
}

func ptrStr[T any](p *T) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprint(*p)
}

// RunPrepCLI is 'tx-bench prep'.
func RunPrepCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cf := cliflags.New("prep")
	cf.SetOutput(stderr)
	var p PrepFlags
	p.register(cf)
	cf.FlagSet().Usage = func() {
		fmt.Fprint(stderr, `usage: tx-bench prep [options] [BENCH_DIR]

Prepare a benchmark dataset: generate or import it, check it, and set its
page-cache state. Each step runs only when its options are given.

  BENCH_DIR    bench root; generated data lives in BENCH_DIR/data
               (default ./tx-bench-src)

Steps, in order:
  generate   any of --size, --fill, --profile, --mix, --seed
  import     --in DIR (read-only, in place); excludes generate
  check      --check
  cache      --cache-warm

Without shape flags or --in, the existing dataset is used as is.

`)
		cf.PrintDefaults(stderr)
	}
	if code, done := parseFlags(cf, args); done {
		return code
	}
	p.noteGiven(cf)
	benchDir, err := positionalDir(cf.Args(), defaultBenchDir, "BENCH_DIR")
	if err != nil {
		return reportErr(stderr, err)
	}
	warm, err := p.validate()
	if err != nil {
		return reportErr(stderr, err)
	}
	res, err := runPrep(ctx, &p, warm, benchDir, stdout)
	if err != nil {
		return reportErr(stderr, err)
	}
	fmt.Fprintf(stdout, "fingerprint %s  cache-warm %s (hot %.1f%%)  prep %s\n",
		res.Dataset.Fingerprint, warmLabel(warm), res.Cache.HotPct, roundDur(res.Duration))
	fmt.Fprintf(stdout, "\nServe it with:\n  %s\n", sendTreeCommand(benchDir))
	return 0
}

func warmLabel(w dataset.WarmSpec) string {
	if w.Unset {
		return "unset"
	}
	label := w.Raw
	if w.Skew != 1 {
		label += fmt.Sprintf(" skew=%g", w.Skew)
	}
	return label
}

func sendTreeCommand(benchDir string) string {
	cmd := selfName() + " remote send-tree"
	if rel, err := filepath.Rel(mustCwd(), benchDir); err == nil && rel != defaultBenchDir && "./"+rel != defaultBenchDir {
		cmd += " " + shellQuote(benchDir)
	}
	return cmd
}
