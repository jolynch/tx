package main

import (
	"archive/zip"
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"math/rand"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

const defaultSilesiaArchiveURL = "http://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip"

var (
	silesiaArchiveURL           = defaultSilesiaArchiveURL
	silesiaCacheDir             = filepath.Join("bench", "data", "silesia")
	silesiaHTTPClient           = http.DefaultClient
	generateLogOutput io.Writer = os.Stderr
)

var allowedSilesiaNames = []string{
	"dickens",
	"mozilla",
	"mr",
	"nci",
	"ooffice",
	"osdb",
	"reymont",
	"samba",
	"sao",
	"webster",
	"xml",
	"x-ray",
}

type genSpec struct {
	outdir string
	count  int
	size   int64
}

type generateConfig struct {
	source sourceSpec
	specs  []genSpec
}

type sourceSpec struct {
	kind  string
	names []string
}

type generateSource interface {
	generateSpec(genSpec) error
}

type randSource struct{}

type silesiaSource struct {
	names []string
	files map[string][]byte
}

type downloadProgressWriter struct {
	label        string
	total        int64
	written      int64
	nextReport   int64
	reportEvery  int64
	startedAt    time.Time
	lastReported int64
}

func printGenerateUsage(w io.Writer) {
	fmt.Fprintf(w, `usage: bench generate [-source rand|silesia:<csv>] <spec> [<spec>...]

Generate benchmark files into one or more output directories.

Options:
  -source string
      Data source for generated files.
      rand                deterministic random bytes (default)
      silesia:<csv>       repeat/truncate named Silesia corpus files

Specs:
  <outdir>:<count>@<size>

Examples:
  bench generate bench/data/src:100@10MiB
  bench generate -source rand bench/data/src:81920@64KiB
  bench generate -source silesia:osdb bench/data/src:10@100MiB
  bench generate -source silesia:osdb,nci bench/data/src:10@100MiB

Silesia corpus names:
  %s
`, strings.Join(allowedSilesiaNames, ", "))
}

func parseGenSpec(s string) (genSpec, error) {
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return genSpec{}, fmt.Errorf("invalid spec %q: missing '@'", s)
	}
	lhs, sizeStr := s[:at], s[at+1:]
	colon := strings.LastIndex(lhs, ":")
	if colon < 0 {
		return genSpec{}, fmt.Errorf("invalid spec %q: missing ':'", s)
	}
	outdir, countStr := lhs[:colon], lhs[colon+1:]
	if outdir == "" {
		return genSpec{}, fmt.Errorf("invalid spec %q: empty outdir", s)
	}
	count, err := strconv.Atoi(countStr)
	if err != nil || count <= 0 {
		return genSpec{}, fmt.Errorf("invalid count %q in spec %q", countStr, s)
	}
	size, err := encoding.ParseByteSize(sizeStr)
	if err != nil {
		return genSpec{}, fmt.Errorf("invalid size %q in spec %q: %w", sizeStr, s, err)
	}
	return genSpec{outdir: outdir, count: count, size: size}, nil
}

func parseGenerateConfig(args []string) (generateConfig, error) {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	sourceRaw := fs.String("source", "rand", "")
	if err := fs.Parse(args); err != nil {
		return generateConfig{}, err
	}
	if len(fs.Args()) == 0 {
		return generateConfig{}, errors.New("requires at least one spec of form <outdir>:<count>@<size>")
	}
	source, err := parseSourceSpec(*sourceRaw)
	if err != nil {
		return generateConfig{}, err
	}
	specs := make([]genSpec, 0, len(fs.Args()))
	for _, a := range fs.Args() {
		spec, err := parseGenSpec(a)
		if err != nil {
			return generateConfig{}, err
		}
		specs = append(specs, spec)
	}
	return generateConfig{source: source, specs: specs}, nil
}

func parseSourceSpec(raw string) (sourceSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "rand" {
		return sourceSpec{kind: "rand"}, nil
	}
	if !strings.HasPrefix(raw, "silesia:") {
		return sourceSpec{}, fmt.Errorf("unsupported source %q; supported sources: rand or silesia:<csv>", raw)
	}
	csv := strings.TrimSpace(strings.TrimPrefix(raw, "silesia:"))
	if csv == "" {
		return sourceSpec{}, fmt.Errorf("invalid source %q: missing Silesia corpus names", raw)
	}
	names := make([]string, 0, len(strings.Split(csv, ",")))
	for _, part := range strings.Split(csv, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			return sourceSpec{}, fmt.Errorf("invalid source %q: empty Silesia corpus name", raw)
		}
		if !slices.Contains(allowedSilesiaNames, name) {
			return sourceSpec{}, fmt.Errorf("unknown Silesia corpus %q; allowed values: %s", name, strings.Join(allowedSilesiaNames, ", "))
		}
		names = append(names, name)
	}
	return sourceSpec{kind: "silesia", names: names}, nil
}

func runGenerate(args []string) error {
	cfg, err := parseGenerateConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printGenerateUsage(os.Stderr)
			return nil
		}
		return err
	}
	logGenerate("source=%s specs=%d", formatSourceSpec(cfg.source), len(cfg.specs))
	source, err := newGenerateSource(cfg.source)
	if err != nil {
		return err
	}
	for _, spec := range cfg.specs {
		if err := source.generateSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

func newGenerateSource(spec sourceSpec) (generateSource, error) {
	switch spec.kind {
	case "rand":
		return randSource{}, nil
	case "silesia":
		files, err := loadSilesiaFiles(spec.names)
		if err != nil {
			return nil, err
		}
		return &silesiaSource{names: spec.names, files: files}, nil
	default:
		return nil, fmt.Errorf("unsupported generate source kind %q", spec.kind)
	}
}

func (randSource) generateSpec(s genSpec) error {
	if err := ensureBenchDirs(s.outdir); err != nil {
		return err
	}
	logGenerate("generating %d file(s) of %s into %s using source=rand", s.count, encoding.HumanBytes(s.size), s.outdir)
	pad := len(strconv.Itoa(s.count))
	rng := rand.New(rand.NewSource(1))
	buf := make([]byte, 1<<20)
	progressEvery := generateProgressEvery(s.count)
	for i := 0; i < s.count; i++ {
		name := fmt.Sprintf("file-%0*d.bin", pad, i)
		path := filepath.Join(s.outdir, name)
		if err := writeRandom(path, s.size, buf, rng); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		logGenerationProgress(i+1, s.count, progressEvery, path)
	}
	fmt.Printf("generated %d files of %s in %s\n",
		s.count, encoding.HumanBytes(s.size), s.outdir)
	return nil
}

func (s *silesiaSource) generateSpec(spec genSpec) error {
	if err := ensureBenchDirs(spec.outdir); err != nil {
		return err
	}
	logGenerate("generating %d file(s) of %s into %s using source=silesia:%s", spec.count, encoding.HumanBytes(spec.size), spec.outdir, strings.Join(s.names, ","))
	pad := len(strconv.Itoa(spec.count))
	progressEvery := generateProgressEvery(spec.count)
	sourceCounts := make(map[string]int, len(s.names))
	for i := 0; i < spec.count; i++ {
		sourceName := s.names[i%len(s.names)]
		sourceBytes := s.files[sourceName]
		if len(sourceBytes) == 0 && spec.size > 0 {
			return fmt.Errorf("silesia corpus %q is empty", sourceName)
		}
		sourceIndex := sourceCounts[sourceName]
		sourceCounts[sourceName] = sourceIndex + 1
		name := fmt.Sprintf("%s-%0*d.bin", sourceName, pad, sourceIndex)
		path := filepath.Join(spec.outdir, name)
		if err := writeRepeatedBytes(path, spec.size, sourceBytes); err != nil {
			return fmt.Errorf("write %s from %s: %w", path, sourceName, err)
		}
		logGenerationProgress(i+1, spec.count, progressEvery, fmt.Sprintf("%s (from %s)", path, sourceName))
	}
	fmt.Printf("generated %d files of %s in %s using source=%s\n",
		spec.count, encoding.HumanBytes(spec.size), spec.outdir, strings.Join(s.names, ","))
	return nil
}

func ensureBenchDirs(outdir string) error {
	if err := os.MkdirAll(outdir, 0o755); err != nil {
		return err
	}
	if parent := filepath.Dir(outdir); parent != "." && parent != "" {
		_ = os.MkdirAll(filepath.Join(parent, "dst"), 0o755)
	}
	return nil
}

func resolveSilesiaCacheDir() string {
	if filepath.IsAbs(silesiaCacheDir) {
		return silesiaCacheDir
	}
	cleaned := filepath.Clean(silesiaCacheDir)
	cwd, err := os.Getwd()
	if err != nil {
		return cleaned
	}
	if filepath.Base(cwd) != "bench" {
		return cleaned
	}
	prefix := "bench" + string(filepath.Separator)
	if strings.HasPrefix(cleaned, prefix) {
		return strings.TrimPrefix(cleaned, prefix)
	}
	return cleaned
}

func loadSilesiaFiles(names []string) (map[string][]byte, error) {
	cacheDir := resolveSilesiaCacheDir()
	logGenerate("using silesia cache dir: %s", cacheDir)
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create silesia cache dir: %w", err)
	}
	unique := uniqueStrings(names)
	files := make(map[string][]byte, len(unique))
	missing := make([]string, 0, len(unique))
	for _, name := range unique {
		path := filepath.Join(cacheDir, name)
		data, err := os.ReadFile(path)
		if err == nil {
			logGenerate("reusing cached silesia corpus %s (%s)", name, encoding.HumanBytes(int64(len(data))))
			files[name] = data
			continue
		}
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read cached silesia corpus %s: %w", name, err)
		}
		missing = append(missing, name)
	}
	if len(missing) > 0 {
		logGenerate("missing silesia corpus file(s): %s", strings.Join(missing, ", "))
		if err := fetchMissingSilesiaFiles(missing); err != nil {
			return nil, err
		}
		for _, name := range missing {
			data, err := os.ReadFile(filepath.Join(cacheDir, name))
			if err != nil {
				return nil, fmt.Errorf("read downloaded silesia corpus %s: %w", name, err)
			}
			files[name] = data
		}
	}
	return files, nil
}

func fetchMissingSilesiaFiles(names []string) error {
	logGenerate("downloading silesia archive from %s", silesiaArchiveURL)
	resp, err := silesiaHTTPClient.Get(silesiaArchiveURL)
	if err != nil {
		return fmt.Errorf("download silesia archive: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download silesia archive: unexpected status %s", resp.Status)
	}
	tmp, err := os.CreateTemp("", "silesia-*.zip")
	if err != nil {
		return fmt.Errorf("create temp silesia archive: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	progress := newDownloadProgressWriter("downloaded silesia archive", resp.ContentLength)
	if _, err := io.Copy(tmp, io.TeeReader(resp.Body, progress)); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp silesia archive: %w", err)
	}
	progress.Finish()
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp silesia archive: %w", err)
	}
	return extractSilesiaFiles(tmpPath, resolveSilesiaCacheDir(), names)
}

func extractSilesiaFiles(zipPath string, cacheDir string, names []string) error {
	archive, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("open silesia archive: %w", err)
	}
	defer archive.Close()
	requested := make(map[string]struct{}, len(names))
	for _, name := range names {
		requested[name] = struct{}{}
	}
	found := make(map[string]bool, len(names))
	for _, file := range archive.File {
		if file.FileInfo().IsDir() {
			continue
		}
		base := path.Base(file.Name)
		if _, ok := requested[base]; !ok || found[base] {
			continue
		}
		logGenerate("extracting silesia corpus %s to %s", base, filepath.Join(cacheDir, base))
		if err := extractZipFile(file, filepath.Join(cacheDir, base)); err != nil {
			return fmt.Errorf("extract silesia corpus %s: %w", base, err)
		}
		found[base] = true
	}
	missing := make([]string, 0, len(requested))
	for _, name := range names {
		if !found[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("silesia archive missing corpus file(s): %s", strings.Join(uniqueStrings(missing), ", "))
	}
	return nil
}

func extractZipFile(file *zip.File, dstPath string) error {
	rc, err := file.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, rc); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func formatSourceSpec(spec sourceSpec) string {
	switch spec.kind {
	case "rand":
		return "rand"
	case "silesia":
		return "silesia:" + strings.Join(spec.names, ",")
	default:
		return spec.kind
	}
}

func logGenerate(format string, args ...any) {
	if generateLogOutput == nil {
		return
	}
	fmt.Fprintf(generateLogOutput, "generate: "+format+"\n", args...)
}

func generateProgressEvery(count int) int {
	switch {
	case count <= 10:
		return 1
	case count <= 100:
		return 10
	case count <= 1000:
		return 100
	default:
		return 1000
	}
}

func logGenerationProgress(done int, total int, every int, path string) {
	if every <= 0 {
		return
	}
	if done != total && done%every != 0 {
		return
	}
	logGenerate("progress %d/%d files: %s", done, total, path)
}

func newDownloadProgressWriter(label string, total int64) *downloadProgressWriter {
	const reportEvery = 8 << 20
	return &downloadProgressWriter{
		label:       label,
		total:       total,
		reportEvery: reportEvery,
		nextReport:  reportEvery,
		startedAt:   time.Now(),
	}
}

func (w *downloadProgressWriter) Write(p []byte) (int, error) {
	n := len(p)
	w.written += int64(n)
	if w.written >= w.nextReport {
		w.report()
		for w.nextReport <= w.written {
			w.nextReport += w.reportEvery
		}
	}
	return n, nil
}

func (w *downloadProgressWriter) Finish() {
	if w.written == 0 {
		logGenerate("%s: 0 B", w.label)
		return
	}
	if w.lastReported != w.written {
		w.report()
	}
}

func (w *downloadProgressWriter) report() {
	w.lastReported = w.written
	elapsed := time.Since(w.startedAt)
	rate := "n/a"
	if elapsed > 0 {
		rate = encoding.HumanBytes(int64(float64(w.written)/elapsed.Seconds())) + "/s"
	}
	if w.total > 0 {
		pct := float64(w.written) * 100 / float64(w.total)
		logGenerate("%s: %s / %s (%.1f%%, %s)", w.label, encoding.HumanBytes(w.written), encoding.HumanBytes(w.total), pct, rate)
		return
	}
	logGenerate("%s: %s (%s)", w.label, encoding.HumanBytes(w.written), rate)
}

func writeRandom(path string, size int64, buf []byte, rng *rand.Rand) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	remaining := size
	for remaining > 0 {
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		_, _ = rng.Read(buf[:chunk])
		if _, err := w.Write(buf[:chunk]); err != nil {
			f.Close()
			return err
		}
		remaining -= chunk
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func writeRepeatedBytes(path string, size int64, src []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	remaining := size
	for remaining > 0 {
		chunk := int64(len(src))
		if remaining < chunk {
			chunk = remaining
		}
		if _, err := w.Write(src[:chunk]); err != nil {
			f.Close()
			return err
		}
		remaining -= chunk
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
