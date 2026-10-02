package dataset

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/bits"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/jolynch/tx/internal/utils"
)

// zstdSampleBytes bounds the sample an import compresses to estimate the
// dataset's compressibility.
const zstdSampleBytes = 64 << 20

// ImportOptions describes a user-supplied dataset.
type ImportOptions struct {
	BenchDir string
	In       string
	Jobs     int
	Logf     func(string, ...any)
}

// Chroot returns the deepest directory containing both a and b, which tx
// send tree must serve so both trees are reachable.
func Chroot(a, b string) string {
	a, b = filepath.Clean(a), filepath.Clean(b)
	for {
		if utils.PathWithinRoot(a, b) {
			return a
		}
		parent := filepath.Dir(a)
		if parent == a {
			return a
		}
		a = parent
	}
}

// RemotePath is p as seen by a tx send tree chrooted at root.
func RemotePath(root, p string) string {
	rel, err := filepath.Rel(root, p)
	if err != nil || rel == "." {
		return "/"
	}
	return "/" + filepath.ToSlash(rel)
}

// CheckNotNested rejects a BENCH_DIR and --in DIR nested in either direction.
func CheckNotNested(benchDir, in string) error {
	benchDir, in = filepath.Clean(benchDir), filepath.Clean(in)
	if utils.PathWithinRoot(in, benchDir) || utils.PathWithinRoot(benchDir, in) {
		return fmt.Errorf("BENCH_DIR %s and --in %s may not be nested inside each other; use a sibling BENCH_DIR such as %s",
			benchDir, in, filepath.Join(filepath.Dir(in), filepath.Base(in)+"-tx-bench"))
	}
	return nil
}

// Import hashes opts.In into files.tsv, in-cache.tsv, and bench.json. Files
// whose (dev, inode, size, mtime, ctime) match the previous import of the
// same directory keep their hash; everything else is rehashed. Nothing under
// opts.In is written.
func Import(ctx context.Context, opts ImportOptions) (*BenchJSON, []Entry, error) {
	logf := opts.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	info, err := os.Stat(opts.In)
	if err != nil {
		return nil, nil, fmt.Errorf("--in: %w", err)
	}
	if !info.IsDir() {
		return nil, nil, fmt.Errorf("--in %s is not a directory", opts.In)
	}
	if err := CheckNotNested(opts.BenchDir, opts.In); err != nil {
		return nil, nil, err
	}
	walked, err := Walk(opts.In)
	if err != nil {
		return nil, nil, err
	}
	entries := Entries(walked)

	// Reuse hashes from the previous import of the same directory.
	prevHash := map[string]string{}
	if prev, err := ReadBenchJSON(opts.BenchDir); err == nil && prev.In != nil && prev.In.Path == opts.In {
		cache, _ := readInCache(filepath.Join(opts.BenchDir, InCacheName))
		old, _, _ := ReadFilesTSVFile(filepath.Join(opts.BenchDir, FilesTSVName))
		oldByPath := make(map[string]Entry, len(old))
		for _, e := range old {
			oldByPath[e.Path] = e
		}
		for _, w := range walked {
			if w.Type != TypeFile {
				continue
			}
			if c, ok := cache[w.Path]; ok && c == cacheKeyOf(w) {
				if o, ok := oldByPath[w.Path]; ok && o.Type == TypeFile {
					prevHash[w.Path] = o.Hash
				}
			}
		}
	}
	need := 0
	var needBytes int64
	for i := range entries {
		if entries[i].Type != TypeFile {
			continue
		}
		if h, ok := prevHash[entries[i].Path]; ok {
			entries[i].Hash = h
			continue
		}
		need++
		needBytes += entries[i].Size
	}
	start := time.Now()
	logf("import: hashing %d of %d files", need, countFiles(entries))
	if err := HashEntries(ctx, opts.In, entries, func(i int) bool { return entries[i].Hash == "" }, opts.Jobs, nil); err != nil {
		return nil, nil, err
	}
	logf("import: hashed %d files in %s", need, time.Since(start).Round(time.Millisecond))

	files, total := Totals(entries)
	ratio, err := estimateZstdRatio(opts.In, entries)
	if err != nil {
		return nil, nil, err
	}
	chroot := Chroot(opts.BenchDir, opts.In)
	b := &BenchJSON{
		Version: benchJSONVer,
		Bytes:   total,
		Files:   files,
		In: &InJSON{
			Path:          opts.In,
			SizeHistogram: sizeHistogram(entries),
			ZstdRatioEst:  ratio,
		},
		Remote:  Remote{Bench: RemotePath(chroot, opts.BenchDir), Data: RemotePath(chroot, opts.In)},
		Created: time.Now().UTC().Format(time.RFC3339),
	}
	// Generated data never coexists with an import.
	if err := os.RemoveAll(filepath.Join(opts.BenchDir, DataDirName)); err != nil {
		return nil, nil, err
	}
	if err := os.Remove(filepath.Join(opts.BenchDir, BenchJSONName)); err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	if err := writeInCache(filepath.Join(opts.BenchDir, InCacheName), walked); err != nil {
		return nil, nil, err
	}
	if err := WriteDataset(opts.BenchDir, b, entries); err != nil {
		return nil, nil, err
	}
	return b, entries, nil
}

func countFiles(entries []Entry) int {
	n, _ := Totals(entries)
	return n
}

// cacheKey is the identity in-cache.tsv records for each entry. ctime is
// included because touch -r and rsync -t preserve mtime but cannot set ctime.
type cacheKey struct {
	Type    byte
	Dev     uint64
	Ino     uint64
	Size    int64
	MtimeNS int64
	CtimeNS int64
}

func cacheKeyOf(w WalkedEntry) cacheKey {
	t := w.Type
	if t == TypeHardlink {
		t = TypeFile
	}
	return cacheKey{Type: t, Dev: w.Dev, Ino: w.Ino, Size: w.Size, MtimeNS: w.MtimeNS, CtimeNS: w.CtimeNS}
}

func writeInCache(path string, walked []WalkedEntry) error {
	var buf bytes.Buffer
	for _, w := range walked {
		k := cacheKeyOf(w)
		fmt.Fprintf(&buf, "%c\t%d\t%d\t%d\t%d\t%d\t%s\n", k.Type, k.Dev, k.Ino, k.Size, k.MtimeNS, k.CtimeNS, w.Path)
	}
	return WriteFileAtomic(path, buf.Bytes())
}

func readInCache(path string) (map[string]cacheKey, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]cacheKey{}
	br := bufio.NewReaderSize(f, 1<<20)
	for {
		line, err := br.ReadString('\n')
		if line == "" && err == io.EOF {
			return out, nil
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		fields := strings.SplitN(strings.TrimSuffix(line, "\n"), "\t", 7)
		if len(fields) != 7 || len(fields[0]) != 1 {
			return nil, fmt.Errorf("%s: malformed line %q", path, line)
		}
		var k cacheKey
		k.Type = fields[0][0]
		nums := make([]int64, 5)
		for i := range nums {
			if nums[i], err = strconv.ParseInt(fields[i+1], 10, 64); err != nil {
				return nil, fmt.Errorf("%s: malformed line %q", path, line)
			}
		}
		k.Dev, k.Ino, k.Size, k.MtimeNS, k.CtimeNS = uint64(nums[0]), uint64(nums[1]), nums[2], nums[3], nums[4]
		out[fields[6]] = k
	}
}

// SourceChanges stat-walks an imported --in directory against in-cache.tsv
// and returns every path that was added, removed, or changed since import.
func SourceChanges(benchDir, in string) ([]string, error) {
	cache, err := readInCache(filepath.Join(benchDir, InCacheName))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", InCacheName, err)
	}
	walked, err := Walk(in)
	if err != nil {
		return nil, err
	}
	var changed []string
	seen := make(map[string]bool, len(walked))
	for _, w := range walked {
		seen[w.Path] = true
		if c, ok := cache[w.Path]; !ok || c != cacheKeyOf(w) {
			changed = append(changed, w.Path)
		}
	}
	for p := range cache {
		if !seen[p] {
			changed = append(changed, p)
		}
	}
	SortStrings(changed)
	return changed, nil
}

func sizeHistogram(entries []Entry) map[string]int {
	h := map[string]int{}
	for _, e := range entries {
		if e.Type != TypeFile {
			continue
		}
		bucket := int64(0)
		if e.Size > 0 {
			bucket = int64(1) << bits.Len64(uint64(e.Size-1))
		}
		h[FormatSize(bucket)]++
	}
	return h
}

// estimateZstdRatio compresses a seeded sample of up to 64 MiB, taken 1 MiB
// at a time from files in shuffled order, and returns logical/compressed.
func estimateZstdRatio(root string, entries []Entry) (float64, error) {
	var idx []int
	for i, e := range entries {
		if e.Type == TypeFile && e.Size > 0 {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return 1, nil
	}
	rng := rand.New(rand.NewPCG(1, 0))
	rng.Shuffle(len(idx), func(a, b int) { idx[a], idx[b] = idx[b], idx[a] })
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return 0, err
	}
	defer enc.Close()
	const chunk = 1 << 20
	buf := make([]byte, chunk)
	var in, out int64
	for _, i := range idx {
		if in >= zstdSampleBytes {
			break
		}
		f, err := os.Open(filepath.Join(root, filepath.FromSlash(entries[i].Path)))
		if err != nil {
			return 0, err
		}
		n, err := io.ReadFull(f, buf)
		f.Close()
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return 0, err
		}
		if n == 0 {
			continue
		}
		in += int64(n)
		out += int64(len(enc.EncodeAll(buf[:n], nil)))
	}
	if out == 0 {
		return 1, nil
	}
	return float64(int(float64(in)/float64(out)*100+0.5)) / 100, nil
}
