package dataset

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Dataset is a complete dataset loaded from BENCH_DIR.
type Dataset struct {
	Dir         string
	Bench       *BenchJSON
	Entries     []Entry
	Fingerprint string
}

// ErrNoDataset means BENCH_DIR holds no complete dataset.
var ErrNoDataset = errors.New("no dataset")

// Load reads bench.json and files.tsv and checks they agree. It does not
// look at the data itself; see CheckStat.
func Load(benchDir string) (*Dataset, error) {
	b, err := ReadBenchJSON(benchDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%s: %w", benchDir, ErrNoDataset)
	}
	if err != nil {
		return nil, err
	}
	entries, fp, err := ReadFilesTSVFile(filepath.Join(benchDir, FilesTSVName))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", FilesTSVName, err)
	}
	if fp != b.Fingerprint {
		return nil, fmt.Errorf("%s fingerprint %s does not match bench.json %s", FilesTSVName, fp, b.Fingerprint)
	}
	return &Dataset{Dir: benchDir, Bench: b, Entries: entries, Fingerprint: fp}, nil
}

// DataRoot is the directory recv-copy copies: BENCH_DIR/data, or the --in
// directory of an import.
func (d *Dataset) DataRoot() string {
	if d.Bench.In != nil {
		return d.Bench.In.Path
	}
	return filepath.Join(d.Dir, DataDirName)
}

// CheckStat runs the stat-only walk a reused dataset must pass: for
// generated data, paths, types, and sizes against files.tsv; for an import,
// no change against in-cache.tsv.
func (d *Dataset) CheckStat() error {
	if d.Bench.In != nil {
		changed, err := SourceChanges(d.Dir, d.Bench.In.Path)
		if err != nil {
			return err
		}
		if len(changed) > 0 {
			return fmt.Errorf("%d paths under %s changed since import (first: %s); rerun with --in %s to re-import",
				len(changed), d.Bench.In.Path, changed[0], d.Bench.In.Path)
		}
		return nil
	}
	res, err := StatMatches(d.DataRoot(), d.Entries)
	if err != nil {
		return err
	}
	if !res.OK() {
		return fmt.Errorf("%s was modified (%d differences, first: %s); rerun with --regen", d.DataRoot(), res.MismatchCount, res.Mismatches[0])
	}
	return nil
}

// Files lists the absolute paths of every regular file, in files.tsv
// order, with their sizes. Hardlinks are included once, as their 'f' entry.
func (d *Dataset) Files() (paths []string, sizes []int64) {
	root := d.DataRoot()
	for _, e := range d.Entries {
		if e.Type == TypeFile {
			paths = append(paths, filepath.Join(root, filepath.FromSlash(e.Path)))
			sizes = append(sizes, e.Size)
		}
	}
	return paths, sizes
}
