package dataset

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/zeebo/xxh3"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// SilesiaArchiveURL is where missing corpus files are downloaded from.
var SilesiaArchiveURL = "http://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip"

// silesiaHTTPClient is replaced by tests.
var silesiaHTTPClient = http.DefaultClient

// DefaultSilesiaCache is $XDG_CACHE_HOME/tx-bench/silesia, falling back to
// ~/.cache.
func DefaultSilesiaCache() string {
	base := os.Getenv("XDG_CACHE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return filepath.Join(os.TempDir(), "tx-bench", "silesia")
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "tx-bench", "silesia")
}

// Corpus holds loaded Silesia originals.
type Corpus struct {
	files  map[string][]byte
	hashes map[string]string
}

// Ring concatenates the named originals in the order given.
func (c *Corpus) Ring(names []string) []byte {
	n := 0
	for _, name := range names {
		n += len(c.files[name])
	}
	ring := make([]byte, 0, n)
	for _, name := range names {
		ring = append(ring, c.files[name]...)
	}
	return ring
}

// Hashes returns the xxh128 token of each loaded original.
func (c *Corpus) Hashes() map[string]string { return c.hashes }

// LoadCorpus loads the named originals from cacheDir, downloading the
// archive once if any are missing. logf reports progress.
func LoadCorpus(cacheDir string, names []string, logf func(string, ...any)) (*Corpus, error) {
	c := &Corpus{files: map[string][]byte{}, hashes: map[string]string{}}
	if len(names) == 0 {
		return c, nil
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create silesia cache %s: %w", cacheDir, err)
	}
	var missing []string
	for _, name := range names {
		if _, err := os.Stat(filepath.Join(cacheDir, name)); err != nil {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		if err := downloadCorpus(cacheDir, missing, logf); err != nil {
			return nil, fmt.Errorf("%w\n  missing: %s\n  cache:   %s\n  archive: %s\n  (on an air-gapped host, unzip the archive into the cache directory)",
				err, strings.Join(missing, ", "), cacheDir, SilesiaArchiveURL)
		}
	}
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(cacheDir, name))
		if err != nil {
			return nil, fmt.Errorf("read silesia %s: %w", name, err)
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("silesia %s in %s is empty", name, cacheDir)
		}
		c.files[name] = data
		c.hashes[name] = encoding.FormatXXH128HashToken(xxh3.Hash128(data))
	}
	return c, nil
}

func downloadCorpus(cacheDir string, names []string, logf func(string, ...any)) error {
	logf("silesia: downloading %s", SilesiaArchiveURL)
	resp, err := silesiaHTTPClient.Get(SilesiaArchiveURL)
	if err != nil {
		return fmt.Errorf("download silesia archive: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download silesia archive: %s", resp.Status)
	}
	tmp, err := os.CreateTemp(cacheDir, ".silesia-*.zip")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	n, err := io.Copy(tmp, resp.Body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("download silesia archive: %w", err)
	}
	logf("silesia: downloaded %s", encoding.HumanBytes(n))

	archive, err := zip.OpenReader(tmp.Name())
	if err != nil {
		return fmt.Errorf("open silesia archive: %w", err)
	}
	defer archive.Close()
	found := map[string]bool{}
	for _, f := range archive.File {
		base := path.Base(f.Name)
		if f.FileInfo().IsDir() || !slices.Contains(names, base) || found[base] {
			continue
		}
		if err := extractZipFile(f, filepath.Join(cacheDir, base)); err != nil {
			return fmt.Errorf("extract silesia %s: %w", base, err)
		}
		found[base] = true
	}
	for _, name := range names {
		if !found[name] {
			return fmt.Errorf("silesia archive lacks %s", name)
		}
	}
	return nil
}

func extractZipFile(f *zip.File, dst string) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, rc); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
