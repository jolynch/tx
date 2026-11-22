//go:build linux

package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"golang.org/x/sys/unix"
)

const (
	suitePrepPrefix = ".prep-"
	suiteCopyPrefix = ".copy-"
	suiteDirMarker  = ".tx-bench-suite"
)

type suiteWorkspace struct {
	locks      []*os.File
	temp       string
	root       string
	sources    string
	scratch    string
	filesystem suiteFilesystem
	references map[string]*dataset.BenchJSON
}

type suiteFilesystem struct {
	device uint64
	mount  uint64
}

func openSuiteWorkspace(root, out, profile string, cases []suiteCase) (*suiteWorkspace, error) {
	w := &suiteWorkspace{references: make(map[string]*dataset.BenchJSON)}
	ready := false
	defer func() {
		if !ready {
			w.close()
		}
	}()
	if root == "" {
		var err error
		w.temp, err = os.MkdirTemp("", "tx-bench-smoke-")
		if err != nil {
			return nil, err
		}
		root = w.temp
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fs, err := suiteDirFilesystem(root)
	if err != nil {
		return nil, err
	}
	// Compare the canonical paths when checking where output may live.
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	w.filesystem = fs
	if err := w.lockRoot(root); err != nil {
		return nil, err
	}
	w.root = root
	if profile == "smoke" && w.temp == "" {
		w.root = filepath.Join(root, "smoke")
		if err := w.makeDir(w.root); err != nil {
			return nil, err
		}
		// Someone may also pass this subdirectory directly as their root.
		if err := w.lockRoot(w.root); err != nil {
			return nil, err
		}
	}
	w.sources = filepath.Join(w.root, "sources")
	w.scratch = filepath.Join(w.root, "scratch")
	for _, dir := range []string{w.sources, w.scratch} {
		rel, err := filepath.Rel(dir, out)
		if err == nil && filepath.IsLocal(rel) {
			return nil, fmt.Errorf("output directory must be outside %s", dir)
		}
		if err := w.claimDir(dir); err != nil {
			return nil, err
		}
	}
	if err := w.reclaim(w.sources, suitePrepPrefix); err != nil {
		return nil, err
	}
	if err := w.reclaim(w.scratch, suiteCopyPrefix); err != nil {
		return nil, err
	}
	if err := w.prepareReferences(profile, cases); err != nil {
		return nil, err
	}
	ready = true
	return w, nil
}

func (w *suiteWorkspace) close() {
	if w.temp != "" {
		_ = os.RemoveAll(w.temp)
	}
	for i := len(w.locks) - 1; i >= 0; i-- {
		_ = w.locks[i].Close()
	}
	w.locks = nil
}

func (w *suiteWorkspace) lockRoot(root string) error {
	f, err := lockSuiteRoot(root)
	if err == nil {
		w.locks = append(w.locks, f)
	}
	return err
}

func lockSuiteRoot(root string) (*os.File, error) {
	path := filepath.Join(root, ".suite.lock")
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open suite lock: %w", err)
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err == nil && !st.Mode().IsRegular() {
		err = fmt.Errorf("%s must be a regular file", path)
	}
	if err == nil {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("lock suite root %s: %w", root, err)
	}
	return f, nil
}

func suiteDirFilesystem(path string) (suiteFilesystem, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return suiteFilesystem{}, err
	}
	if !st.IsDir() {
		return suiteFilesystem{}, fmt.Errorf("%s must be a directory, without a symlink", path)
	}
	fs := suiteFilesystem{device: uint64(st.Sys().(*syscall.Stat_t).Dev)}
	// Mount IDs also catch bind mounts of the same filesystem. Older kernels
	// may only provide the device ID.
	var sx unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_MNT_ID, &sx); err == nil && sx.Mask&unix.STATX_MNT_ID != 0 {
		fs.mount = sx.Mnt_id
	}
	return fs, nil
}

func (w *suiteWorkspace) checkDir(path string) error {
	fs, err := suiteDirFilesystem(path)
	if err != nil {
		return err
	}
	if fs.device != w.filesystem.device || (fs.mount != 0 && w.filesystem.mount != 0 && fs.mount != w.filesystem.mount) {
		return fmt.Errorf("%s must share the filesystem and mount of %s", path, w.root)
	}
	return nil
}

func (w *suiteWorkspace) makeDir(path string) error {
	if err := os.Mkdir(path, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return w.checkDir(path)
}

func (w *suiteWorkspace) claimDir(path string) error {
	if err := w.makeDir(path); err != nil {
		return err
	}
	marker := filepath.Join(path, suiteDirMarker)
	if err := checkSuiteRegular(marker); !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return dataset.ClaimDir(path, suiteDirMarker)
}

func (w *suiteWorkspace) reclaim(dir, prefix string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			path := filepath.Join(dir, entry.Name())
			if err := w.removeTree(path); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *suiteWorkspace) removeTree(path string) error {
	if err := w.checkDir(path); err != nil {
		return err
	}
	// RemoveAll skips symlinks. Check directories for mounted filesystems
	// before removing a tree left behind by an earlier run.
	if err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			return w.checkDir(p)
		}
		return err
	}); err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func (w *suiteWorkspace) prepareReferences(profile string, cases []suiteCase) error {
	var missing, scratch int64
	for _, c := range cases {
		scratch = max(scratch, c.Bytes)
		entry := w.cacheEntry(c)
		switch err := w.checkDir(entry); {
		case errors.Is(err, os.ErrNotExist):
			missing += c.Bytes
			continue
		case err != nil:
			return err
		}
		d, err := w.readReference(entry, c)
		if err != nil {
			return fmt.Errorf("validate cached reference %s: %w", entry, err)
		}
		if err := d.CheckStat(); err != nil {
			return fmt.Errorf("validate cached files in %s: %w", entry, err)
		}
		runs := filepath.Join(d.Dir, runsDir)
		if err := w.removeTree(runs); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("clear cached run state: %w", err)
		}
		w.references[c.ID] = d.Bench
	}
	needed := missing + scratch
	if profile == "full" {
		needed += 64 * suiteGiB
	}
	fs, err := dataset.StatFS(w.root)
	if err != nil {
		return err
	}
	if fs.Available < needed {
		return fmt.Errorf("%s has %s available; suite needs %s for references, scratch, and headroom",
			w.root, encoding.HumanBytes(fs.Available), encoding.HumanBytes(needed))
	}
	return nil
}

func (w *suiteWorkspace) cacheEntry(c suiteCase) string {
	recipe, _ := json.Marshal(c.Recipe)
	hash := sha256.Sum256(recipe)
	return filepath.Join(w.sources, c.ID+"-"+hex.EncodeToString(hash[:10]))
}

func (w *suiteWorkspace) reference(ctx context.Context, c suiteCase, bench string, log *os.File) (string, *dataset.BenchJSON, error) {
	entry := w.cacheEntry(c)
	if b := w.references[c.ID]; b != nil {
		fmt.Fprintf(log, "Reusing reference %s (%s)\n", entry, b.Fingerprint)
		return filepath.Join(entry, "source"), b, nil
	}
	staging, err := os.MkdirTemp(w.sources, suitePrepPrefix+c.ID+"-")
	if err != nil {
		return "", nil, err
	}
	defer os.RemoveAll(staging)
	source := filepath.Join(staging, "source")
	if err := runSuiteCommand(ctx, log, append([]string{bench}, c.prepArgs(source)...), nil, true); err != nil {
		return "", nil, fmt.Errorf("prepare reference: %w", err)
	}
	d, err := w.loadReference(source, c)
	if err != nil {
		return "", nil, err
	}
	if err := dataset.WriteJSONAtomic(filepath.Join(staging, "reference.json"), d.Fingerprint); err != nil {
		return "", nil, err
	}
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if err := os.Rename(staging, entry); err != nil {
		return "", nil, err
	}
	fmt.Fprintf(log, "Provisioned reference %s (%s)\n", entry, d.Fingerprint)
	return filepath.Join(entry, "source"), d.Bench, nil
}

func (w *suiteWorkspace) readReference(entry string, c suiteCase) (*dataset.Dataset, error) {
	path := filepath.Join(entry, "reference.json")
	if err := checkSuiteRegular(path); err != nil {
		return nil, err
	}
	var fingerprint string
	if err := readJSON(path, &fingerprint); err != nil {
		return nil, fmt.Errorf("read cached fingerprint: %w", err)
	}
	d, err := w.loadReference(filepath.Join(entry, "source"), c)
	if err != nil {
		return nil, err
	}
	if fingerprint != d.Fingerprint {
		return nil, fmt.Errorf("%s fingerprint %s does not match cached fingerprint %q", entry, d.Fingerprint, fingerprint)
	}
	return d, nil
}

func (w *suiteWorkspace) loadReference(source string, c suiteCase) (*dataset.Dataset, error) {
	if err := w.checkDir(source); err != nil {
		return nil, err
	}
	dataRoot := filepath.Join(source, dataset.DataDirName)
	if err := w.checkDir(dataRoot); err != nil {
		return nil, err
	}
	for _, name := range []string{dataset.BenchJSONName, dataset.FilesTSVName, dataset.BenchMarker} {
		if err := checkSuiteRegular(filepath.Join(source, name)); err != nil {
			return nil, err
		}
	}
	d, err := dataset.Load(source)
	if err != nil {
		return nil, err
	}
	b, recipe := d.Bench, c.Recipe
	if !b.Generated() || b.Seed == nil || *b.Seed != recipe.Seed || b.Mix == nil || *b.Mix != recipe.Mix ||
		b.SizeSpec == nil || *b.SizeSpec != recipe.Size || b.Bytes != c.Bytes || b.Files <= 0 {
		return nil, fmt.Errorf("generated metadata in %s does not match the suite recipe", source)
	}
	var files int
	var bytes int64
	for _, e := range d.Entries {
		if !filepath.IsLocal(e.Path) {
			return nil, fmt.Errorf("invalid generated path %q in %s", e.Path, source)
		}
		switch e.Type {
		case dataset.TypeDir:
			if err := w.checkDir(filepath.Join(dataRoot, filepath.FromSlash(e.Path))); err != nil {
				return nil, err
			}
		case dataset.TypeFile:
			files++
			bytes += e.Size
		default:
			return nil, fmt.Errorf("unexpected entry type %q for %s in %s", e.Type, e.Path, source)
		}
	}
	if files != b.Files || bytes != b.Bytes {
		return nil, fmt.Errorf("generated file list in %s does not match its metadata", source)
	}
	return d, nil
}

func checkSuiteRegular(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file, without a symlink", path)
	}
	return nil
}
