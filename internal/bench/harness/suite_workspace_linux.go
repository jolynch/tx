//go:build linux

package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"golang.org/x/sys/unix"
)

const (
	suitePrepPrefix = ".prep-"
	suiteCopyPrefix = ".copy-"
	suiteDirMarker  = ".tx-bench-suite"
	// suiteSmokeParent holds smoke workspaces made without --data-root. It is
	// under the checkout, not the system temp directory, which is often a
	// tmpfs whose pages cannot be evicted for a cold copy.
	suiteSmokeParent = "bench/data"
)

// suiteStatFS reads free space; tests replace it to simulate a full disk.
var suiteStatFS = dataset.StatFS

type suiteWorkspaceOptions struct {
	Root    string // --data-root; empty makes a temporary smoke workspace
	Out     string // canonical result directory, which must lie outside Root
	Profile string
	Cases   []suiteCase // the SSD cases whose references to validate
	Log     io.Writer   // notes about cache repairs
	// Evictable rejects a root whose filesystem cannot evict cached pages,
	// before anything is written there.
	Evictable bool
	// KeepUnused is how long a reference no case in Cases uses stays after
	// its last use. Zero keeps it until a run needs its space.
	KeepUnused time.Duration
}

type suiteWorkspace struct {
	locks      []*os.File
	temp       string
	top        string // the locked root as given, before any smoke/ subdirectory
	root       string
	sources    string
	scratch    string
	filesystem suiteFilesystem
	references map[string]*dataset.BenchJSON
	log        io.Writer
	escalate   <-chan struct{} // closed on a second interrupt
	grace      time.Duration   // SIGTERM-to-SIGKILL grace; zero means suiteKillGrace
}

type suiteFilesystem struct {
	device uint64
	mount  uint64
}

func openSuiteWorkspace(opts suiteWorkspaceOptions) (*suiteWorkspace, error) {
	root, out, profile, cases := opts.Root, opts.Out, opts.Profile, opts.Cases
	w := &suiteWorkspace{references: make(map[string]*dataset.BenchJSON), log: opts.Log}
	if w.log == nil {
		w.log = io.Discard
	}
	ready := false
	defer func() {
		if !ready {
			w.close()
		}
	}()
	if root == "" {
		if opts.Evictable {
			if err := checkEviction(filepath.Dir(suiteSmokeParent)); err != nil {
				return nil, err
			}
		}
		if err := os.MkdirAll(suiteSmokeParent, 0o755); err != nil {
			return nil, err
		}
		var err error
		w.temp, err = os.MkdirTemp(suiteSmokeParent, "suite-smoke-")
		if err != nil {
			return nil, err
		}
		root = w.temp
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if opts.Evictable {
		// Resolve symlinked parents first: StatFS matches /proc/mounts by path.
		resolved, err := filepath.EvalSymlinks(root)
		if err != nil {
			return nil, err
		}
		if err := checkEviction(resolved); err != nil {
			return nil, err
		}
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
	if rel, err := filepath.Rel(root, out); err == nil && filepath.IsLocal(rel) {
		return nil, usageErrorf("output directory must be outside the suite root %s", root)
	}
	w.top, w.root = root, root
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
	if err := w.prepareReferences(profile, cases, opts.KeepUnused); err != nil {
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

func (w *suiteWorkspace) prepareReferences(profile string, cases []suiteCase, keepUnused time.Duration) error {
	now := time.Now()
	var missing, scratch int64
	for _, c := range cases {
		scratch = max(scratch, c.Bytes)
		entry := w.cacheEntry(c)
		switch err := w.checkDir(entry); {
		case errors.Is(err, os.ErrNotExist):
			missing += c.Bytes
			continue
		case err != nil:
			// A symlink or another filesystem here is not ours to remove.
			return err
		}
		d, err := w.readReference(entry, c)
		if err == nil {
			err = d.CheckStat()
		}
		if err != nil {
			fmt.Fprintf(w.log, "Cached reference %s is invalid (%v); provisioning it again\n", entry, err)
			if err := w.quarantine(entry); err != nil {
				return fmt.Errorf("remove invalid cached reference: %w", err)
			}
			missing += c.Bytes
			continue
		}
		runs := filepath.Join(d.Dir, runsDir)
		if err := w.removeTree(runs); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("clear cached run state: %w", err)
		}
		// The source directory's mtime records the last run that used this
		// entry. Older harnesses bump it too, by replacing runs/ and
		// send-tree's state files in it.
		ts := unix.NsecToTimespec(now.UnixNano())
		if err := unix.UtimesNanoAt(unix.AT_FDCWD, filepath.Join(entry, "source"), []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			fmt.Fprintf(w.log, "Could not mark reference %s as used: %v\n", entry, err)
		}
		w.references[c.ID] = d.Bench
	}
	if keepUnused > 0 {
		// Another commit's harness may still use an older recipe, so keep
		// an unused entry for a while rather than thrash with it.
		if err := w.removeStaleReferences(cases, now.Add(-keepUnused)); err != nil {
			return err
		}
	}
	needed := missing + scratch
	if profile == "full" {
		needed += 64 * suiteGiB
	}
	fs, err := suiteStatFS(w.root)
	if err != nil {
		return err
	}
	if fs.Available < needed {
		// References for other recipes only matter to the commits that
		// made them. Remove them only when this run needs their space.
		if err := w.removeStaleReferences(cases, time.Time{}); err != nil {
			return err
		}
		if fs, err = suiteStatFS(w.root); err != nil {
			return err
		}
	}
	if fs.Available < needed {
		return fmt.Errorf("%s has %s available; suite needs %s for references, scratch, and headroom",
			w.root, encoding.HumanBytes(fs.Available), encoding.HumanBytes(needed))
	}
	return nil
}

// quarantine moves a cache entry under a staging prefix, so an interrupted
// removal is reclaimed by the next run, then removes it.
func (w *suiteWorkspace) quarantine(entry string) error {
	hold, err := os.MkdirTemp(w.sources, suitePrepPrefix+"stale-")
	if err != nil {
		return err
	}
	if err := os.Rename(entry, filepath.Join(hold, filepath.Base(entry))); err != nil {
		_ = os.Remove(hold)
		return err
	}
	return w.removeTree(hold)
}

// removeStaleReferences reclaims owned entries for older recipes or retired
// cases. A nonzero usedBefore keeps entries a run used at or after it.
func (w *suiteWorkspace) removeStaleReferences(cases []suiteCase, usedBefore time.Time) error {
	keep := make(map[string]bool, len(cases))
	for _, c := range cases {
		keep[filepath.Base(w.cacheEntry(c))] = true
	}
	entries, err := os.ReadDir(w.sources)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		sep := strings.LastIndexByte(name, '-') // <id>-<20 hex digits>
		if !e.IsDir() || keep[name] || sep < 0 || !suiteID.MatchString(name[:sep]) || !isSuiteHash(name[sep+1:]) {
			continue
		}
		entry := filepath.Join(w.sources, name)
		source := filepath.Join(entry, "source")
		marker := filepath.Join(entry, "reference.json")
		if w.checkDir(entry) != nil || w.checkDir(source) != nil ||
			checkSuiteRegular(filepath.Join(source, dataset.BenchMarker)) != nil || checkSuiteRegular(marker) != nil {
			continue
		}
		var fingerprint string
		if err := readJSON(marker, &fingerprint); err != nil || len(fingerprint) != 16 {
			continue
		}
		if _, err := hex.DecodeString(fingerprint); err != nil {
			continue
		}
		used, err := os.Lstat(source)
		if err != nil {
			continue
		}
		if !usedBefore.IsZero() && !used.ModTime().Before(usedBefore) {
			continue
		}
		fmt.Fprintf(w.log, "Removing stale reference %s, last used %s\n", name, used.ModTime().UTC().Format(time.RFC3339))
		if err := w.quarantine(entry); err != nil {
			return fmt.Errorf("remove stale reference: %w", err)
		}
	}
	return nil
}

func isSuiteHash(s string) bool {
	if len(s) != 20 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// checkEviction rejects a workspace whose filesystem keeps file pages
// cached regardless of posix_fadvise, so a cold copy is impossible.
func checkEviction(root string) error {
	fs, err := dataset.StatFS(root)
	if err != nil {
		return err
	}
	if fs.IgnoresFadvise() || fs.Type == "tmpfs" || fs.Type == "ramfs" {
		return usageErrorf("%s is on %s, which cannot evict cached pages for a cold copy; choose a --data-root on a disk-backed filesystem", root, fs.Type)
	}
	return nil
}

// recheckReference rehashes a reference after a copy that mismatched it. A
// reference that fails its own check is removed so the next run provisions
// it again, and recheckReference reports true: the copy measured a damaged
// source, not tx. Otherwise the mismatch stands as tx's.
func (w *suiteWorkspace) recheckReference(ctx context.Context, c suiteCase, bench, source string, log *os.File) (damaged bool) {
	checkCtx, cancel := context.WithTimeout(ctx, suiteSSDTimeout)
	defer cancel()
	args := []string{bench, "prep", "--check", source}
	err := w.run(checkCtx, log, suiteCommand{args: args, harness: true, workspace: true})
	switch {
	case err == nil:
		fmt.Fprintln(log, "The reference passed its check; the mismatch is in the copy.")
		return false
	case ctx.Err() != nil || checkCtx.Err() != nil:
		fmt.Fprintf(log, "Reference check did not finish: %v\n", err)
		return false
	}
	fmt.Fprintf(log, "The reference failed its check (%v); removing it\n", err)
	if err := w.quarantine(w.cacheEntry(c)); err != nil {
		fmt.Fprintf(log, "Could not remove the reference: %v\n", err)
	}
	delete(w.references, c.ID)
	return true
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
	prep := suiteCommand{args: append([]string{bench}, c.prepArgs(source)...), harness: true, workspace: true}
	if err := w.run(ctx, log, prep); err != nil {
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
