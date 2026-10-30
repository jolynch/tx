package filexfercli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/idset"
)

// staleMtimeMinGap is how far the stale-marking mtime sits from the source
// mtime. Two seconds clears the coarsest timestamps, such as FAT's.
const staleMtimeMinGap = 2 * time.Second

// staleMtime returns the mtime that marks a file stale: Unix epoch 0, or
// Unix 4s when the source mtime is within staleMtimeMinGap of the epoch, so
// it never matches the source mtime.
func staleMtime(remoteMtimeNS int64) time.Time {
	if remoteMtimeNS > -int64(staleMtimeMinGap) && remoteMtimeNS < int64(staleMtimeMinGap) {
		return time.Unix(4, 0)
	}
	return time.Unix(0, 0)
}

// invalidateForRefetch sets the atime and mtime of the regular file at
// dstPath to a value that does not match remoteMtimeNS, so the next copy's
// size, mtime, and mode comparison treats the file as stale and fetches it
// again. It skips a final path component that is not a regular file, such as
// a symlink. A filesystem can clamp the time: FAT and exFAT store nothing
// before 1980, so epoch 0 lands on a 1980 source mtime. It reads the time
// back and, if it still matches, moves it a day past the source mtime.
func invalidateForRefetch(dstPath string, remoteMtimeNS int64) error {
	info, err := os.Lstat(dstPath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", dstPath)
	}
	for _, t := range []time.Time{staleMtime(remoteMtimeNS), time.Unix(0, remoteMtimeNS).Add(24 * time.Hour)} {
		if err := os.Chtimes(dstPath, t, t); err != nil {
			return err
		}
		if info, err = os.Lstat(dstPath); err != nil {
			return err
		}
		if info.ModTime().UnixNano() != remoteMtimeNS {
			return nil
		}
	}
	return fmt.Errorf("%s keeps the source mtime after chtimes", dstPath)
}

// refetchMarker marks files stale best-effort and counts the ones it marked.
// It takes one path at a time, so a run that fails every file holds no list
// of them.
type refetchMarker struct {
	stderr io.Writer
	prefix string
	marked int
}

// mark invalidates the file at path, whose source mtime is mtimeNS. A
// failure prints a warning and never changes the run's exit code.
func (r *refetchMarker) mark(path string, mtimeNS int64) {
	if err := invalidateForRefetch(path, mtimeNS); err != nil {
		fmt.Fprintf(r.stderr, "%s: WARNING: could not mark %s to fetch again: %v\n", r.prefix, path, err)
		return
	}
	r.marked++
}

// finish reports how many files it marked, once, as "<prefix>: marked N files
// to fetch again on the next run". It prints nothing when it marked none.
func (r *refetchMarker) finish() {
	if r.marked == 0 {
		return
	}
	noun := "files"
	if r.marked == 1 {
		noun = "file"
	}
	fmt.Fprintf(r.stderr, "%s: marked %d %s to fetch again on the next run\n", r.prefix, r.marked, noun)
}

// markForRefetch marks the destination file of each mismatching verify file,
// in one pass over the run's files.
func (m *mismatchError) markForRefetch(stderr io.Writer, prefix string) {
	marker := refetchMarker{stderr: stderr, prefix: prefix}
	for _, f := range m.files {
		if m.ids.Has(f.entry.ID) {
			marker.mark(f.dstPath, f.entry.Mtime)
		}
	}
	marker.finish()
}

// markEntriesForRefetch marks the regular files of entries whose IDs are in
// ids, under root, in one pass over the manifest. A file's path is built when
// its turn comes, so the work is proportional to the failures and the memory
// to nothing. root is the directory the files are in now: the target after
// the staging rename, so no path needs rewriting.
func markEntriesForRefetch(stderr io.Writer, prefix, root string, ids *idset.Set, entries []tx.ManifestEntry) {
	if ids.Len() == 0 {
		return
	}
	m := refetchMarker{stderr: stderr, prefix: prefix}
	for _, e := range entries {
		if isRegularFileEntry(e.Type) && ids.Has(e.ID) {
			m.mark(filepath.Join(root, filepath.FromSlash(e.Path)), e.Mtime)
		}
	}
	m.finish()
}
