package dataset

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/zeebo/xxh3"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

// Stat is the identity of a walked entry, used for the import stat cache and
// hardlink grouping.
type Stat struct {
	Dev, Ino uint64
	Nlink    uint64
	CtimeNS  int64
}

// WalkedEntry is an Entry plus the stat facts the walk saw.
type WalkedEntry struct {
	Entry
	Stat
}

// UnsupportedError lists entries of a type tx-bench cannot benchmark.
type UnsupportedError struct{ Paths []string }

func (e *UnsupportedError) Error() string {
	const show = 20
	paths := e.Paths
	more := ""
	if len(paths) > show {
		more = fmt.Sprintf(" (and %d more)", len(paths)-show)
		paths = paths[:show]
	}
	return fmt.Sprintf("unsupported entries (sockets, FIFOs, or devices): %s%s", strings.Join(paths, ", "), more)
}

// Walk stats every entry under root (not root itself) and returns them in
// files.tsv order. File hashes are left empty; HashEntries fills them.
// Regular files sharing an inode become one 'f' entry (the first in path
// order) and 'h' entries naming it. Sockets, FIFOs, and devices produce an
// *UnsupportedError listing every one of them.
func Walk(root string) ([]WalkedEntry, error) {
	var (
		out         []WalkedEntry
		unsupported []string
	)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if strings.ContainsAny(rel, "\n\r") {
			return fmt.Errorf("path %q contains a line break, which tx cannot transfer", rel)
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("stat %s: no unix stat", path)
		}
		we := WalkedEntry{
			Entry: Entry{
				Mode:    uint32(info.Mode().Perm()),
				MtimeNS: info.ModTime().UnixNano(),
				Path:    filepath.ToSlash(rel),
			},
			Stat: Stat{
				Dev:     uint64(st.Dev),
				Ino:     st.Ino,
				Nlink:   uint64(st.Nlink),
				CtimeNS: st.Ctim.Nano(),
			},
		}
		switch mode := info.Mode(); {
		case mode.IsDir():
			we.Type = TypeDir
		case mode.IsRegular():
			we.Type = TypeFile
			we.Size = info.Size()
		case mode&fs.ModeSymlink != 0:
			we.Type = TypeSymlink
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			we.Hash = target
			we.Mode, we.MtimeNS = 0, 0
		default:
			unsupported = append(unsupported, we.Path)
			return nil
		}
		out = append(out, we)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(unsupported) > 0 {
		return nil, &UnsupportedError{Paths: unsupported}
	}
	sortWalked(out)
	groupHardlinks(out)
	return out, nil
}

func sortWalked(entries []WalkedEntry) {
	// WalkDir visits in lexical order per directory, which differs from a
	// bytewise sort of full paths ("a/b" vs "a.b"), so sort explicitly.
	slices.SortFunc(entries, func(a, b WalkedEntry) int { return strings.Compare(a.Path, b.Path) })
}

func groupHardlinks(entries []WalkedEntry) {
	type key struct{ dev, ino uint64 }
	first := make(map[key]string)
	for i := range entries {
		e := &entries[i]
		if e.Type != TypeFile || e.Nlink < 2 {
			continue
		}
		k := key{e.Dev, e.Ino}
		if p, ok := first[k]; ok {
			e.Type = TypeHardlink
			e.Hash = p
			continue
		}
		first[k] = e.Path
	}
}

// Entries strips the stat facts.
func Entries(walked []WalkedEntry) []Entry {
	out := make([]Entry, len(walked))
	for i, w := range walked {
		out[i] = w.Entry
	}
	return out
}

// HashProgress is called after each file is hashed.
type HashProgress func(bytes int64)

// HashEntries fills the content hash of every 'f' entry selected by need
// (nil selects all), reading files under root with jobs workers.
func HashEntries(ctx context.Context, root string, entries []Entry, need func(int) bool, jobs int, progress HashProgress) error {
	if jobs <= 0 {
		jobs = runtime.NumCPU()
	}
	idx := make(chan int)
	errCh := make(chan error, jobs)
	var wg sync.WaitGroup
	for w := 0; w < jobs; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1<<20)
			for i := range idx {
				h, err := HashFile(filepath.Join(root, filepath.FromSlash(entries[i].Path)), buf)
				if err != nil {
					errCh <- err
					return
				}
				entries[i].Hash = h
				if progress != nil {
					progress(entries[i].Size)
				}
			}
		}()
	}
	var sendErr error
send:
	for i := range entries {
		if entries[i].Type != TypeFile || (need != nil && !need(i)) {
			continue
		}
		select {
		case idx <- i:
		case err := <-errCh:
			sendErr = err
			break send
		case <-ctx.Done():
			sendErr = ctx.Err()
			break send
		}
	}
	close(idx)
	wg.Wait()
	close(errCh)
	if sendErr != nil {
		return sendErr
	}
	for err := range errCh {
		return err
	}
	return nil
}

// HashFile returns the "xxh128:<hex32>" token of a file's contents.
func HashFile(path string, buf []byte) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := xxh3.New128()
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return encoding.FormatXXH128HashToken(h.Sum128()), nil
}

// HexOf strips the "xxh128:" prefix of a hash token.
func HexOf(token string) string {
	_, hex, _ := strings.Cut(token, ":")
	return hex
}
