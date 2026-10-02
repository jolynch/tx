package dataset

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/zeebo/xxh3"
)

// Entry types in files.tsv.
const (
	TypeFile     = 'f'
	TypeDir      = 'd'
	TypeSymlink  = 'l'
	TypeHardlink = 'h' // a hardlink to an earlier 'f' entry
)

// Entry is one line of files.tsv.
type Entry struct {
	Type byte
	Mode uint32 // permission bits; unused for symlinks
	Size int64  // bytes for f and h
	// Hash is "xxh128:<hex32>" for f, the link target for l, the group's
	// first path for h, and empty for d.
	Hash    string
	MtimeNS int64 // unused for symlinks
	Path    string
}

// Line renders the entry as its files.tsv line, newline included.
func (e Entry) Line() string {
	var b strings.Builder
	e.appendFields(&b, true)
	return b.String()
}

func (e Entry) appendFields(b *strings.Builder, withMtime bool) {
	b.WriteByte(e.Type)
	b.WriteByte('\t')
	if e.Type == TypeSymlink {
		b.WriteByte('-')
	} else {
		fmt.Fprintf(b, "%04o", e.Mode)
	}
	b.WriteByte('\t')
	b.WriteString(strconv.FormatInt(e.Size, 10))
	b.WriteByte('\t')
	if e.Hash == "" {
		b.WriteByte('-')
	} else {
		b.WriteString(e.Hash)
	}
	if withMtime {
		b.WriteByte('\t')
		if e.Type == TypeSymlink {
			b.WriteByte('-')
		} else {
			b.WriteString(strconv.FormatInt(e.MtimeNS, 10))
		}
	}
	b.WriteByte('\t')
	b.WriteString(e.Path)
	b.WriteByte('\n')
}

// SortEntries orders entries bytewise by path, the files.tsv order.
func SortEntries(entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(a.Path, b.Path) })
}

// WriteFilesTSV writes entries, which must already be sorted.
func WriteFilesTSV(w io.Writer, entries []Entry) error {
	bw := bufio.NewWriterSize(w, 1<<20)
	for _, e := range entries {
		if _, err := bw.WriteString(e.Line()); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadFilesTSV parses a files.tsv stream.
func ReadFilesTSV(r io.Reader) ([]Entry, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	var entries []Entry
	for lineNo := 1; ; lineNo++ {
		line, err := br.ReadString('\n')
		if line == "" && err == io.EOF {
			return entries, nil
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		if !strings.HasSuffix(line, "\n") {
			return nil, fmt.Errorf("files.tsv line %d: missing newline", lineNo)
		}
		e, perr := parseEntry(strings.TrimSuffix(line, "\n"))
		if perr != nil {
			return nil, fmt.Errorf("files.tsv line %d: %w", lineNo, perr)
		}
		entries = append(entries, e)
	}
}

// ReadFilesTSVFile reads a files.tsv file and returns its fingerprint too.
func ReadFilesTSVFile(path string) ([]Entry, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	entries, err := ReadFilesTSV(bytes.NewReader(data))
	if err != nil {
		return nil, "", err
	}
	return entries, Fingerprint(entries), nil
}

func parseEntry(line string) (Entry, error) {
	// The path is last and may contain tabs, so split off exactly five
	// leading fields.
	fields := strings.SplitN(line, "\t", 6)
	if len(fields) != 6 {
		return Entry{}, fmt.Errorf("want 6 tab-separated fields, got %d", len(fields))
	}
	if len(fields[0]) != 1 || !strings.Contains("fdlh", fields[0]) {
		return Entry{}, fmt.Errorf("bad type %q", fields[0])
	}
	e := Entry{Type: fields[0][0], Path: fields[5]}
	if e.Path == "" {
		return Entry{}, fmt.Errorf("empty path")
	}
	if e.Type != TypeSymlink {
		mode, err := strconv.ParseUint(fields[1], 8, 32)
		if err != nil {
			return Entry{}, fmt.Errorf("bad mode %q", fields[1])
		}
		e.Mode = uint32(mode)
		if e.MtimeNS, err = strconv.ParseInt(fields[4], 10, 64); err != nil {
			return Entry{}, fmt.Errorf("bad mtime %q", fields[4])
		}
	}
	var err error
	if e.Size, err = strconv.ParseInt(fields[2], 10, 64); err != nil || e.Size < 0 {
		return Entry{}, fmt.Errorf("bad size %q", fields[2])
	}
	if fields[3] != "-" {
		e.Hash = fields[3]
	}
	return e, nil
}

// Fingerprint is the first 16 hex digits of xxh3-64 over the files.tsv lines
// with the mtime field removed. It commits to every path, type, mode, size,
// link target, hardlink group, and content hash, but not to mtimes, which
// differ wherever the data was generated.
func Fingerprint(entries []Entry) string {
	h := xxh3.New()
	var b strings.Builder
	for _, e := range entries {
		b.Reset()
		e.appendFields(&b, false)
		_, _ = h.WriteString(b.String())
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

// Totals counts the regular files (f and h) and their bytes.
func Totals(entries []Entry) (files int, bytes int64) {
	for _, e := range entries {
		if e.Type == TypeFile || e.Type == TypeHardlink {
			files++
			bytes += e.Size
		}
	}
	return files, bytes
}
