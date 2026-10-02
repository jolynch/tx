package dataset

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MaxReportedMismatches bounds the offending paths a failed check lists.
const MaxReportedMismatches = 20

// Mismatch is one difference between a tree and files.tsv.
type Mismatch struct {
	Path     string `json:"path"`
	Field    string `json:"field"` // missing, extra, type, mode, size, mtime, hash, target, hardlink
	Expected string `json:"expected"`
	Got      string `json:"got"`
}

func (m Mismatch) String() string {
	switch m.Field {
	case "missing", "extra":
		return fmt.Sprintf("%s: %s", m.Path, m.Field)
	}
	return fmt.Sprintf("%s: %s expected %s, got %s", m.Path, m.Field, m.Expected, m.Got)
}

// VerifyOptions selects how deeply Verify checks.
type VerifyOptions struct {
	// Content hashes every regular file (--oracle full); otherwise only
	// structure and metadata are compared (--oracle names).
	Content bool
	// SkipMtime skips mtime comparison, for structural checks of a tree whose
	// mtimes are not meaningful.
	SkipMtime bool
	Jobs      int
	// Excused, when set, marks paths whose mismatches are expected (an
	// --in source that changed since import). They are counted apart from
	// real mismatches.
	Excused func(path string) bool
}

// VerifyResult summarizes a check.
type VerifyResult struct {
	Entries        int           `json:"entries"`
	Files          int           `json:"files"`
	Bytes          int64         `json:"bytes"`
	MismatchCount  int           `json:"mismatches"`
	Mismatches     []Mismatch    `json:"mismatch_paths,omitempty"` // at most MaxReportedMismatches
	ExcusedCount   int           `json:"excused"`
	Excused        []Mismatch    `json:"excused_paths,omitempty"` // at most MaxReportedMismatches
	Duration       time.Duration `json:"-"`
	DurationMillis int64         `json:"duration_ms"`
}

// OK reports whether the tree matched.
func (r VerifyResult) OK() bool { return r.MismatchCount == 0 }

// Paths returns every reported mismatched path.
func (r VerifyResult) Paths() []string {
	out := make([]string, 0, len(r.Mismatches))
	for _, m := range r.Mismatches {
		out = append(out, m.Path)
	}
	return out
}

type mismatchSink struct {
	count, excusedCount int
	list, excused       []Mismatch
	isExcused           func(string) bool
}

func (s *mismatchSink) add(m Mismatch) {
	if s.isExcused != nil && s.isExcused(m.Path) {
		s.excusedCount++
		if len(s.excused) < MaxReportedMismatches {
			s.excused = append(s.excused, m)
		}
		return
	}
	s.count++
	if len(s.list) < MaxReportedMismatches {
		s.list = append(s.list, m)
	}
}

// Verify walks root and checks it against expected (in files.tsv order):
// the same set of paths; the same type, mode, size, mtime, symlink target,
// and hardlink grouping for each; and, with Content, the same file hashes.
// It never consults tx.
func Verify(ctx context.Context, root string, expected []Entry, opts VerifyOptions) (VerifyResult, error) {
	start := time.Now()
	walked, err := Walk(root)
	if err != nil {
		return VerifyResult{}, err
	}
	got := Entries(walked)
	sink := mismatchSink{isExcused: opts.Excused}
	var toHash []Entry
	var expectedHash []string
	res := VerifyResult{Entries: len(got)}

	i, j := 0, 0
	for i < len(expected) || j < len(got) {
		switch {
		case j >= len(got) || (i < len(expected) && expected[i].Path < got[j].Path):
			sink.add(Mismatch{Path: expected[i].Path, Field: "missing"})
			i++
		case i >= len(expected) || got[j].Path < expected[i].Path:
			sink.add(Mismatch{Path: got[j].Path, Field: "extra"})
			j++
		default:
			e, g := expected[i], got[j]
			if compareEntry(e, g, opts, &sink) && e.Type == TypeFile {
				res.Files++
				if opts.Content {
					toHash = append(toHash, Entry{Type: TypeFile, Path: g.Path, Size: g.Size})
					expectedHash = append(expectedHash, e.Hash)
				}
			}
			i++
			j++
		}
	}
	if len(toHash) > 0 {
		if err := HashEntries(ctx, root, toHash, nil, opts.Jobs, nil); err != nil {
			return VerifyResult{}, err
		}
		for k, h := range toHash {
			res.Bytes += h.Size
			if h.Hash != expectedHash[k] {
				sink.add(Mismatch{Path: h.Path, Field: "hash", Expected: expectedHash[k], Got: h.Hash})
			}
		}
	}
	res.MismatchCount, res.Mismatches = sink.count, sink.list
	res.ExcusedCount, res.Excused = sink.excusedCount, sink.excused
	res.Duration = time.Since(start)
	res.DurationMillis = res.Duration.Milliseconds()
	return res, nil
}

// compareEntry records every metadata difference and reports whether the
// entry matched.
func compareEntry(e, g Entry, opts VerifyOptions, sink *mismatchSink) bool {
	before := sink.count + sink.excusedCount
	if e.Type != g.Type {
		field := "type"
		if e.Type == TypeHardlink || g.Type == TypeHardlink {
			field = "hardlink"
		}
		sink.add(Mismatch{Path: e.Path, Field: field, Expected: string(e.Type), Got: string(g.Type)})
		return false
	}
	if e.Type != TypeSymlink && e.Mode != g.Mode {
		sink.add(Mismatch{Path: e.Path, Field: "mode", Expected: fmt.Sprintf("%04o", e.Mode), Got: fmt.Sprintf("%04o", g.Mode)})
	}
	if e.Size != g.Size {
		sink.add(Mismatch{Path: e.Path, Field: "size", Expected: strconv.FormatInt(e.Size, 10), Got: strconv.FormatInt(g.Size, 10)})
	}
	if !opts.SkipMtime && e.Type != TypeSymlink && e.MtimeNS != g.MtimeNS {
		sink.add(Mismatch{Path: e.Path, Field: "mtime", Expected: strconv.FormatInt(e.MtimeNS, 10), Got: strconv.FormatInt(g.MtimeNS, 10)})
	}
	switch e.Type {
	case TypeSymlink:
		if e.Hash != g.Hash {
			sink.add(Mismatch{Path: e.Path, Field: "target", Expected: e.Hash, Got: g.Hash})
		}
	case TypeHardlink:
		if e.Hash != g.Hash {
			sink.add(Mismatch{Path: e.Path, Field: "hardlink", Expected: e.Hash, Got: g.Hash})
		}
	}
	return sink.count+sink.excusedCount == before
}

// StatMatches compares a stat-only walk of root with files.tsv on paths,
// types, and sizes, the check a reused dataset must pass.
func StatMatches(root string, expected []Entry) (VerifyResult, error) {
	walked, err := Walk(root)
	if err != nil {
		return VerifyResult{}, err
	}
	got := Entries(walked)
	strip := func(es []Entry) []Entry {
		out := make([]Entry, len(es))
		for i, e := range es {
			out[i] = Entry{Type: e.Type, Size: e.Size, Path: e.Path}
		}
		return out
	}
	var sink mismatchSink
	exp, g := strip(expected), strip(got)
	i, j := 0, 0
	for i < len(exp) || j < len(g) {
		switch {
		case j >= len(g) || (i < len(exp) && exp[i].Path < g[j].Path):
			sink.add(Mismatch{Path: exp[i].Path, Field: "missing"})
			i++
		case i >= len(exp) || g[j].Path < exp[i].Path:
			sink.add(Mismatch{Path: g[j].Path, Field: "extra"})
			j++
		default:
			if exp[i].Type == TypeHardlink || g[j].Type == TypeHardlink {
				// Grouping is not a stat-walk property worth failing reuse on
				// beyond type, which compareEntry reports.
				exp[i].Hash, g[j].Hash = "", ""
			}
			compareEntry(exp[i], g[j], VerifyOptions{SkipMtime: true}, &sink)
			i++
			j++
		}
	}
	return VerifyResult{Entries: len(got), MismatchCount: sink.count, Mismatches: sink.list}, nil
}

// SortStrings sorts paths bytewise.
func SortStrings(s []string) { slices.SortFunc(s, strings.Compare) }
