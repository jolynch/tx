package dataset

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// FuzzPlan checks that any mix splits any total exactly, deterministically,
// and within each part's size bounds.
func FuzzPlan(f *testing.F) {
	f.Add(int64(10<<30), int64(1), uint8(40), int64(1<<30), int64(1<<30), int64(4096), int64(256<<10))
	f.Add(int64(1), int64(7), uint8(1), int64(1), int64(1), int64(1), int64(2))
	f.Add(int64(123457), int64(-3), uint8(99), int64(1000), int64(1000), int64(3), int64(3))
	f.Fuzz(func(t *testing.T, total, seed int64, share uint8, lo1, hi1, lo2, hi2 int64) {
		total = 1 + abs64(total)%(1<<34)
		pct := 1 + int(share)%99
		norm := func(lo, hi int64) (int64, int64) {
			// Keep file counts small enough to fuzz quickly.
			floor := max(1, total/4096)
			lo = floor + abs64(lo)%(1<<24)
			hi = lo + abs64(hi)%(1<<24)
			return lo, hi
		}
		lo1, hi1 = norm(lo1, hi1)
		lo2, hi2 = norm(lo2, hi2)
		mix := Mix{
			{Source: Source{Kind: SourceRand}, SharePct: pct, Lo: lo1, Hi: lo1},
			{Source: Source{Kind: SourceRand}, SharePct: 100 - pct, Lo: lo2, Hi: hi2},
		}
		plans, err := Plan(mix, total, seed)
		if err != nil {
			t.Fatal(err)
		}
		again, _ := Plan(mix, total, seed)
		var sum int64
		for pi, p := range plans {
			if len(again[pi].Sizes) != len(p.Sizes) {
				t.Fatalf("part %d not deterministic", pi)
			}
			var partSum int64
			for i, s := range p.Sizes {
				if s <= 0 || s > p.Part.Hi {
					t.Fatalf("part %d file %d size %d outside (0, %d]", pi, i, s, p.Part.Hi)
				}
				if again[pi].Sizes[i] != s {
					t.Fatalf("part %d file %d not deterministic", pi, i)
				}
				partSum += s
			}
			if partSum != p.Budget {
				t.Fatalf("part %d sizes sum %d != budget %d", pi, partSum, p.Budget)
			}
			sum += partSum
		}
		if sum != total {
			t.Fatalf("plan sums to %d, want %d", sum, total)
		}
	})
}

func abs64(v int64) int64 {
	if v < 0 {
		if v == -v { // MinInt64
			return 0
		}
		return -v
	}
	return v
}

// FuzzFilesTSVRoundTrip checks files.tsv parsing inverts writing, and that
// the fingerprint ignores mtimes but nothing else.
func FuzzFilesTSVRoundTrip(f *testing.F) {
	f.Add(byte('f'), uint32(0o644), int64(10), "xxh128:00112233445566778899aabbccddeeff", int64(1700000000123456789), "a/b c\td.bin")
	f.Add(byte('l'), uint32(0), int64(0), "../target", int64(0), "link")
	f.Add(byte('d'), uint32(0o755), int64(0), "", int64(5), "dir")
	f.Fuzz(func(t *testing.T, typ byte, mode uint32, size int64, hash string, mtime int64, path string) {
		if !strings.ContainsRune("fdlh", rune(typ)) || path == "" || strings.ContainsAny(path, "\n\r") ||
			strings.ContainsAny(hash, "\t\n\r") || hash == "-" || size < 0 {
			t.Skip()
		}
		e := Entry{Type: typ, Mode: mode & 0o7777, Size: size, Hash: hash, MtimeNS: mtime, Path: path}
		if typ == TypeSymlink {
			e.Mode, e.MtimeNS = 0, 0
		}
		var buf bytes.Buffer
		if err := WriteFilesTSV(&buf, []Entry{e}); err != nil {
			t.Fatal(err)
		}
		got, err := ReadFilesTSV(&buf)
		if err != nil {
			t.Fatalf("read %q: %v", e.Line(), err)
		}
		if len(got) != 1 || got[0] != e {
			t.Fatalf("round trip %+v -> %+v", e, got)
		}
		moved := e
		moved.MtimeNS++
		if Fingerprint([]Entry{e}) != Fingerprint([]Entry{moved}) && typ != TypeSymlink {
			t.Fatal("fingerprint depends on mtime")
		}
		resized := e
		resized.Size++
		if Fingerprint([]Entry{e}) == Fingerprint([]Entry{resized}) {
			t.Fatal("fingerprint ignores size")
		}
	})
}

// fakeCorpus seeds a Silesia cache with synthetic files so tests never
// download the real corpus.
func fakeCorpus(t testing.TB) string {
	dir := t.TempDir()
	for i, name := range SilesiaNames {
		data := bytes.Repeat([]byte(name+" lorem ipsum "), 50+i)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func generateSmall(t testing.TB, benchDir string, mixRaw string, size int64, seed int64) (*BenchJSON, []Entry) {
	t.Helper()
	mix, err := ParseMix(mixRaw)
	if err != nil {
		t.Fatal(err)
	}
	if err := ClaimDir(benchDir, BenchMarker); err != nil {
		t.Fatal(err)
	}
	opts := GenerateOptions{BenchDir: benchDir, Seed: seed, Size: SizeSpec{Raw: FormatSize(size), Bytes: size}, Mix: mix, Jobs: 4, SilesiaCache: fakeCorpus(t)}
	req, err := opts.Resolve(0)
	if err != nil {
		t.Fatal(err)
	}
	b, entries, err := Generate(context.Background(), opts, req)
	if err != nil {
		t.Fatal(err)
	}
	return b, entries
}

const testMix = "rand=50%@4KiB,silesia:osdb+xml=30%@3000B,rand=20%@1B..2KiB"

func TestGenerateDeterministicAndSelfVerifying(t *testing.T) {
	a, ea := generateSmall(t, t.TempDir(), testMix, 64<<10, 7)
	b, _ := generateSmall(t, t.TempDir(), testMix, 64<<10, 7)
	c, _ := generateSmall(t, t.TempDir(), testMix, 64<<10, 8)
	if a.Fingerprint != b.Fingerprint {
		t.Fatalf("same seed, different fingerprints %s %s", a.Fingerprint, b.Fingerprint)
	}
	if a.Fingerprint == c.Fingerprint {
		t.Fatal("different seeds, same fingerprint")
	}
	files, bytes := Totals(ea)
	if bytes != 64<<10 || files != a.Files {
		t.Fatalf("totals %d files %d bytes; bench.json says %d files", files, bytes, a.Files)
	}
	// Every file name carries the hash of its contents.
	for _, e := range ea {
		if e.Type == TypeFile && !strings.Contains(e.Path, "."+HexOf(e.Hash)+".") {
			t.Fatalf("%s does not carry hash %s", e.Path, e.Hash)
		}
	}
}

func TestGenerateRandParts(t *testing.T) {
	// Two rand parts must not repeat each other's bytes.
	dir := t.TempDir()
	_, entries := generateSmall(t, dir, "rand=50%@1KiB,rand=50%@1KiB", 4<<10, 1)
	seen := map[string]string{}
	for _, e := range entries {
		if e.Type != TypeFile {
			continue
		}
		if prev, ok := seen[e.Hash]; ok {
			t.Fatalf("%s and %s have identical contents", prev, e.Path)
		}
		seen[e.Hash] = e.Path
	}
}

// FuzzVerifyDetectsCorruption flips one byte, one mode bit, or one mtime in
// a generated dataset; the oracle must report it.
func FuzzVerifyDetectsCorruption(f *testing.F) {
	benchDir := f.TempDir()
	_, entries := generateSmall(f, benchDir, testMix, 32<<10, 3)
	root := filepath.Join(benchDir, DataDirName)
	var files []Entry
	for _, e := range entries {
		if e.Type == TypeFile && e.Size > 0 {
			files = append(files, e)
		}
	}
	if res, err := Verify(context.Background(), root, entries, VerifyOptions{Content: true}); err != nil || !res.OK() {
		f.Fatalf("pristine dataset fails: %v %+v", err, res.Mismatches)
	}
	f.Add(uint16(0), uint32(0), uint8(0), uint8(0))
	f.Add(uint16(3), uint32(4095), uint8(1), uint8(7))
	f.Add(uint16(9), uint32(1), uint8(2), uint8(1))
	f.Add(uint16(1), uint32(0), uint8(3), uint8(0))
	f.Add(uint16(2), uint32(0), uint8(4), uint8(0))
	f.Fuzz(func(t *testing.T, fileSel uint16, off uint32, kind uint8, bit uint8) {
		e := files[int(fileSel)%len(files)]
		path := filepath.Join(root, e.Path)
		var restore func()
		var field string
		switch kind % 5 {
		case 0: // flip one bit of one byte
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			i := int(off) % len(data)
			data[i] ^= 1 << (bit % 8)
			writeKeepMtime(t, path, data)
			restore = func() { data[i] ^= 1 << (bit % 8); writeKeepMtime(t, path, data) }
			field = "hash"
		case 1: // flip one permission bit
			m := os.FileMode(e.Mode ^ (1 << (bit % 9)))
			if err := os.Chmod(path, m); err != nil {
				t.Fatal(err)
			}
			restore = func() { _ = os.Chmod(path, os.FileMode(e.Mode)) }
			field = "mode"
		case 2: // move the mtime by 1ns..256ns
			mt := time.Unix(0, e.MtimeNS+int64(bit)+1)
			if err := os.Chtimes(path, mt, mt); err != nil {
				t.Fatal(err)
			}
			restore = func() { mt := time.Unix(0, e.MtimeNS); _ = os.Chtimes(path, mt, mt) }
			field = "mtime"
		case 3: // an extra file
			extra := path + ".extra"
			if err := os.WriteFile(extra, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Dir(path)
			st, _ := os.Stat(dir)
			restore = func() { os.Remove(extra); _ = os.Chtimes(dir, st.ModTime(), st.ModTime()) }
			field = "extra"
		case 4: // a missing file
			data, _ := os.ReadFile(path)
			if err := os.Rename(path, path+".gone"); err != nil {
				t.Fatal(err)
			}
			_ = data
			dir := filepath.Dir(path)
			st, _ := os.Stat(dir)
			restore = func() { _ = os.Rename(path+".gone", path); _ = os.Chtimes(dir, st.ModTime(), st.ModTime()) }
			field = "missing"
		}
		res, err := Verify(context.Background(), root, entries, VerifyOptions{Content: true, Jobs: 2})
		restore()
		if err != nil {
			t.Fatal(err)
		}
		if res.OK() {
			t.Fatalf("corruption (%s) of %s went undetected", field, e.Path)
		}
		found := false
		for _, m := range res.Mismatches {
			if m.Field == field && strings.HasPrefix(m.Path, e.Path) {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected a %s mismatch on %s, got %v", field, e.Path, res.Mismatches)
		}
	})
}

func writeKeepMtime(t *testing.T, path string, data []byte) {
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, st.Mode()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, st.ModTime(), st.ModTime()); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyNamesSkipsContent(t *testing.T) {
	benchDir := t.TempDir()
	_, entries := generateSmall(t, benchDir, "rand=100%@1KiB", 4<<10, 1)
	root := filepath.Join(benchDir, DataDirName)
	for _, e := range entries {
		if e.Type == TypeFile {
			writeKeepMtime(t, filepath.Join(root, e.Path), make([]byte, e.Size))
			break
		}
	}
	names, err := Verify(context.Background(), root, entries, VerifyOptions{})
	if err != nil || !names.OK() {
		t.Fatalf("names oracle should not read contents: %v %v", err, names.Mismatches)
	}
	full, _ := Verify(context.Background(), root, entries, VerifyOptions{Content: true})
	if full.OK() {
		t.Fatal("full oracle missed the zeroed file")
	}
}

func TestImportHardlinksSymlinksAndReuse(t *testing.T) {
	parent := t.TempDir()
	in := filepath.Join(parent, "photos")
	benchDir := filepath.Join(parent, "bench")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(in, "sub"), 0o755))
	must(os.WriteFile(filepath.Join(in, "a.txt"), []byte("hello"), 0o600))
	must(os.Link(filepath.Join(in, "a.txt"), filepath.Join(in, "sub", "z-link")))
	must(os.Symlink("../a.txt", filepath.Join(in, "sub", "sym")))
	must(os.WriteFile(filepath.Join(in, "sub", "b"), []byte("world!"), 0o644))
	must(ClaimDir(benchDir, BenchMarker))

	b, entries, err := Import(context.Background(), ImportOptions{BenchDir: benchDir, In: in, Jobs: 2})
	must(err)
	types := map[string]byte{}
	for _, e := range entries {
		types[e.Path] = e.Type
	}
	if types["a.txt"] != TypeFile || types["sub/z-link"] != TypeHardlink || types["sub/sym"] != TypeSymlink || types["sub"] != TypeDir {
		t.Fatalf("unexpected types %v", types)
	}
	if b.Remote.Bench != "/bench" || b.Remote.Data != "/photos" {
		t.Fatalf("remote paths %+v", b.Remote)
	}
	if b.In.ZstdRatioEst <= 0 || b.Files != 3 || b.Bytes != 16 {
		t.Fatalf("bench.json %+v %+v", b, b.In)
	}
	d, err := Load(benchDir)
	must(err)
	must(d.CheckStat())

	// A preserved mtime does not hide a content change: ctime moves.
	st, _ := os.Stat(filepath.Join(in, "sub", "b"))
	time.Sleep(10 * time.Millisecond)
	must(os.WriteFile(filepath.Join(in, "sub", "b"), []byte("WORLD!"), 0o644))
	must(os.Chtimes(filepath.Join(in, "sub", "b"), st.ModTime(), st.ModTime()))
	changed, err := SourceChanges(benchDir, in)
	must(err)
	if len(changed) != 1 || changed[0] != "sub/b" {
		t.Fatalf("changed = %v", changed)
	}
	if d.CheckStat() == nil {
		t.Fatal("CheckStat missed a changed source")
	}
	b2, entries2, err := Import(context.Background(), ImportOptions{BenchDir: benchDir, In: in})
	must(err)
	if b2.Fingerprint == b.Fingerprint {
		t.Fatal("re-import did not pick up the change")
	}
	res, err := Verify(context.Background(), in, entries2, VerifyOptions{Content: true})
	must(err)
	if !res.OK() {
		t.Fatalf("re-imported tree fails its own oracle: %v", res.Mismatches)
	}
}

func TestImportRejectsNestingAndSpecialFiles(t *testing.T) {
	parent := t.TempDir()
	if err := CheckNotNested(parent, filepath.Join(parent, "x")); err == nil {
		t.Fatal("nested in accepted")
	}
	if err := CheckNotNested(filepath.Join(parent, "x"), parent); err == nil {
		t.Fatal("nested bench accepted")
	}
	in := filepath.Join(parent, "in")
	if err := os.MkdirAll(in, 0o755); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(in, "pipe")
	if err := mkfifo(fifo); err != nil {
		t.Skip("mkfifo:", err)
	}
	_, err := Walk(in)
	var unsupported *UnsupportedError
	if !errors.As(err, &unsupported) || unsupported.Paths[0] != "pipe" {
		t.Fatalf("Walk = %v", err)
	}
}

func TestClaimDir(t *testing.T) {
	dir := t.TempDir()
	unmarked := filepath.Join(dir, "data")
	if err := os.MkdirAll(unmarked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unmarked, "precious"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ClaimDir(unmarked, DstMarker); !errors.Is(err, ErrUnmarked) {
		t.Fatalf("ClaimDir on unmarked non-empty dir = %v", err)
	}
	fresh := filepath.Join(dir, "fresh")
	if err := ClaimDir(fresh, DstMarker); err != nil || !IsMarked(fresh, DstMarker) {
		t.Fatalf("ClaimDir on absent dir = %v", err)
	}
	if err := ClaimDir(fresh, DstMarker); err != nil {
		t.Fatalf("ClaimDir on marked dir = %v", err)
	}
	file := filepath.Join(dir, "tx-bench")
	if err := os.WriteFile(file, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ClaimDir(file, BenchMarker); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("ClaimDir on a file = %v", err)
	}
}

func TestReuseDetection(t *testing.T) {
	benchDir := t.TempDir()
	generateSmall(t, benchDir, "rand=100%@1KiB", 8<<10, 1)
	d, err := Load(benchDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CheckStat(); err != nil {
		t.Fatal(err)
	}
	mix, _ := ParseMix("rand=100%@1024B")
	opts := GenerateOptions{BenchDir: benchDir, Seed: 1, Size: SizeSpec{Bytes: 8 << 10}, Mix: mix}
	req, _ := opts.Resolve(0)
	if diff := req.Matches(d.Bench); diff != "" {
		t.Fatalf("same shape in other units reported as different: %s", diff)
	}
	opts.Seed = 2
	req, _ = opts.Resolve(0)
	if req.Matches(d.Bench) == "" {
		t.Fatal("seed change not detected")
	}
	// Truncating a file breaks the stat walk.
	for _, e := range d.Entries {
		if e.Type == TypeFile {
			if err := os.Truncate(filepath.Join(d.DataRoot(), e.Path), 1); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if d.CheckStat() == nil {
		t.Fatal("CheckStat missed a truncated file")
	}
}

func TestGrammars(t *testing.T) {
	good := map[string]string{
		"rand=100%@1GiB": "rand=100%@1GiB",
		"rand=40%@1024MiB,silesia=60%@1KiB..2MiB": "rand=40%@1GiB,silesia=60%@1KiB..2MiB",
		"silesia:osdb+xml=100%@1000":              "silesia:osdb+xml=100%@1000B",
	}
	for in, want := range good {
		m, err := ParseMix(in)
		if err != nil || m.String() != want {
			t.Errorf("ParseMix(%q) = %v, %v; want %s", in, m, err, want)
		}
	}
	for _, bad := range []string{"", "rand=50%@1KiB", "rand=100%", "foo=100%@1", "silesia:nope=100%@1",
		"rand=100%@2KiB..1KiB", "rand:x=100%@1", "rand=0%@1,rand=100%@1", "rand=100@1"} {
		if _, err := ParseMix(bad); err == nil {
			t.Errorf("ParseMix(%q) accepted", bad)
		}
	}
	for _, p := range ProfileNames {
		if _, err := ResolveMix(p, ""); err != nil {
			t.Errorf("profile %s: %v", p, err)
		}
	}
	if s, err := ParseSize("25%mem"); err != nil || s.MemPct != 25 {
		t.Errorf("ParseSize 25%%mem = %+v %v", s, err)
	}
	if s, err := ParseSize("200%mem"); err != nil || s.MemPct != 200 {
		t.Errorf("ParseSize 200%%mem = %+v %v", s, err)
	}
	for _, bad := range []string{"0", "101%disk", "x", "-1GiB"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("ParseSize(%q) accepted", bad)
		}
	}
	if _, err := ParseFill("50"); err == nil {
		t.Error("ParseFill accepted a bare number")
	}
}
