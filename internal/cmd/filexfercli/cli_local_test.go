package filexfercli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/pagecache"
)

// writeLocalTestFile creates a file (and any parent dirs) with the given bytes.
func writeLocalTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// patternBytes returns n deterministic, non-repeating-ish bytes that exercise
// the copy_file_range loop (size not page-aligned) and content verification.
func patternBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func buildLocalSourceTree(t *testing.T, src string) []byte {
	t.Helper()
	big := patternBytes(5*1024*1024 + 123) // exercises the loop + a trailing partial copy
	writeLocalTestFile(t, filepath.Join(src, "big.bin"), big)
	writeLocalTestFile(t, filepath.Join(src, "sub", "hello.txt"), []byte("hello world\n"))
	writeLocalTestFile(t, filepath.Join(src, "empty.bin"), nil)
	if err := os.MkdirAll(filepath.Join(src, "sub", "deep"), 0o755); err != nil {
		t.Fatalf("mkdir deep: %v", err)
	}
	if err := os.Symlink("../hello.txt", filepath.Join(src, "sub", "deep", "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.Link(filepath.Join(src, "sub", "hello.txt"), filepath.Join(src, "sub", "hardlink.txt")); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	return big
}

func assertLocalTreeCopied(t *testing.T, src, dst string, big []byte) {
	t.Helper()
	// Regular file contents.
	if got := mustReadFile(t, filepath.Join(dst, "big.bin")); !bytes.Equal(got, big) {
		t.Fatalf("big.bin content mismatch: got %d bytes want %d", len(got), len(big))
	}
	if got := mustReadFile(t, filepath.Join(dst, "sub", "hello.txt")); string(got) != "hello world\n" {
		t.Fatalf("hello.txt content mismatch: %q", got)
	}
	// Empty file present with size 0.
	if info, err := os.Stat(filepath.Join(dst, "empty.bin")); err != nil || info.Size() != 0 {
		t.Fatalf("empty.bin missing or non-empty: info=%v err=%v", info, err)
	}
	// Mode preserved (compare src vs dst to be umask-independent).
	srcInfo, err := os.Stat(filepath.Join(src, "big.bin"))
	if err != nil {
		t.Fatalf("stat src big.bin: %v", err)
	}
	dstInfo, err := os.Stat(filepath.Join(dst, "big.bin"))
	if err != nil {
		t.Fatalf("stat dst big.bin: %v", err)
	}
	if srcInfo.Mode() != dstInfo.Mode() {
		t.Fatalf("big.bin mode mismatch: src=%v dst=%v", srcInfo.Mode(), dstInfo.Mode())
	}
	// Symlink preserved.
	if target, err := os.Readlink(filepath.Join(dst, "sub", "deep", "link.txt")); err != nil || target != "../hello.txt" {
		t.Fatalf("symlink target mismatch: got %q err=%v", target, err)
	}
	// Hardlink shares an inode with its primary.
	primary, err := os.Stat(filepath.Join(dst, "sub", "hello.txt"))
	if err != nil {
		t.Fatalf("stat primary: %v", err)
	}
	link, err := os.Stat(filepath.Join(dst, "sub", "hardlink.txt"))
	if err != nil {
		t.Fatalf("stat hardlink: %v", err)
	}
	if !os.SameFile(primary, link) {
		t.Fatalf("hardlink does not share inode with primary")
	}
}

func TestRunCLILocalCopyTree(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	big := buildLocalSourceTree(t, src)

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--verify=full", src, dst}, &stdout, &stderr); code != 0 {
		t.Fatalf("local copy failed code=%d stderr=%s", code, stderr.String())
	}
	assertLocalTreeCopied(t, src, dst, big)
	if !strings.Contains(stderr.String(), "local-verify-meta: [ok]") {
		t.Fatalf("expected meta verify ok, stderr=%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "local-verify-data: [ok]") {
		t.Fatalf("expected data verify ok, stderr=%s", stderr.String())
	}
}

func TestRunCLILocalCopyFileScheme(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	big := buildLocalSourceTree(t, src)

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--verify=meta", "file://" + src, dst}, &stdout, &stderr); code != 0 {
		t.Fatalf("file:// local copy failed code=%d stderr=%s", code, stderr.String())
	}
	assertLocalTreeCopied(t, src, dst, big)
}

func TestRunCLILocalCopyRelativeSrc(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	big := buildLocalSourceTree(t, src)
	t.Chdir(base)

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--verify=meta", "src", "dst_rel"}, &stdout, &stderr); code != 0 {
		t.Fatalf("relative local copy failed code=%d stderr=%s", code, stderr.String())
	}
	assertLocalTreeCopied(t, src, filepath.Join(base, "dst_rel"), big)
}

func TestRunCLILocalGetSingleFile(t *testing.T) {
	base := t.TempDir()
	srcFile := filepath.Join(base, "big.bin")
	data := patternBytes(2*1024*1024 + 77)
	writeLocalTestFile(t, srcFile, data)
	dstFile := filepath.Join(base, "copy.bin")

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"get", srcFile, dstFile}, &stdout, &stderr); code != 0 {
		t.Fatalf("local get failed code=%d stderr=%s", code, stderr.String())
	}
	if got := mustReadFile(t, dstFile); !bytes.Equal(got, data) {
		t.Fatalf("get content mismatch: got %d want %d bytes", len(got), len(data))
	}
}

func TestRunCLILocalGetDefaultBasename(t *testing.T) {
	base := t.TempDir()
	srcFile := filepath.Join(base, "thing.dat")
	data := []byte("default-basename payload")
	writeLocalTestFile(t, srcFile, data)
	outDir := t.TempDir()
	t.Chdir(outDir)

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"get", srcFile}, &stdout, &stderr); code != 0 {
		t.Fatalf("local get (default dst) failed code=%d stderr=%s", code, stderr.String())
	}
	if got := mustReadFile(t, filepath.Join(outDir, "thing.dat")); !bytes.Equal(got, data) {
		t.Fatalf("default-basename get content mismatch: %q", got)
	}
}

func TestRunCLILocalGetRejectsDir(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "adir"), 0o755); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"get", filepath.Join(base, "adir"), filepath.Join(base, "out")}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected exit 2 getting a directory, got %d", code)
	}
	if !strings.Contains(stderr.String(), "is a directory") {
		t.Fatalf("expected directory rejection message, got %s", stderr.String())
	}
}

func TestRunCLILocalRejectsGentle(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("a"))

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--mode", "gentle", src, filepath.Join(base, "dst")}, &stdout, &stderr); code != 2 {
		t.Fatalf("expected exit 2 for gentle local copy, got %d", code)
	}
	if !strings.Contains(stderr.String(), "gentle") || !strings.Contains(stderr.String(), "local") {
		t.Fatalf("expected gentle/local rejection, got %s", stderr.String())
	}
}

func TestRunCLILocalSkipWrite(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.bin"), patternBytes(1024*1024))
	writeLocalTestFile(t, filepath.Join(src, "b.txt"), []byte("hi"))

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--verify=none", "--skip-write", src, dst}, &stdout, &stderr); code != 0 {
		t.Fatalf("skip-write local copy failed code=%d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("expected dst to not exist after --skip-write, err=%v", err)
	}
	// No staging directory should remain behind in the parent.
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tx-local-") {
			t.Fatalf("leftover staging directory: %s", e.Name())
		}
	}
}

func TestRunCLILocalVerifyMetaDetectsTamper(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("aaaa"))
	writeLocalTestFile(t, filepath.Join(src, "b.txt"), []byte("bbbb"))

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--verify=meta", src, dst}, &stdout, &stderr); code != 0 {
		t.Fatalf("local copy failed code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "local-verify-meta: [ok]") {
		t.Fatalf("expected meta ok on clean copy, got %s", stderr.String())
	}

	// Change a destination file's size so the meta comparison diverges.
	if err := os.WriteFile(filepath.Join(dst, "a.txt"), []byte("aaaaXXXX"), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, _, _, err := enumerateLocalSource(src)
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}
	var verr bytes.Buffer
	if code := verifyLocalCopy(src, dst, entries, copyCLIConfig{verifyMeta: true}, 2, &verr); code == 0 {
		t.Fatalf("expected verify failure after tamper, got 0; out=%s", verr.String())
	}
	if !strings.Contains(verr.String(), "local-verify-meta: [fail]") {
		t.Fatalf("expected meta fail line, got %s", verr.String())
	}
}

func TestRunCLILocalConcurrency(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	for i := 0; i < 50; i++ {
		writeLocalTestFile(t, filepath.Join(src, "d", string(rune('a'+i%26)), patternName(i)), patternBytes(1000+i))
	}
	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--verify=full", "--concurrency", "8", src, dst}, &stdout, &stderr); code != 0 {
		t.Fatalf("concurrent local copy failed code=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "local-verify-data: [ok]") {
		t.Fatalf("expected data verify ok, got %s", stderr.String())
	}
}

func patternName(i int) string {
	return "f" + strings.Repeat("x", i%5) + string(rune('0'+i%10)) + ".bin"
}

// TestLocalSuggestedConcurrencyFormula confirms the local default uses the
// server-side io-depth x cores formula (clamped to [2,256]), not NumCPU*2.
func TestLocalSuggestedConcurrencyFormula(t *testing.T) {
	if got := tx.LocalSuggestedConcurrency(8, tx.DefaultTargetIODepth); got != 32 {
		t.Fatalf("LocalSuggestedConcurrency(8,4)=%d want 32", got)
	}
	if got := tx.LocalSuggestedConcurrency(1, tx.DefaultTargetIODepth); got != 4 {
		t.Fatalf("LocalSuggestedConcurrency(1,4)=%d want 4", got)
	}
	if got := tx.LocalSuggestedConcurrency(1000, tx.DefaultTargetIODepth); got != 256 {
		t.Fatalf("LocalSuggestedConcurrency(1000,4)=%d want 256 (clamped)", got)
	}
	if tx.DefaultTargetIODepth != 4 {
		t.Fatalf("DefaultTargetIODepth=%d want 4", tx.DefaultTargetIODepth)
	}
}

// TestLocalProgressUsesTxferFormat locks the local periodic progress line to the
// shared remote "txfer-progress:" format, with no trailing link suffix.
func TestLocalProgressUsesTxferFormat(t *testing.T) {
	line := formatTxferProgressLine(5, 10, 500, 1000, 250, fixedWidthETA(2*time.Second), "")
	if !strings.HasPrefix(line, "txfer-progress:[") {
		t.Fatalf("expected txfer-progress prefix, got %q", line)
	}
	for _, sub := range []string{"](", " [", "[eta:", "]@[", "/s]"} {
		if !strings.Contains(line, sub) {
			t.Fatalf("progress line missing %q: %q", sub, line)
		}
	}
	if strings.Contains(line, "of link=") {
		t.Fatalf("local progress must not include a link suffix: %q", line)
	}
	// A non-empty suffix (the remote link segment) is appended verbatim.
	withLink := formatTxferProgressLine(5, 10, 500, 1000, 250, fixedWidthETA(2*time.Second), " (50% of link=1.00 GiB/s @  2s)")
	if !strings.HasSuffix(withLink, " (50% of link=1.00 GiB/s @  2s)") {
		t.Fatalf("expected link suffix appended, got %q", withLink)
	}
}

func TestRunCLILocalSyncConverged(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("alpha"))
	var stdout, stderr bytes.Buffer
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("fresh copy failed: %d %s", c, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("re-copy failed: %d %s", c, stderr.String())
	}
	if !strings.Contains(stderr.String(), "converged, nothing to do") {
		t.Fatalf("expected converged message, got %s", stderr.String())
	}
}

func TestRunCLILocalSyncConvergedRunsVerify(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("alpha"))
	var stdout, stderr bytes.Buffer
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("fresh copy failed: %d %s", c, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	// Re-run on a converged tree with --verify: verification must still run
	// (matching the remote copy, which verifies even when sync is a no-op).
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=meta", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("re-copy failed: %d %s", c, stderr.String())
	}
	if !strings.Contains(stderr.String(), "converged, nothing to do") {
		t.Fatalf("expected converged message, got %s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "local-verify-meta: [ok]") {
		t.Fatalf("expected verify to run on converged tree, got %s", stderr.String())
	}
}

func TestRunCLILocalSyncOnlyNewProceeds(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("alpha"))
	var stdout, stderr bytes.Buffer
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("fresh copy failed: %d %s", c, stderr.String())
	}
	// Force "terminal" so that, if the only-new path wrongly prompted, it would
	// be visible in the output. It must not prompt for pure additions.
	withSyncPromptTestInput(t, "", true)
	writeLocalTestFile(t, filepath.Join(src, "new.txt"), []byte("new-content"))
	stdout.Reset()
	stderr.Reset()
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=meta", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("only-new sync failed: %d %s", c, stderr.String())
	}
	if strings.Contains(stderr.String(), "proceed?") {
		t.Fatalf("only-new delta must not prompt, got %s", stderr.String())
	}
	if got := mustReadFile(t, filepath.Join(dst, "new.txt")); string(got) != "new-content" {
		t.Fatalf("new file not synced: %q", got)
	}
}

func TestRunCLILocalSyncDestructiveYes(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("alpha"))
	writeLocalTestFile(t, filepath.Join(src, "gone.txt"), []byte("x"))
	var stdout, stderr bytes.Buffer
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("fresh copy failed: %d %s", c, stderr.String())
	}
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("ALPHA-CHANGED-LONGER"))
	if err := os.Remove(filepath.Join(src, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	writeLocalTestFile(t, filepath.Join(src, "new.txt"), []byte("brand new"))
	stdout.Reset()
	stderr.Reset()
	if c := RunCLI([]string{"copy", "--progress=false", "--yes", "--verify=meta", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("destructive sync -y failed: %d %s", c, stderr.String())
	}
	if !strings.Contains(stderr.String(), "local-verify-meta: [ok]") {
		t.Fatalf("expected meta ok after sync, got %s", stderr.String())
	}
	if got := mustReadFile(t, filepath.Join(dst, "a.txt")); string(got) != "ALPHA-CHANGED-LONGER" {
		t.Fatalf("stale file not overwritten: %q", got)
	}
	if _, err := os.Stat(filepath.Join(dst, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("removed file still present: %v", err)
	}
	if got := mustReadFile(t, filepath.Join(dst, "new.txt")); string(got) != "brand new" {
		t.Fatalf("new file not synced: %q", got)
	}
}

func TestRunCLILocalSyncPromptAbort(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("alpha"))
	var stdout, stderr bytes.Buffer
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("fresh copy failed: %d %s", c, stderr.String())
	}
	// Destructive change + answer "n" at the prompt => abort, no mutation.
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("CHANGED"))
	withSyncPromptTestInput(t, "n\n", true)
	stdout.Reset()
	stderr.Reset()
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("expected exit 0 on abort, got %d %s", c, stderr.String())
	}
	if !strings.Contains(stderr.String(), "aborted") {
		t.Fatalf("expected 'aborted', got %s", stderr.String())
	}
	if got := mustReadFile(t, filepath.Join(dst, "a.txt")); string(got) != "alpha" {
		t.Fatalf("abort must not mutate destination, got %q", got)
	}
}

func TestRunCLILocalSyncPromptYes(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("alpha"))
	var stdout, stderr bytes.Buffer
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=none", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("fresh copy failed: %d %s", c, stderr.String())
	}
	writeLocalTestFile(t, filepath.Join(src, "a.txt"), []byte("CHANGED-AND-LONGER"))
	withSyncPromptTestInput(t, "y\n", true)
	stdout.Reset()
	stderr.Reset()
	if c := RunCLI([]string{"copy", "--progress=false", "--verify=meta", src, dst}, &stdout, &stderr); c != 0 {
		t.Fatalf("confirmed sync failed: %d %s", c, stderr.String())
	}
	if got := mustReadFile(t, filepath.Join(dst, "a.txt")); string(got) != "CHANGED-AND-LONGER" {
		t.Fatalf("confirmed sync did not apply change: %q", got)
	}
}

func TestRunCLILocalCacheLoad(t *testing.T) {
	if !pagecache.TouchSupported() {
		t.Skip("page-cache touch not supported on this platform")
	}
	base := t.TempDir()
	src := filepath.Join(base, "src")
	dst := filepath.Join(base, "dst")
	srcFile := filepath.Join(src, "warm.bin")
	writeLocalTestFile(t, srcFile, patternBytes(4*1024*1024))
	// Warm the source's page cache so there is residency to replicate.
	if data, err := os.ReadFile(srcFile); err != nil || len(data) == 0 {
		t.Fatalf("warm read: %v", err)
	}

	var stdout, stderr bytes.Buffer
	if code := RunCLI([]string{"copy", "--progress=false", "--cache-load", "full", "--verify=none", src, dst}, &stdout, &stderr); code != 0 {
		t.Fatalf("cache-load local copy failed code=%d stderr=%s", code, stderr.String())
	}
	if got := mustReadFile(t, filepath.Join(dst, "warm.bin")); len(got) != 4*1024*1024 {
		t.Fatalf("warm.bin size mismatch: %d", len(got))
	}
	if !strings.Contains(stderr.String(), "cache-load:") {
		t.Fatalf("expected cache-load report, got %s", stderr.String())
	}
	// The destination file should now have resident pages mirrored from src.
	var dstCE pagecache.CacheEntry
	if err := dstCE.Load(filepath.Join(dst, "warm.bin")); err != nil {
		t.Fatalf("load dst residency: %v", err)
	}
	if dstCE.NumResidentPages() == 0 {
		t.Fatalf("expected destination to have resident pages after cache-load")
	}
}
