package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestParseGenerateConfigDefaultsToRand(t *testing.T) {
	cfg, err := parseGenerateConfig([]string{"bench/data/src:2@16B"})
	if err != nil {
		t.Fatalf("parseGenerateConfig failed: %v", err)
	}
	if cfg.source.kind != "rand" {
		t.Fatalf("source.kind = %q, want rand", cfg.source.kind)
	}
	if len(cfg.specs) != 1 {
		t.Fatalf("len(specs) = %d, want 1", len(cfg.specs))
	}
}

func TestRunGenerateHelpReturnsNil(t *testing.T) {
	if err := runGenerate([]string{"-h"}); err != nil {
		t.Fatalf("runGenerate(-h) failed: %v", err)
	}
}

func TestRunGenerateSilesiaCacheReuseLogsProgress(t *testing.T) {
	cacheDir := t.TempDir()
	restore := setSilesiaTestGlobals(t, cacheDir, "", nil)
	defer restore()

	var logs bytes.Buffer
	restoreLogs := setGenerateLogOutputForTest(&logs)
	defer restoreLogs()

	if err := os.WriteFile(filepath.Join(cacheDir, "osdb"), []byte("cached"), 0o644); err != nil {
		t.Fatalf("write osdb cache file: %v", err)
	}

	outdir := filepath.Join(t.TempDir(), "src")
	spec := fmt.Sprintf("%s:1@6B", outdir)
	if err := runGenerate([]string{"-source", "silesia:osdb", spec}); err != nil {
		t.Fatalf("runGenerate failed: %v", err)
	}

	got := logs.String()
	for _, want := range []string{
		"generate: source=silesia:osdb specs=1",
		"generate: using silesia cache dir:",
		"generate: reusing cached silesia corpus osdb",
		"generate: generating 1 file(s) of 6 B into",
		"generate: progress 1/1 files:",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("logs missing %q\nlogs:\n%s", want, got)
		}
	}
}

func TestParseGenerateConfigSilesiaSource(t *testing.T) {
	cfg, err := parseGenerateConfig([]string{"-source", "silesia:osdb,nci", "bench/data/src:2@16B"})
	if err != nil {
		t.Fatalf("parseGenerateConfig failed: %v", err)
	}
	want := sourceSpec{kind: "silesia", names: []string{"osdb", "nci"}}
	if !reflect.DeepEqual(cfg.source, want) {
		t.Fatalf("source = %#v, want %#v", cfg.source, want)
	}
}

func TestParseSourceSpecRejectsInvalidValues(t *testing.T) {
	cases := []string{
		"silesia:",
		"silesia:osdb,",
		"silesia:nope",
		"zip:osdb",
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc, func(t *testing.T) {
			if _, err := parseSourceSpec(tc); err == nil {
				t.Fatalf("parseSourceSpec(%q) succeeded, want error", tc)
			}
		})
	}
}

func TestRunGenerateRandDeterministic(t *testing.T) {
	root := t.TempDir()
	outdir := filepath.Join(root, "src")
	spec := fmt.Sprintf("%s:2@16B", outdir)
	if err := runGenerate([]string{spec}); err != nil {
		t.Fatalf("runGenerate failed: %v", err)
	}

	got0, err := os.ReadFile(filepath.Join(outdir, "file-0.bin"))
	if err != nil {
		t.Fatalf("read file-0.bin: %v", err)
	}
	got1, err := os.ReadFile(filepath.Join(outdir, "file-1.bin"))
	if err != nil {
		t.Fatalf("read file-1.bin: %v", err)
	}

	rng := rand.New(rand.NewSource(1))
	want0 := make([]byte, 16)
	want1 := make([]byte, 16)
	_, _ = rng.Read(want0)
	_, _ = rng.Read(want1)
	if !bytes.Equal(got0, want0) {
		t.Fatalf("file-0.bin mismatch")
	}
	if !bytes.Equal(got1, want1) {
		t.Fatalf("file-1.bin mismatch")
	}
}

func TestRunGenerateSilesiaSingleSourceRepeatsAndTruncates(t *testing.T) {
	cacheDir := t.TempDir()
	restore := setSilesiaTestGlobals(t, cacheDir, "", nil)
	defer restore()

	if err := os.WriteFile(filepath.Join(cacheDir, "osdb"), []byte("abc"), 0o644); err != nil {
		t.Fatalf("write osdb cache file: %v", err)
	}

	outdir := filepath.Join(t.TempDir(), "src")
	spec := fmt.Sprintf("%s:2@8B", outdir)
	if err := runGenerate([]string{"-source", "silesia:osdb", spec}); err != nil {
		t.Fatalf("runGenerate failed: %v", err)
	}

	for _, name := range []string{"osdb-0.bin", "osdb-1.bin"} {
		got, err := os.ReadFile(filepath.Join(outdir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(got) != "abcabcab" {
			t.Fatalf("%s = %q, want %q", name, string(got), "abcabcab")
		}
	}
}

func TestRunGenerateSilesiaMultiSourceRoundRobin(t *testing.T) {
	cacheDir := t.TempDir()
	restore := setSilesiaTestGlobals(t, cacheDir, "", nil)
	defer restore()

	if err := os.WriteFile(filepath.Join(cacheDir, "osdb"), []byte("ab"), 0o644); err != nil {
		t.Fatalf("write osdb cache file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, "nci"), []byte("XYZ"), 0o644); err != nil {
		t.Fatalf("write nci cache file: %v", err)
	}

	outdir := filepath.Join(t.TempDir(), "src")
	spec := fmt.Sprintf("%s:3@5B", outdir)
	if err := runGenerate([]string{"-source", "silesia:osdb,nci", spec}); err != nil {
		t.Fatalf("runGenerate failed: %v", err)
	}

	assertFileContents(t, filepath.Join(outdir, "osdb-0.bin"), "ababa")
	assertFileContents(t, filepath.Join(outdir, "nci-0.bin"), "XYZXY")
	assertFileContents(t, filepath.Join(outdir, "osdb-1.bin"), "ababa")
}

func TestRunGenerateSilesiaCacheReuseSkipsDownload(t *testing.T) {
	cacheDir := t.TempDir()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	restore := setSilesiaTestGlobals(t, cacheDir, server.URL, server.Client())
	defer restore()

	if err := os.WriteFile(filepath.Join(cacheDir, "osdb"), []byte("cached"), 0o644); err != nil {
		t.Fatalf("write osdb cache file: %v", err)
	}

	outdir := filepath.Join(t.TempDir(), "src")
	spec := fmt.Sprintf("%s:1@6B", outdir)
	if err := runGenerate([]string{"-source", "silesia:osdb", spec}); err != nil {
		t.Fatalf("runGenerate failed: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}

func TestRunGenerateSilesiaDownloadsOnlyRequestedEntries(t *testing.T) {
	cacheDir := t.TempDir()
	archive := makeSilesiaZip(t, map[string]string{
		"osdb":  "osdb-data",
		"nci":   "nci-data",
		"extra": "ignored",
	})
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
		_, _ = w.Write(archive)
	}))
	defer server.Close()

	restore := setSilesiaTestGlobals(t, cacheDir, server.URL, server.Client())
	defer restore()
	var logs bytes.Buffer
	restoreLogs := setGenerateLogOutputForTest(&logs)
	defer restoreLogs()

	outdir := filepath.Join(t.TempDir(), "src")
	spec := fmt.Sprintf("%s:2@10B", outdir)
	if err := runGenerate([]string{"-source", "silesia:osdb,nci", spec}); err != nil {
		t.Fatalf("runGenerate failed: %v", err)
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", cacheDir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	if !reflect.DeepEqual(names, []string{"nci", "osdb"}) {
		t.Fatalf("cache entries = %v, want [nci osdb]", names)
	}
	assertFileContents(t, filepath.Join(outdir, "osdb-0.bin"), strings.Repeat("osdb-data", 2)[:10])
	assertFileContents(t, filepath.Join(outdir, "nci-0.bin"), strings.Repeat("nci-data", 2)[:10])
	gotLogs := logs.String()
	for _, want := range []string{
		"generate: missing silesia corpus file(s): osdb, nci",
		"generate: downloading silesia archive from",
		"generate: downloaded silesia archive:",
		"generate: extracting silesia corpus osdb",
		"generate: extracting silesia corpus nci",
	} {
		if !strings.Contains(gotLogs, want) {
			t.Fatalf("logs missing %q\nlogs:\n%s", want, gotLogs)
		}
	}
}

func TestRunGenerateSilesiaFromBenchDirUsesDataSilesia(t *testing.T) {
	root := t.TempDir()
	benchDir := filepath.Join(root, "bench")
	if err := os.MkdirAll(filepath.Join(benchDir, "data", "src"), 0o755); err != nil {
		t.Fatalf("MkdirAll data/src: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(benchDir, "data", "silesia"), 0o755); err != nil {
		t.Fatalf("MkdirAll data/silesia: %v", err)
	}
	if err := os.WriteFile(filepath.Join(benchDir, "bench"), []byte("binary placeholder"), 0o755); err != nil {
		t.Fatalf("WriteFile bench binary placeholder: %v", err)
	}
	if err := os.WriteFile(filepath.Join(benchDir, "data", "silesia", "osdb"), []byte("abc"), 0o644); err != nil {
		t.Fatalf("WriteFile cached osdb: %v", err)
	}

	prevCacheDir := silesiaCacheDir
	defer func() { silesiaCacheDir = prevCacheDir }()
	silesiaCacheDir = filepath.Join("bench", "data", "silesia")
	t.Chdir(benchDir)

	if err := runGenerate([]string{"-source", "silesia:osdb", "data/src:1@5B"}); err != nil {
		t.Fatalf("runGenerate from bench dir failed: %v", err)
	}
	assertFileContents(t, filepath.Join(benchDir, "data", "src", "osdb-0.bin"), "abcab")
}

func assertFileContents(t *testing.T, path string, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", path, string(got), want)
	}
}

func makeSilesiaZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, contents := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("Create(%s): %v", name, err)
		}
		if _, err := w.Write([]byte(contents)); err != nil {
			t.Fatalf("Write(%s): %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("Close zip writer: %v", err)
	}
	return buf.Bytes()
}

func setSilesiaTestGlobals(t *testing.T, cacheDir string, archiveURL string, client *http.Client) func() {
	t.Helper()
	prevCacheDir := silesiaCacheDir
	prevArchiveURL := silesiaArchiveURL
	prevHTTPClient := silesiaHTTPClient
	silesiaCacheDir = cacheDir
	silesiaArchiveURL = archiveURL
	if silesiaArchiveURL == "" {
		silesiaArchiveURL = defaultSilesiaArchiveURL
	}
	silesiaHTTPClient = client
	if silesiaHTTPClient == nil {
		silesiaHTTPClient = http.DefaultClient
	}
	return func() {
		silesiaCacheDir = prevCacheDir
		silesiaArchiveURL = prevArchiveURL
		silesiaHTTPClient = prevHTTPClient
	}
}

func setGenerateLogOutputForTest(w *bytes.Buffer) func() {
	prev := generateLogOutput
	generateLogOutput = w
	return func() {
		generateLogOutput = prev
	}
}
