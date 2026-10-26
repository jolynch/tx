package filexfercli

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/store"
)

// Fault-injection tests for the CLI: end-to-end copies with the TX_FAULTS_*
// variables set, through a real server wrapped by the same injector.

// TestCLIThroughFaultWrappers runs copy and get with TX_FAULTS_SEED set, so
// the CLI wraps its connections, output files, verify reads, and syscall
// sites in an injector that injects nothing, against a server wrapped the
// same way. Remote copies (plain, encrypted, gentle, with --cache-load, and
// into an existing destination), daemonless copies (fresh and into an
// existing destination), and remote and local gets must all still copy
// exactly, and the injectors' counts must show each site was reached.
// Plaintext fast copies must still send frames zero-copy.
func TestCLIThroughFaultWrappers(t *testing.T) {
	// Blank counts as unset, which shields the test from the developer's
	// own environment; the seed alone wraps every site.
	for _, env := range []string{faults.EnvNet, faults.EnvMsg, faults.EnvDisk, faults.EnvSys} {
		t.Setenv(env, "")
	}
	t.Setenv(faults.EnvSeed, "1")
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sink := events.NewSink()
	var sendfileWindows atomic.Int64
	sink.Subscribe(func(e events.Event) {
		if v, ok := e.Get("send_path"); ok && e.Name == "window" && v == "sendfile" {
			sendfileWindows.Add(1)
		}
	})
	serverFaults := faults.New(faults.Config{Seed: 2})
	st := store.NewStore()
	served := make(chan error, 1)
	go func() {
		served <- intftcp.Serve(ln, intftcp.ServerOptions{
			Deps:             intftcp.NewRuntimeDeps(st, intftcp.WithRoot(os.TempDir())),
			ServerIdentity:   id,
			KeepAliveTimeout: 5 * time.Second,
			Faults:           serverFaults,
			Events:           sink,
		})
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-served
		st.Close()
	})

	// newTree writes a source tree under the server's root. 3 MiB spans
	// several 1 MiB frames, which plaintext fast mode sends zero-copy.
	newTree := func(t *testing.T) (string, map[string][]byte) {
		t.Helper()
		src := filepath.Join(t.TempDir(), "src")
		fill := rand.NewChaCha8([32]byte{1})
		files := map[string][]byte{"small.txt": []byte("hello"), "d/empty.bin": nil}
		for name, size := range map[string]int{"big.bin": 3 << 20, "d/mid.bin": 70 << 10} {
			files[name] = make([]byte, size)
			_, _ = fill.Read(files[name])
		}
		for name, data := range files {
			writeLocalTestFile(t, filepath.Join(src, name), data)
		}
		return src, files
	}
	remoteURL := func(t *testing.T, src string) string {
		t.Helper()
		rel, err := filepath.Rel(os.TempDir(), src)
		if err != nil {
			t.Fatal(err)
		}
		return "tx://" + ln.Addr().String() + "/" + filepath.ToSlash(rel)
	}
	checkFiles := func(t *testing.T, dst string, files map[string][]byte) {
		t.Helper()
		for name, want := range files {
			got, err := os.ReadFile(filepath.Join(dst, name))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s: %d bytes, err %v; want %d bytes", name, len(got), err, len(want))
			}
		}
	}

	// Capture the injector each command builds from the environment.
	var clientFaults []*faults.Injector
	t.Cleanup(func(prev func(func(string) (string, bool)) (*faults.Injector, error)) func() {
		return func() { faultsFromEnv = prev }
	}(faultsFromEnv))
	faultsFromEnv = func(lookup func(string) (string, bool)) (*faults.Injector, error) {
		inj, err := faults.FromEnv(lookup)
		clientFaults = append(clientFaults, inj)
		return inj, err
	}
	// run runs one command and returns its injector's counts.
	run := func(t *testing.T, args ...string) faults.Counts {
		t.Helper()
		clientFaults = nil
		var stdout, stderr bytes.Buffer
		if code := RunCLI(args, &stdout, &stderr); code != 0 {
			t.Fatalf("%s exit %d: %s", args[0], code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "fault injection is on") {
			t.Fatalf("missing fault warning: %s", stderr.String())
		}
		verifyFull := slices.Contains(args, "--verify") && args[slices.Index(args, "--verify")+1] == "full"
		if verifyFull && !strings.Contains(stderr.String(), "verify-data: [ok]") {
			t.Fatalf("missing verify result: %s", stderr.String())
		}
		if len(clientFaults) != 1 || clientFaults[0] == nil {
			t.Fatalf("%s built %d injectors, want one", args[0], len(clientFaults))
		}
		return clientFaults[0].Counts()
	}
	copyArgs := func(extra ...string) []string {
		return append([]string{"copy", "-y", "--progress=false", "--verify", "full"}, extra...)
	}
	// reached fails the test for any site whose count is zero.
	reached := func(t *testing.T, who string, got map[string]uint64) {
		t.Helper()
		for site, n := range got {
			if n == 0 {
				t.Fatalf("%s injector never reached %s", who, site)
			}
		}
	}
	remoteWrites := func(c faults.Counts) map[string]uint64 {
		return map[string]uint64{
			"connections":    c.ClientConns,
			"socket options": c.Sys[faults.SysSetsockopt],
			"output writes":  c.Writes[faults.SiteOutputWrite],
			"background dup": c.Sys[faults.SysDup],
			"batch fstat":    c.Sys[faults.SysFstat],
			"fdatasync":      c.Sys[faults.SysFdatasync],
			"syncfs":         c.Sys[faults.SysSyncfs],
		}
	}

	src, files := newTree(t)
	remote := remoteURL(t, src)
	for _, tc := range []struct {
		name     string
		extra    []string
		zeroCopy bool
	}{
		{"remote", nil, true},
		{"remote-encrypted", []string{"--encrypt", "auto"}, false},
		{"remote-gentle", []string{"--mode", "gentle"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := sendfileWindows.Load()
			s0 := serverFaults.Counts()
			dst := filepath.Join(t.TempDir(), "dst")
			c := run(t, append(copyArgs(tc.extra...), remote, dst)...)
			checkFiles(t, dst, files)
			if got := sendfileWindows.Load() > before; got != tc.zeroCopy {
				t.Fatalf("zero-copy windows sent: %t, want %t", got, tc.zeroCopy)
			}
			client := remoteWrites(c)
			client["verify reads"] = c.Reads[faults.SiteVerifyRead]
			reached(t, "client", client)
			s := serverFaults.Counts()
			reached(t, "server", map[string]uint64{
				"connections":    s.ServerConns - s0.ServerConns,
				"socket options": s.Sys[faults.SysSetsockopt] - s0.Sys[faults.SysSetsockopt],
				"SEND opens":     s.Opens[faults.SiteSendOpen] - s0.Opens[faults.SiteSendOpen],
				"SEND fstat":     s.Sys[faults.SysFstat] - s0.Sys[faults.SysFstat],
				"CXSUM opens":    s.Opens[faults.SiteCXSUMOpen] - s0.Opens[faults.SiteCXSUMOpen],
				"CXSUM reads":    s.Reads[faults.SiteCXSUMRead] - s0.Reads[faults.SiteCXSUMRead],
			})
			if tc.zeroCopy && s.Sys[faults.SysSplice] == s0.Sys[faults.SysSplice] {
				t.Fatal("zero-copy frames skipped the splice gate")
			}
		})
	}
	// Without --verify, the copy ends after its start phase, whose own
	// worker and syncfs must use the injector.
	t.Run("remote-no-verify", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "dst")
		c := run(t, "copy", "-y", "--progress=false", "--verify", "none", remote, dst)
		checkFiles(t, dst, files)
		reached(t, "client", remoteWrites(c))
	})
	// The command installs its injector as the process's page-cache
	// injector, so the page-cache calls --cache-load makes during the copy
	// must reach it. Which calls it sees depends on what else in this
	// process set one; the page-cache package's own test checks each gate.
	t.Run("remote-cache-load", func(t *testing.T) {
		c := run(t, append(copyArgs("--cache-load", "full"), remote, filepath.Join(t.TempDir(), "dst"))...)
		reached(t, "client", map[string]uint64{"page-cache mmap": c.Sys[faults.SysMmap]})
	})
	t.Run("local", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "dst")
		c := run(t, append(copyArgs("--cache-load", "full"), src, dst)...)
		checkFiles(t, dst, files)
		reached(t, "client", map[string]uint64{
			"copy_file_range": c.Sys[faults.SysCopyFileRange],
			"verify reads":    c.Reads[faults.SiteVerifyRead],
			"syncfs":          c.Sys[faults.SysSyncfs],
			"page-cache mmap": c.Sys[faults.SysMmap],
		})
	})
	// --skip-write copies each file into /dev/null through the same
	// copy_file_range gate.
	t.Run("local-skip-write", func(t *testing.T) {
		c := run(t, "copy", "-y", "--progress=false", "--verify", "none", "--skip-write", src, filepath.Join(t.TempDir(), "dst"))
		reached(t, "client", map[string]uint64{"copy_file_range": c.Sys[faults.SysCopyFileRange]})
	})
	t.Run("remote-get", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "big.bin")
		c := run(t, "get", "--progress=false", "--cache-load", "full", remote+"/big.bin", dst)
		checkFiles(t, filepath.Dir(dst), map[string][]byte{"big.bin": files["big.bin"]})
		client := remoteWrites(c)
		client["page-cache mmap"] = c.Sys[faults.SysMmap]
		reached(t, "client", client)
	})
	t.Run("local-get", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "big.bin")
		c := run(t, "get", "--cache-load", "full", filepath.Join(src, "big.bin"), dst)
		checkFiles(t, filepath.Dir(dst), map[string][]byte{"big.bin": files["big.bin"]})
		reached(t, "client", map[string]uint64{
			"copy_file_range": c.Sys[faults.SysCopyFileRange],
			"syncfs":          c.Sys[faults.SysSyncfs],
			"page-cache mmap": c.Sys[faults.SysMmap],
		})
	})
	// A second copy into the same destination converges it in place: a
	// SYNC for a remote source, a delta for a local one. Each uses its own
	// tree, since it changes a source file.
	t.Run("remote-into-existing", func(t *testing.T) {
		src, files := newTree(t)
		dst := filepath.Join(t.TempDir(), "dst")
		run(t, append(copyArgs(), remoteURL(t, src), dst)...)
		files["small.txt"] = []byte("hello again")
		writeLocalTestFile(t, filepath.Join(src, "small.txt"), files["small.txt"])
		c := run(t, append(copyArgs(), remoteURL(t, src), dst)...)
		checkFiles(t, dst, files)
		reached(t, "client", remoteWrites(c))
	})
	t.Run("local-into-existing", func(t *testing.T) {
		src, files := newTree(t)
		dst := filepath.Join(t.TempDir(), "dst")
		run(t, append(copyArgs(), src, dst)...)
		files["small.txt"] = []byte("hello again")
		writeLocalTestFile(t, filepath.Join(src, "small.txt"), files["small.txt"])
		c := run(t, append(copyArgs(), src, dst)...)
		checkFiles(t, dst, files)
		reached(t, "client", map[string]uint64{
			"copy_file_range": c.Sys[faults.SysCopyFileRange],
			"syncfs":          c.Sys[faults.SysSyncfs],
		})
	})
}

// TestClientFaultsFromEnv checks that TX_FAULTS_* reaches every client-side
// site: the client's connections and the download output files.
func TestClientFaultsFromEnv(t *testing.T) {
	auth, err := clientAuthFlags{}.resolve(func(k string) (string, bool) {
		if k == faults.EnvSeed {
			return "3", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if auth.faults == nil || auth.faults.Seed() != 3 {
		t.Fatalf("resolve did not build the injector: %+v", auth.faults)
	}
	client := tx.NewClient("127.0.0.1:1", auth.options()...)
	defer client.Close()
	if client.Faults() != auth.faults {
		t.Fatal("client options dropped the injector")
	}
	path := filepath.Join(t.TempDir(), "out.bin")
	w, syncOutput, err := openDownloadOutput(tx.ManifestEntry{ID: 1, Path: "out.bin"}, 0, path, nil, nil, auth.faults)
	if err != nil {
		t.Fatal(err)
	}
	if _, isFile := w.(*os.File); isFile || auth.faults.Counts().Writes[faults.SiteOutputWrite] != 1 {
		t.Fatalf("output %T not wrapped; counts %+v", w, auth.faults.Counts())
	}
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := syncOutput(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "abc" {
		t.Fatalf("read back %q, %v", got, err)
	}
}

// TestVerifyLocalItemsWrapsBothSides checks local verification wraps the
// source and the destination read of every item.
func TestVerifyLocalItemsWrapsBothSides(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	data := bytes.Repeat([]byte("x"), 3<<10)
	writeLocalTestFile(t, filepath.Join(src, "a.bin"), data)
	writeLocalTestFile(t, filepath.Join(dst, "a.bin"), data)
	entries := []tx.ManifestEntry{{ID: 1, Path: "a.bin", Size: int64(len(data))}}
	inj := faults.New(faults.Config{Seed: 1})
	items := verifyTestItems(entries, src, dst, 1<<10)
	if err := verifyLocalItems(context.Background(), items, inj); err != nil {
		t.Fatal(err)
	}
	if got, want := inj.Counts().Reads[faults.SiteVerifyRead], uint64(2*len(items)); got != want {
		t.Fatalf("verify read wraps = %d, want %d (source and destination per item)", got, want)
	}
}

// TestOutputWriteFaultsNameTheirFile checks download outputs pass the
// entry's ID and window offset to the write fault site, which keys faults
// per file and offset; a fault event reports both.
func TestOutputWriteFaultsNameTheirFile(t *testing.T) {
	sink := events.NewSink()
	type at struct {
		file   uint64
		offset int64
	}
	var got []at
	sink.Subscribe(func(e events.Event) {
		if e.Name == "fault" {
			off, _ := e.Get("offset")
			got = append(got, at{uint64(e.Int("file")), off.(int64)})
		}
	})
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyDisk: "write.eio=1"}, Events: sink})
	const offset = 128 << 10 // a resumed window, on a write-fault block boundary
	w, _, err := openDownloadOutput(tx.ManifestEntry{ID: 7, Path: "out.bin"}, offset, filepath.Join(t.TempDir(), "out.bin"), nil, nil, inj)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write([]byte("abc")); err == nil {
		t.Fatal("write.eio=1 did not fail the write")
	}
	if want := (at{7, offset}); len(got) != 1 || got[0] != want {
		t.Fatalf("fault events %v, want [%v]", got, want)
	}
}

// TestVerifyReadFaultsNameTheirFile checks verify reads pass the entry's ID
// to the read fault site, which keys faults per file and offset; a fault
// event reports both.
func TestVerifyReadFaultsNameTheirFile(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	data := bytes.Repeat([]byte("x"), 3<<10)
	writeLocalTestFile(t, filepath.Join(dst, "a.bin"), data)
	sink := events.NewSink()
	var file uint64
	var offset int64
	sink.Subscribe(func(e events.Event) {
		if e.Name == "fault" {
			off, _ := e.Get("offset")
			file, offset = uint64(e.Int("file")), off.(int64)
		}
	})
	inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyDisk: "read.eio=1"}, Events: sink})
	entries := []tx.ManifestEntry{{ID: 11, Path: "a.bin", Size: int64(len(data))}}
	items := verifyTestItems(entries, src, dst, 1<<10)[1:] // start at the second 1 KiB sample
	if _, err := hashItems(context.Background(), items, false, inj); err == nil {
		t.Fatal("read.eio=1 did not fail the verify read")
	}
	if file != 11 || offset != 1<<10 {
		t.Fatalf("fault event named file %d offset %d, want file 11 offset %d", file, offset, 1<<10)
	}
}

// syncFaultServer is a fault-free server rooted at os.TempDir, for tests
// whose CLI side alone takes faults from TX_FAULTS_*.
type syncFaultServer struct{ addr string }

func startSyncFaultServer(t *testing.T) syncFaultServer {
	t.Helper()
	for _, env := range []string{faults.EnvSeed, faults.EnvProfile, faults.EnvNet, faults.EnvMsg, faults.EnvDisk, faults.EnvSys} {
		t.Setenv(env, "")
	}
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewStore()
	served := make(chan error, 1)
	go func() {
		served <- intftcp.Serve(ln, intftcp.ServerOptions{
			Deps:             intftcp.NewRuntimeDeps(st, intftcp.WithRoot(os.TempDir())),
			ServerIdentity:   id,
			KeepAliveTimeout: 5 * time.Second,
		})
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-served
		st.Close()
	})
	return syncFaultServer{addr: ln.Addr().String()}
}

// newTree writes a source tree under the server's root and returns its
// path, its tx:// URL, and its files' contents.
func (s syncFaultServer) newTree(t *testing.T) (string, string, map[string][]byte) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "src")
	files := map[string][]byte{
		"small.txt":                    []byte("hello"),
		filepath.Join("d", "mid.bin"):  bytes.Repeat([]byte("m"), 70<<10),
		filepath.Join("d", "e", "big"): bytes.Repeat([]byte("b"), 2<<20),
	}
	for name, data := range files {
		writeLocalTestFile(t, filepath.Join(src, name), data)
	}
	rel, err := filepath.Rel(os.TempDir(), src)
	if err != nil {
		t.Fatal(err)
	}
	return src, "tx://" + s.addr + "/" + filepath.ToSlash(rel), files
}

func checkSyncFaultFiles(t *testing.T, dst string, files map[string][]byte) {
	t.Helper()
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dst, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %d bytes, err %v; want %d bytes", name, len(got), err, len(want))
		}
	}
}

// runWithSysFaults runs one command with TX_FAULTS_SYS set to sys (and a
// seed, when sys is set) and returns its exit code and stderr.
func runWithSysFaults(t *testing.T, sys string, args ...string) (int, string) {
	t.Helper()
	seed := ""
	if sys != "" {
		seed = "1"
	}
	t.Setenv(faults.EnvSeed, seed)
	t.Setenv(faults.EnvSys, sys)
	var stdout, stderr bytes.Buffer
	code := RunCLI(args, &stdout, &stderr)
	return code, stderr.String()
}

// TestCLIFailedBackgroundSyncExitsNonzero checks a copy or get whose
// background fdatasyncs fail still writes every file, then exits nonzero and
// names each file that failed to sync: a fresh copy, a copy into an existing
// destination, and a get. The server runs without faults; the CLI's
// injector, from TX_FAULTS_SYS, fails every fdatasync.
func TestCLIFailedBackgroundSyncExitsNonzero(t *testing.T) {
	srv := startSyncFaultServer(t)
	const failSync = "fdatasync.fail=1"
	// wantSyncFailure fails t unless the command exited nonzero and its
	// stderr names each of names, which may be relative to the destination,
	// since a fresh copy syncs files in its staging directory.
	wantSyncFailure := func(t *testing.T, code int, stderr string, names ...string) {
		t.Helper()
		if code == 0 {
			t.Fatalf("exit 0 though every fdatasync failed; stderr:\n%s", stderr)
		}
		for _, name := range names {
			if !strings.Contains(stderr, name) {
				t.Fatalf("stderr does not name unsynced file %s:\n%s", name, stderr)
			}
		}
	}
	copyArgs := func(remote, dst string) []string {
		return []string{"copy", "-y", "--progress=false", "--verify", "none", "--fsync-interval", "1MiB", remote, dst}
	}

	t.Run("copy", func(t *testing.T) {
		_, remote, files := srv.newTree(t)
		dst := filepath.Join(t.TempDir(), "dst")
		code, stderr := runWithSysFaults(t, failSync, copyArgs(remote, dst)...)
		checkSyncFaultFiles(t, dst, files)
		var names []string
		for name := range files {
			names = append(names, name)
		}
		wantSyncFailure(t, code, stderr, names...)
	})
	t.Run("copy-into-existing", func(t *testing.T) {
		src, remote, files := srv.newTree(t)
		dst := filepath.Join(t.TempDir(), "dst")
		if code, stderr := runWithSysFaults(t, "", copyArgs(remote, dst)...); code != 0 {
			t.Fatalf("first copy exit %d: %s", code, stderr)
		}
		files["small.txt"] = []byte("hello again")
		writeLocalTestFile(t, filepath.Join(src, "small.txt"), files["small.txt"])
		code, stderr := runWithSysFaults(t, failSync, copyArgs(remote, dst)...)
		checkSyncFaultFiles(t, dst, files)
		wantSyncFailure(t, code, stderr, filepath.Join(dst, "small.txt"))
	})
	t.Run("get", func(t *testing.T) {
		_, remote, files := srv.newTree(t)
		dst := filepath.Join(t.TempDir(), "out.bin")
		code, stderr := runWithSysFaults(t, failSync, "get", "--progress=false", "--fsync-interval", "1MiB", remote+"/d/mid.bin", dst)
		if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, files[filepath.Join("d", "mid.bin")]) {
			t.Fatalf("get wrote %d bytes, err %v", len(got), err)
		}
		wantSyncFailure(t, code, stderr, dst)
	})
}

// TestCLIFailedSyncfsWarns checks a final syncfs failure is a warning, not
// a failure: the run still writes every file and exits 0, stderr carries
// the WARNING, and the --stats object records it under warnings while
// keeping status ok and exit_code 0. It covers a fresh copy with background
// fdatasync and syncfs-only (--fsync-interval -1), a copy into an existing
// destination, and a get. The server runs without faults; the CLI's
// injector, from TX_FAULTS_SYS, fails every syncfs.
func TestCLIFailedSyncfsWarns(t *testing.T) {
	srv := startSyncFaultServer(t)
	const failSyncfs = "syncfs.fail=1"
	// wantWarning fails t unless the command exited 0, stderr reports the
	// syncfs failure, and the stats file at path records a warning about
	// syncfs without changing the outcome.
	wantWarning := func(t *testing.T, code int, stderr, path string) {
		t.Helper()
		if code != 0 {
			t.Fatalf("exit %d though only syncfs failed; stderr:\n%s", code, stderr)
		}
		if !strings.Contains(stderr, "final-syncfs:") || !strings.Contains(stderr, "failed") {
			t.Fatalf("stderr does not report the syncfs failure:\n%s", stderr)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var stats struct {
			Status   string   `json:"status"`
			Error    string   `json:"error"`
			ExitCode *int     `json:"exit_code"`
			Warnings []string `json:"warnings"`
		}
		if err := json.Unmarshal(data, &stats); err != nil {
			t.Fatalf("stats %q: %v", data, err)
		}
		if stats.Status != "ok" || stats.Error != "" || stats.ExitCode == nil || *stats.ExitCode != 0 {
			t.Fatalf("stats outcome changed by a warning: %s", data)
		}
		if !slices.ContainsFunc(stats.Warnings, func(w string) bool { return strings.Contains(w, "syncfs") }) {
			t.Fatalf("stats warnings %q do not name syncfs; stats %s", stats.Warnings, data)
		}
	}
	copyArgs := func(stats, remote, dst string, extra ...string) []string {
		args := append([]string{"copy", "-y", "--progress=false", "--verify", "none", "--stats", stats}, extra...)
		return append(args, remote, dst)
	}

	for _, tc := range []struct {
		name  string
		extra []string
	}{
		{"copy", []string{"--fsync-interval", "1MiB"}},
		{"copy-syncfs-only", []string{"--fsync-interval", "-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, remote, files := srv.newTree(t)
			dst := filepath.Join(t.TempDir(), "dst")
			stats := filepath.Join(t.TempDir(), "stats.json")
			code, stderr := runWithSysFaults(t, failSyncfs, copyArgs(stats, remote, dst, tc.extra...)...)
			checkSyncFaultFiles(t, dst, files)
			wantWarning(t, code, stderr, stats)
		})
	}
	t.Run("copy-into-existing", func(t *testing.T) {
		src, remote, files := srv.newTree(t)
		dst := filepath.Join(t.TempDir(), "dst")
		if code, stderr := runWithSysFaults(t, "", copyArgs(filepath.Join(t.TempDir(), "first.json"), remote, dst)...); code != 0 {
			t.Fatalf("first copy exit %d: %s", code, stderr)
		}
		files["small.txt"] = []byte("hello again")
		writeLocalTestFile(t, filepath.Join(src, "small.txt"), files["small.txt"])
		stats := filepath.Join(t.TempDir(), "stats.json")
		code, stderr := runWithSysFaults(t, failSyncfs, copyArgs(stats, remote, dst)...)
		checkSyncFaultFiles(t, dst, files)
		wantWarning(t, code, stderr, stats)
	})
	t.Run("get", func(t *testing.T) {
		_, remote, files := srv.newTree(t)
		dst := filepath.Join(t.TempDir(), "out.bin")
		stats := filepath.Join(t.TempDir(), "stats.json")
		code, stderr := runWithSysFaults(t, failSyncfs, "get", "--progress=false", "--stats", stats, remote+"/d/mid.bin", dst)
		if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, files[filepath.Join("d", "mid.bin")]) {
			t.Fatalf("get wrote %d bytes, err %v", len(got), err)
		}
		wantWarning(t, code, stderr, stats)
	})
}
