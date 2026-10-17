package filexfercli

import (
	"bytes"
	"context"
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
