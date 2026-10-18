//go:build linux

package filexfercli

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	"golang.org/x/sys/unix"

	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/bench/dataset"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	intftcp "github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/store"
)

// The pbt-faults property for the CLI. It lives apart from cli_faults_test.go
// because it needs tmpfs, which only Linux identifies.

// quietLog silences the server's and the CLI's log lines for the test.
func quietLog(t *testing.T) {
	out := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(out) })
}

// treeMode is one session mode of the CLI against its server: the client's
// --encrypt and credential, and the server's admission.
type treeMode struct {
	encrypt     string // "none", "aes", "chacha20", or "auto"
	cred        string // "", "token", or "age"
	requireAuth bool   // age only; a token allowlist already requires auth
	allowClient bool   // age only: AllowedClientRecipients holds the client
}

// treeModes is every session mode tx supports, as memxfer.Modes lists them
// for the library: plaintext, each cipher with each credential, and age-key
// servers that require auth or allowlist the client.
var treeModes = func() []treeMode {
	modes := []treeMode{{encrypt: "none"}}
	for _, cred := range []string{"token", "age"} {
		for _, enc := range []string{"aes", "chacha20", "auto"} {
			modes = append(modes, treeMode{encrypt: enc, cred: cred})
		}
	}
	return append(modes, treeMode{encrypt: "chacha20", cred: "age", requireAuth: true}, treeMode{encrypt: "aes", cred: "age", allowClient: true})
}()

func (m treeMode) String() string {
	s := m.encrypt
	if m.cred != "" {
		s = m.cred + "+" + s
	}
	if m.requireAuth {
		s += "+require-auth"
	}
	if m.allowClient {
		s += "+allowlist"
	}
	return s
}

// pbtTmpfs returns a fresh directory on tmpfs, under TX_PBT_TMPFS or
// /dev/shm, skipping t when neither is tmpfs: the fault properties never
// touch a real disk.
func pbtTmpfs(t *testing.T) string {
	t.Helper()
	root := os.Getenv("TX_PBT_TMPFS")
	if root == "" {
		root = "/dev/shm"
	}
	var st unix.Statfs_t
	if err := unix.Statfs(root, &st); err != nil || st.Type != unix.TMPFS_MAGIC {
		t.Skipf("%s is not tmpfs (set TX_PBT_TMPFS): %v", root, err)
	}
	dir, err := os.MkdirTemp(root, "tx-pbt-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// writeFaultTree writes a seeded tree under src: nested directories with
// varied modes, regular and empty files of varied sizes, modes, and mtimes,
// hardlink groups, and relative, absolute, directory, and dangling
// symlinks. Contents come from ChaCha8, so the tree is the seed's.
func writeFaultTree(t *testing.T, src string, seed uint64) {
	t.Helper()
	r := rand.New(rand.NewPCG(seed, 0x54524545)) // "TREE"
	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], seed)
	fill := rand.NewChaCha8(key)
	dirs := []string{"."}
	for i := range 1 + r.IntN(5) {
		dirs = append(dirs, filepath.Join(dirs[r.IntN(len(dirs))], fmt.Sprintf("d%d", i)))
		if err := os.MkdirAll(filepath.Join(src, dirs[i+1]), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	var files []string
	for i := range 2 + r.IntN(12) {
		size := 0
		if r.IntN(6) != 0 {
			size = int(math.Exp(r.Float64() * math.Log(3<<20))) // up to 3 MiB, mostly small
		}
		data := make([]byte, size)
		_, _ = fill.Read(data)
		rel := filepath.Join(dirs[r.IntN(len(dirs))], fmt.Sprintf("f%d.bin", i))
		writeLocalTestFile(t, filepath.Join(src, rel), data)
		files = append(files, rel)
	}
	for i := range r.IntN(4) {
		target := files[r.IntN(len(files))]
		link := filepath.Join(dirs[r.IntN(len(dirs))], fmt.Sprintf("h%d.bin", i))
		if err := os.Link(filepath.Join(src, target), filepath.Join(src, link)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range r.IntN(5) {
		link := filepath.Join(src, dirs[r.IntN(len(dirs))], fmt.Sprintf("s%d", i))
		var target string
		switch r.IntN(4) {
		case 0: // relative, to a file
			rel, _ := filepath.Rel(filepath.Dir(link), filepath.Join(src, files[r.IntN(len(files))]))
			target = rel
		case 1: // absolute
			target = filepath.Join(src, files[r.IntN(len(files))])
		case 2: // to a directory
			target = filepath.Join(src, dirs[r.IntN(len(dirs))])
		default:
			target = "missing/target"
		}
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	// Modes and mtimes last, deepest first, since writing into a directory
	// moves its mtime.
	for _, rel := range files {
		p := filepath.Join(src, rel)
		_ = os.Chmod(p, []os.FileMode{0o600, 0o644, 0o640, 0o755}[r.IntN(4)])
		mtime := time.Unix(1_600_000_000+r.Int64N(100_000_000), r.Int64N(1e9))
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		p := filepath.Join(src, dirs[i])
		_ = os.Chmod(p, []os.FileMode{0o755, 0o750, 0o700}[r.IntN(3)])
		mtime := time.Unix(1_600_000_000+r.Int64N(100_000_000), 0)
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
}

// TestTreeCopyUnderFaults is the pbt-faults property for the CLI: tx recv
// copy of a seeded tree on tmpfs, with hardlinks and symlinks, in every
// session mode, against a real server, both faulted by one swarm (or
// bad-day, or a benign swarm). Up to three faulted copies run, with and
// without --verify full and into the destination the last one left, so
// later runs take the resume, delta, and SYNC paths; then the server
// restarts without faults and one clean copy runs. It checks:
//
//  1. A copy that exits 0, with or without --verify, leaves a tree equal to
//     the source: paths, types, content, modes, mtimes, symlink targets,
//     hardlink groups, and the root directory's mode and mtime.
//  2. The clean copy exits 0 and converges to an equal tree, whatever the
//     earlier runs left in the destination and its .tx/ state.
//  3. Benign faults are harmless: under only latencies and failures of
//     advisory syscalls, the first copy exits 0.
//  4. Every copy returns within copyTimeout.
//
// This is the property that reaches the manifest walk, metadata, and the
// CLI's resume and convergence logic, which memxfer bypasses. It sweeps
// seeds rather than fuzzing: Go's fuzzer stops any input after 10s, and a
// copy under faults may wait out tx's own timeouts, such as 15s for an
// ACK, so no budget that fits would tell a hang from a slow success. Each
// seed derives the input's mode, flags, tree, and faults.
//
// It runs only with TX_PBT_FAULTS set (make pbt-faults). TX_PBT_FAULTS_SEED
// runs that one seed; otherwise seeds count up from TX_PBT_FAULTS_SEED_BASE
// (random by default, and logged) until TX_PBT_FAULTS_TIME (default 2m)
// runs out. With TX_FAULTS_* set, every seed runs under those faults
// instead of its own; TX_PBT_FAULTS_EXCLUDE turns kinds off.
func TestTreeCopyUnderFaults(t *testing.T) {
	faults.SkipUnlessPBT(t)
	prev := faultsFromEnv
	t.Cleanup(func() { faultsFromEnv = prev })
	if raw := os.Getenv("TX_PBT_FAULTS_SEED"); raw != "" {
		seed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { treeCopyUnderFaults(t, seed) })
		return
	}
	base := rand.Uint64() >> 16
	if raw := os.Getenv("TX_PBT_FAULTS_SEED_BASE"); raw != "" {
		var err error
		if base, err = strconv.ParseUint(raw, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	budget := 2 * time.Minute
	if raw := os.Getenv("TX_PBT_FAULTS_TIME"); raw != "" {
		var err error
		if budget, err = time.ParseDuration(raw); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("seeds from %d for %v", base, budget)
	end := time.Now().Add(budget)
	seed := base
	for ; time.Now().Before(end); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { treeCopyUnderFaults(t, seed) })
		// A hung copy keeps running, faulting and writing, and would
		// disturb the seeds after it.
		if treeCopyHung.Load() {
			t.Logf("stopping after seed %d: its hung copy is still running", seed)
			seed++
			break
		}
	}
	t.Logf("ran seeds %d to %d", base, seed-1)
}

// treeCopyUnderFaults runs TestTreeCopyUnderFaults for one seed.
func treeCopyUnderFaults(t *testing.T, seed uint64) {
	quietLog(t)
	r := rand.New(rand.NewPCG(seed, 0x50425454)) // "PBTT"
	mode := treeModes[seed%uint64(len(treeModes))]
	flags := uint8(r.Uint32())
	set := faults.PBTFaultSet(r.IntN(4))
	cfg, err := faults.PBTConfig(seed, set)
	if err != nil {
		t.Fatal(err)
	}
	inj, err := faults.PBTInjector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var clientFaults atomic.Pointer[faults.Injector]
	clientFaults.Store(inj)
	faultsFromEnv = func(func(string) (string, bool)) (*faults.Injector, error) { return clientFaults.Load(), nil }

	base := pbtTmpfs(t)
	src, dst := filepath.Join(base, "src"), filepath.Join(base, "work", "dst")
	writeFaultTree(t, src, seed)
	walked, err := dataset.Walk(src)
	if err != nil {
		t.Fatal(err)
	}
	want := dataset.Entries(walked)
	if err := dataset.HashEntries(context.Background(), src, want, nil, 2, nil); err != nil {
		t.Fatal(err)
	}
	srcRoot, err := os.Lstat(src)
	if err != nil {
		t.Fatal(err)
	}

	srv := startFaultTreeServer(t, base, mode)
	srv.restart(t, inj)
	args := func(verify bool) []string {
		a := []string{"copy", "-y", "--progress=false", "--concurrency", "2", "--encrypt", mode.encrypt}
		switch mode.cred {
		case "token":
			a = append(a, "--auth-token", srv.token)
		case "age":
			a = append(a, "--keys", srv.keysDir)
		}
		// Mirroring the page cache, which only fast mode does, reaches the
		// mmap, mincore, and fadvise calls a copy makes.
		switch {
		case flags&0b10 != 0:
			a = append(a, "--mode", "gentle")
		case flags&0b100 != 0:
			a = append(a, "--cache-load", "full")
		}
		if verify {
			a = append(a, "--verify", "full")
		}
		return append(a, "tx://"+srv.addr+"/src", dst)
	}
	where := func() string {
		return fmt.Sprintf("seed %d, mode %s, flags %#x, faults %v; rerun with TX_PBT_FAULTS=1 TX_PBT_FAULTS_SEED=%d %s",
			seed, mode, flags, inj.Counts().InjectedByKind(), seed, inj.Repro())
	}
	copyOnce := func(name string, verify bool) (int, string) {
		// A copy that outlives copyTimeout still writes them, so the
		// timeout's snapshot of stderr must lock.
		var stdout, stderr lockedBuffer
		code := make(chan int, 1)
		go func() { code <- RunCLI(args(verify), &stdout, &stderr) }()
		select {
		case c := <-code:
			if c == 0 {
				got, err := dataset.Verify(context.Background(), dst, want, dataset.VerifyOptions{Content: true, Jobs: 2})
				if err != nil || !got.OK() {
					t.Fatalf("%s copy (verify=%v) exited 0 but the tree differs: %v %v; %s\nstderr:\n%s", name, verify, err, got.Mismatches, where(), stderr.String())
				}
				// dataset.Verify checks the entries under the root, not
				// the root itself.
				if got, err := os.Lstat(dst); err != nil || got.Mode() != srcRoot.Mode() || !got.ModTime().Equal(srcRoot.ModTime()) {
					t.Fatalf("%s copy (verify=%v) exited 0 but the root is %v, want mode %v mtime %v; %s", name, verify, describeRoot(got, err), srcRoot.Mode(), srcRoot.ModTime(), where())
				}
			}
			return c, stderr.String()
		case <-time.After(copyTimeout):
			treeCopyHung.Store(true)
			buf := make([]byte, 1<<20)
			t.Fatalf("%s copy (verify=%v) still running after %v; %s\nstderr:\n%s\n%s", name, verify, copyTimeout, where(), stderr.String(), buf[:runtime.Stack(buf, true)])
			return -1, ""
		}
	}
	for run := range 3 {
		code, stderr := copyOnce(fmt.Sprintf("fault run %d", run), flags&(1<<(4+run)) != 0)
		if run == 0 && code != 0 && inj.Benign() {
			t.Fatalf("benign faults failed the first copy (exit %d); %s\nstderr:\n%s", code, where(), stderr)
		}
		if code == 0 {
			break
		}
	}
	clientFaults.Store(nil)
	srv.restart(t, nil)
	if code, stderr := copyOnce("clean", flags&0b1 != 0); code != 0 {
		t.Fatalf("clean copy exited %d after faulted runs; %s\nstderr:\n%s", code, where(), stderr)
	}
	if testing.Verbose() {
		t.Logf("mode %s: faults %v", mode, inj.Counts().InjectedByKind())
	}
}

// lockedBuffer is a bytes.Buffer safe to read while another goroutine
// writes it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func describeRoot(info os.FileInfo, err error) string {
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("mode %v mtime %v", info.Mode(), info.ModTime())
}

// treeCopyHung reports that a copy outlived copyTimeout and still runs.
var treeCopyHung atomic.Bool

// copyTimeout bounds one CLI copy of a fault tree.
const copyTimeout = 90 * time.Second

// faultTreeServer is one TestTreeCopyUnderFaults seed's server: chrooted
// at the input's tmpfs directory, keeping its transfer state across
// restarts on one address.
type faultTreeServer struct {
	mode    treeMode
	st      *store.Store
	root    string
	addr    string
	id      *age.X25519Identity
	token   string
	keysDir string
	client  string // the client's age recipient
	stop    func() error
	// handlers counts connection handlers still running, from the
	// server's events: Serve returns without waiting for them.
	handlers atomic.Int64
	events   *events.Sink
}

func startFaultTreeServer(t *testing.T, base string, mode treeMode) *faultTreeServer {
	t.Helper()
	s := &faultTreeServer{mode: mode, st: store.NewStore(), root: base, events: events.NewSink()}
	s.events.Subscribe(func(ev events.Event) {
		switch ev.Name {
		case "accept":
			s.handlers.Add(1)
		case "conn_close":
			s.handlers.Add(-1)
		}
	})
	t.Cleanup(func() {
		if s.stop != nil {
			_ = s.stop()
		}
		s.st.Close()
	})
	if mode.encrypt == "none" {
		return s
	}
	var err error
	if s.id, err = age.GenerateX25519Identity(); err != nil {
		t.Fatal(err)
	}
	switch mode.cred {
	case "token":
		if s.token, err = aead.NewAuthToken(); err != nil {
			t.Fatal(err)
		}
	case "age":
		s.keysDir = filepath.Join(base, "keys")
		if err := os.Mkdir(s.keysDir, 0o700); err != nil {
			t.Fatal(err)
		}
		clientID, _, err := aead.LoadOrGenerateAgeIdentity(s.keysDir, false)
		if err != nil {
			t.Fatal(err)
		}
		s.client = clientID.Recipient().String()
	}
	return s
}

// restart stops the running server, if any, and starts one with inj on the
// same address and store, as restarting tx send tree would. Like a process
// exit, it first waits for the old server's handlers, so none can change
// the store under the new one.
func (s *faultTreeServer) restart(t *testing.T, inj *faults.Injector) {
	t.Helper()
	if s.stop != nil {
		if err := s.stop(); err != nil {
			t.Fatal(err)
		}
	}
	addr := s.addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	s.addr = ln.Addr().String()
	opts := intftcp.ServerOptions{
		Deps:             intftcp.NewRuntimeDeps(s.st, intftcp.WithRoot(s.root)),
		ServerIdentity:   s.id,
		RequireAuth:      s.mode.requireAuth,
		KeepAliveTimeout: 2 * time.Second,
		Faults:           inj,
		Events:           s.events,
	}
	if s.token != "" {
		opts.AllowedAuthTokens = []string{s.token}
	}
	if s.mode.allowClient {
		opts.AllowedClientRecipients = []string{s.client}
	}
	served := make(chan error, 1)
	go func() { served <- intftcp.Serve(ln, opts) }()
	s.stop = func() error {
		_ = ln.Close()
		<-served
		for deadline := time.Now().Add(2 * time.Second); s.handlers.Load() > 0; {
			if time.Now().After(deadline) {
				return fmt.Errorf("%d server handlers still running 2s after the copy returned", s.handlers.Load())
			}
			time.Sleep(time.Millisecond)
		}
		return nil
	}
}
