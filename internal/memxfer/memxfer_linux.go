//go:build linux

// Package memxfer copies files between a real FTCP server and a real
// tx.Client in one process, with every file's contents in a memfd on both
// sides. Tests use it to run whole transfers, faults and all, with no disk,
// and to check every byte the client wrote. File contents derive from a seed,
// so the expected bytes are regenerated rather than kept.
//
// Every file gets its own source and sink memfd, so the harness suits
// thousands of files, not the hundreds of thousands BenchmarkTransferInMemory
// copies through one shared memfd.
//
// Linux only: memfds (unix.MemfdCreate) exist nowhere else.
package memxfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"filippo.io/age"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/cmd/filexfercli"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/ftcp"
	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/fsync"
)

// Mode is how the client and server set up a session.
type Mode struct {
	// Encrypt is the client's encryption: "" for plaintext, or "aes",
	// "chacha20", or "auto".
	Encrypt string
	// Token authenticates with a shared token (Noise IKpsk0) instead of the
	// client's age key (Noise IK). It applies only when Encrypt is set.
	Token bool
	// RequireAuth makes the server reject plaintext and admit connections
	// only with a permit. A token allowlist already does both, so it
	// changes only age-key modes.
	RequireAuth bool
	// AllowClient restricts an age-key server to the client's recipient
	// (AllowedClientRecipients), which also requires auth.
	AllowClient bool
}

// Modes lists every session mode tx supports: plaintext, and each cipher
// with each credential, where a token server always holds an allowlist,
// plus an age-key server that requires auth and one that allowlists the
// client.
func Modes() []Mode {
	modes := []Mode{{}}
	for _, token := range []bool{true, false} {
		for _, enc := range []string{"aes", "chacha20", "auto"} {
			modes = append(modes, Mode{Encrypt: enc, Token: token})
		}
	}
	return append(modes,
		Mode{Encrypt: "chacha20", RequireAuth: true},
		Mode{Encrypt: "aes", AllowClient: true})
}

func (m Mode) String() string {
	if m.Encrypt == "" {
		return "plaintext"
	}
	cred := "age"
	if m.Token {
		cred = "token"
	}
	s := cred + "+" + m.Encrypt
	if m.RequireAuth {
		s += "+require-auth"
	}
	if m.AllowClient {
		s += "+allowlist"
	}
	return s
}

// Config is one environment: the files to copy and how to copy them.
type Config struct {
	// Sizes holds one file per size.
	Sizes []int64
	// Seed derives every file's contents.
	Seed uint64
	Mode Mode
	// KeepAlive lets pooled connections carry many commands.
	KeepAlive bool
	// Comp is the client's compression: "", "none", "lz4", "zstd", or "adapt".
	Comp string
	// Concurrency is the client's connection count; zero picks 4.
	Concurrency int
	// Faults, when set, faults both sides.
	Faults *faults.Injector
	// Events, when set, receives the server's event timeline.
	Events *events.Sink
	// KeepAliveTimeout is the server's idle timeout for kept-alive
	// connections; zero picks one minute.
	KeepAliveTimeout time.Duration
}

// Env is a running server and client, the source files the server reads,
// and the sink files the client writes.
type Env struct {
	cfg        Config
	conc       int
	st         *store.Store
	ln         net.Listener
	served     chan error
	serverOpts ftcp.ServerOptions
	clientOpts []tx.ClientOption
	deps       *deps
	client     *tx.Client
	src, dst   []*os.File // file ID i is index i-1
	manifest   *tx.Manifest
	transferID string
	handlers   atomic.Int64 // server connection handlers still running
	writes     atomic.Int64 // output Write calls, across rounds
	open       atomic.Int64 // outputs not yet closed
	writing    atomic.Int64 // output Write calls not yet returned

	openedMu sync.Mutex
	opened   map[uint64][]int64 // where the last round opened each file's outputs
	unsynced int64              // outputs the last round's sync worker gave up on
}

const root = "/memxfer"

// Start creates the files, starts the server and client, and registers the
// transfer, as TXFER would.
func Start(cfg Config) (_ *Env, err error) {
	e := &Env{cfg: cfg, st: store.NewStore()}
	defer func() {
		if err != nil {
			e.Close()
		}
	}()
	for id := range len(cfg.Sizes) {
		src, err := memfd(fmt.Sprintf("memxfer-src-%d", id+1))
		if err != nil {
			return nil, err
		}
		e.src = append(e.src, src)
		if _, err := io.Copy(src, io.LimitReader(content(cfg.Seed, uint64(id+1)), cfg.Sizes[id])); err != nil {
			return nil, err
		}
		dst, err := memfd(fmt.Sprintf("memxfer-dst-%d", id+1))
		if err != nil {
			return nil, err
		}
		e.dst = append(e.dst, dst)
	}

	// Serve returns without waiting for its handlers, so the environment
	// counts them through the server's events, joining them as a process
	// exit would before a restart reuses the store.
	sink := cfg.Events
	if sink == nil {
		sink = events.NewSink()
	}
	sink.Subscribe(func(ev events.Event) {
		switch ev.Name {
		case "accept":
			e.handlers.Add(1)
		case "conn_close":
			e.handlers.Add(-1)
		}
	})
	serverOpts := ftcp.ServerOptions{Events: sink, RequireAuth: cfg.Mode.RequireAuth}
	if cfg.KeepAlive {
		serverOpts.KeepAliveTimeout = cfg.KeepAliveTimeout
		if serverOpts.KeepAliveTimeout <= 0 {
			serverOpts.KeepAliveTimeout = time.Minute
		}
	}
	clientOpts := []tx.ClientOption{tx.WithComp(cfg.Comp)}
	if cfg.Mode.Encrypt != "" {
		serverID, err := age.GenerateX25519Identity()
		if err != nil {
			return nil, err
		}
		serverOpts.ServerIdentity = serverID
		clientOpts = append(clientOpts, tx.WithEncryptMode(cfg.Mode.Encrypt))
		if cfg.Mode.Token {
			token, err := aead.NewAuthToken()
			if err != nil {
				return nil, err
			}
			serverOpts.AllowedAuthTokens = []string{token}
			clientOpts = append(clientOpts, tx.WithClientAuthTokens(token))
		} else {
			clientID, err := age.GenerateX25519Identity()
			if err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts,
				tx.WithClientAgePublicKey(clientID.Recipient().String()),
				tx.WithClientAgeIdentity(clientID.String()))
			if cfg.Mode.AllowClient {
				serverOpts.AllowedClientRecipients = []string{clientID.Recipient().String()}
			}
		}
	}
	e.conc = cfg.Concurrency
	if e.conc <= 0 {
		e.conc = 4
	}
	e.clientOpts = append(clientOpts, tx.WithConcurrency(e.conc))
	e.deps = &deps{Deps: ftcp.NewRuntimeDeps(e.st), files: e.src}
	serverOpts.Deps = e.deps
	e.serverOpts = serverOpts
	if err := e.serve(cfg.Faults); err != nil {
		return nil, err
	}
	return e, e.register()
}

// serve starts a server and a client with inj.
func (e *Env) serve(inj *faults.Injector) error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	e.ln, e.served, e.cfg.Faults = ln, make(chan error, 1), inj
	opts := e.serverOpts
	opts.Faults = inj
	go func(served chan<- error) { served <- ftcp.Serve(ln, opts) }(e.served)
	e.client = tx.NewClient(ln.Addr().String(), append(e.clientOpts, tx.WithFaults(inj))...)
	return nil
}

// Restart stops the server and client and starts new ones with inj, nil
// for none, over the same transfer state and files, as rerunning both
// processes would. It first waits for every old server handler to finish,
// so none can change the state under the new ones, and fails if one is
// still running after handlerWait. No round may be running.
func (e *Env) Restart(inj *faults.Injector) error {
	if err := e.stop(); err != nil {
		return err
	}
	return e.serve(inj)
}

// handlerWait bounds how long stop waits for server handlers to finish
// once their client has closed its connections.
const handlerWait = 2 * time.Second

// stop closes the client, whose connections ending ends their handlers,
// then the listener, and waits for the handlers to finish.
func (e *Env) stop() error {
	if e.client != nil {
		e.client.Close()
		e.client = nil
	}
	if e.ln != nil {
		_ = e.ln.Close()
		<-e.served
		e.ln = nil
	}
	for deadline := time.Now().Add(handlerWait); e.handlers.Load() > 0; {
		if time.Now().After(deadline) {
			return fmt.Errorf("%d server handlers still running %v after their client closed", e.handlers.Load(), handlerWait)
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// deps serves file i from its memfd instead of opening a path. Serve
// returns without waiting for its handlers, so one can still open a file
// as the environment closes; closed makes it fail instead of racing the
// close. A descriptor opened earlier is the handler's own and stays valid.
type deps struct {
	ftcp.Deps
	mu     sync.RWMutex
	closed bool
	files  []*os.File
}

func (d *deps) OpenFileRef(ref ftcp.FileRef) (*os.File, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, os.ErrClosed
	}
	if ref.FileID == 0 || ref.FileID > uint64(len(d.files)) {
		return nil, &store.FileLookupError{Code: 404, Msg: "file not found"}
	}
	return dup(d.files[ref.FileID-1])
}

func (e *Env) register() error {
	tr, err := e.st.NewTransfer(root, 0, 0, store.WithRequestPath(root))
	if err != nil {
		return err
	}
	e.transferID = tr.ID
	updates := make(chan store.TransferFileStateUpdate, len(e.cfg.Sizes)+1)
	updates <- store.TransferFileStateUpdate{FileID: encoding.RootFileID, EntryType: encoding.EntryTypeDir, PathHash: xxh3.Hash128([]byte(root))}
	entries := make([]tx.ManifestEntry, 0, len(e.cfg.Sizes))
	for i, size := range e.cfg.Sizes {
		id := uint64(i + 1)
		rel := fmt.Sprintf("d%02d/f%05d", id%16, id)
		updates <- store.TransferFileStateUpdate{FileID: id, EntryType: encoding.EntryTypeFile, PathHash: xxh3.Hash128([]byte(path.Join(root, rel))), FileSize: size}
		entries = append(entries, tx.ManifestEntry{ID: id, Type: encoding.EntryTypeFile, Path: rel, Size: size, Mtime: 1, Mode: 0o644})
	}
	close(updates)
	<-e.st.RegisterTransferFileState(tr.ID, updates, store.TransferStateStarted)
	e.st.ClipTransfer(tr.ID)
	e.manifest = &tx.Manifest{TransferID: tr.ID, Root: root, Mode: tx.LoadStrategyFast, Concurrency: 4, Entries: entries}
	return nil
}

// Pending returns the IDs of the files the server has not marked done.
func (e *Env) Pending() []uint64 {
	tr, ok := e.st.GetTransfer(e.transferID)
	var out []uint64
	for _, entry := range e.manifest.Entries {
		if !ok || int(entry.ID) >= len(tr.State) || tr.State[entry.ID] != store.TransferStateDone {
			out = append(out, entry.ID)
		}
	}
	return out
}

// Round downloads every pending file once, resuming each from the bytes
// the server last acknowledged, as a resumed tx recv copy does.
func (e *Env) Round(ctx context.Context) (tx.StartFromManifestResponse, error) {
	e.unsynced = 0
	tr, ok := e.st.GetTransfer(e.transferID)
	if !ok {
		return tx.StartFromManifestResponse{}, errors.New("transfer expired")
	}
	// GetFiles resumes from the manifest's entries, not the request's, as
	// tx recv copy's manifest carries its .tx/ progress.
	m := e.manifest
	manifest := tx.Manifest{TransferID: m.TransferID, Root: m.Root, Mode: m.Mode, Concurrency: m.Concurrency, Entries: slices.Clone(m.Entries)}
	var entries []tx.ManifestEntry
	for _, id := range e.Pending() {
		manifest.Entries[id-1].Progress.AckBytes = tr.AckedSize[id]
		entries = append(entries, manifest.Entries[id-1])
	}
	if len(entries) == 0 {
		return tx.StartFromManifestResponse{}, nil
	}
	worker, stop := fsync.StartSyncWorker(1<<20, false, 0, io.Discard, fsync.WithFaults(e.cfg.Faults))
	defer func() {
		stop(context.WithoutCancel(ctx))
		e.unsynced = worker.Unsynced()
	}()
	e.openedMu.Lock()
	e.opened = map[uint64][]int64{}
	e.openedMu.Unlock()
	return e.client.StartFromManifest(ctx, tx.StartFromManifestRequest{
		Manifest: &manifest,
		Entries:  entries,
		OutputWriter: func(entry tx.ManifestEntry, offset int64) (io.WriteCloser, func() error, error) {
			fd, err := dup(e.dst[entry.ID-1])
			if err != nil {
				return nil, nil, err
			}
			if _, err := fd.Seek(offset, io.SeekStart); err != nil {
				_ = fd.Close()
				return nil, nil, err
			}
			w := e.cfg.Faults.WrapWriter(faults.SiteOutputWrite, entry.ID, offset, fd)
			e.open.Add(1)
			e.openedMu.Lock()
			e.opened[entry.ID] = append(e.opened[entry.ID], offset)
			e.openedMu.Unlock()
			return countingWriter{WriteCloser: w, e: e}, worker.SyncOutput(fd, offset), nil
		},
		Concurrency:        e.conc,
		BatchMaxBytes:      1 << 20,
		SplitWindowWorkers: 2,
	})
}

// Opened returns the offsets at which the last round opened file id's
// outputs, in the order it opened them.
func (e *Env) Opened(id uint64) []int64 {
	e.openedMu.Lock()
	defer e.openedMu.Unlock()
	return slices.Clone(e.opened[id])
}

// Unsynced returns how many outputs the last round's background syncs gave
// up on, by a failed fdatasync or fstat, which no writer hears about.
func (e *Env) Unsynced() int64 { return e.unsynced }

// AckedBytes returns how many of file id's bytes the server has
// acknowledged.
func (e *Env) AckedBytes(id uint64) int64 {
	tr, ok := e.st.GetTransfer(e.transferID)
	if !ok || int(id) >= len(tr.AckedSize) {
		return 0
	}
	return tr.AckedSize[id]
}

// Done returns the IDs of the files the server has marked done.
func (e *Env) Done() []uint64 {
	pending := e.Pending()
	var out []uint64
	for _, entry := range e.manifest.Entries {
		if !slices.Contains(pending, entry.ID) {
			out = append(out, entry.ID)
		}
	}
	return out
}

// Verify runs tx recv copy --verify full's checks of every file: first each
// copy's size, as its metadata stage does, then its data against the
// server, through the client. It returns nil only when every size and byte
// matched. A budget above zero bounds the data pass: dispatch stops when it
// expires, and checks still running are cancelled a quarter of it later.
func (e *Env) Verify(budget time.Duration) error {
	for i, size := range e.cfg.Sizes {
		info, err := e.dst[i].Stat()
		if err != nil {
			return err
		}
		if info.Size() != size {
			return fmt.Errorf("file %d holds %d bytes, want %d", i+1, info.Size(), size)
		}
	}
	return filexfercli.VerifyCopy(e.client, e.manifest, func(entry tx.ManifestEntry) string {
		return fmt.Sprintf("/proc/self/fd/%d", e.dst[entry.ID-1].Fd())
	}, filexfercli.VerifyCopyOptions{Workers: e.conc, Budget: budget, Grace: budget / 4})
}

// Writes returns how many output Write calls the client has made.
func (e *Env) Writes() int64 { return e.writes.Load() }

// InFlight returns how many outputs the client has not closed and how many
// of its output Write calls have not returned.
func (e *Env) InFlight() (open, writing int64) { return e.open.Load(), e.writing.Load() }

type countingWriter struct {
	io.WriteCloser
	e *Env
}

func (w countingWriter) Write(p []byte) (int, error) {
	w.e.writes.Add(1)
	w.e.writing.Add(1)
	defer w.e.writing.Add(-1)
	return w.WriteCloser.Write(p)
}

func (w countingWriter) Close() error {
	defer w.e.open.Add(-1)
	return w.WriteCloser.Close()
}

// Mismatched returns the IDs of the files whose sink differs from the
// source in size or content.
func (e *Env) Mismatched() []uint64 {
	var out []uint64
	for i, size := range e.cfg.Sizes {
		id := uint64(i + 1)
		got, err := hashFile(e.dst[i], size)
		want := xxh3.New128()
		_, _ = io.Copy(want, io.LimitReader(content(e.cfg.Seed, id), size))
		if err != nil || got != want.Sum128() {
			out = append(out, id)
		}
	}
	return out
}

// Client returns the environment's client.
func (e *Env) Client() *tx.Client { return e.client }

// Close stops the server and client and closes every file.
func (e *Env) Close() {
	_ = e.stop()
	if e.deps != nil {
		e.deps.mu.Lock()
		e.deps.closed = true
		e.deps.mu.Unlock()
	}
	e.st.Close()
	for _, f := range append(e.src, e.dst...) {
		_ = f.Close()
	}
}

// content returns file id's bytes: a ChaCha8 stream keyed by seed and id.
func content(seed, id uint64) io.Reader {
	var key [32]byte
	for i := range 8 {
		key[i] = byte(seed >> (8 * i))
		key[8+i] = byte(id >> (8 * i))
	}
	return rand.NewChaCha8(key)
}

// hashFile hashes f's contents if it holds exactly size bytes.
func hashFile(f *os.File, size int64) (xxh3.Uint128, error) {
	info, err := f.Stat()
	if err != nil {
		return xxh3.Uint128{}, err
	}
	if info.Size() != size {
		return xxh3.Uint128{}, fmt.Errorf("size %d, want %d", info.Size(), size)
	}
	h := xxh3.New128()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, size)); err != nil {
		return xxh3.Uint128{}, err
	}
	return h.Sum128(), nil
}

func memfd(name string) (*os.File, error) {
	fd, err := unix.MemfdCreate(name, 0)
	if err != nil {
		return nil, fmt.Errorf("memfd_create: %w", err)
	}
	return os.NewFile(uintptr(fd), name), nil
}

// dup returns a new descriptor for f, with its own file offset.
func dup(f *os.File) (*os.File, error) {
	// A memfd's /proc/self/fd entry opens a fresh description, so each
	// reader and writer has its own offset.
	return os.OpenFile(fmt.Sprintf("/proc/self/fd/%d", f.Fd()), os.O_RDWR, 0)
}
