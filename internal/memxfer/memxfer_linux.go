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
	"sync/atomic"
	"time"

	"filippo.io/age"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/aead"
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
	// only with a permit. A token allowlist already does both, so token
	// modes can be refused as busy either way.
	RequireAuth bool
}

// Modes lists every session mode tx supports: plaintext, and each cipher
// with each credential, plus one of each credential on a server that
// requires auth.
func Modes() []Mode {
	modes := []Mode{{}}
	for _, token := range []bool{true, false} {
		for _, enc := range []string{"aes", "chacha20", "auto"} {
			modes = append(modes, Mode{Encrypt: enc, Token: token})
		}
	}
	return append(modes,
		Mode{Encrypt: "aes", Token: true, RequireAuth: true},
		Mode{Encrypt: "chacha20", RequireAuth: true})
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
	client     *tx.Client
	src, dst   []*os.File // file ID i is index i-1
	manifest   *tx.Manifest
	transferID string
	writes     atomic.Int64 // output Write calls, across rounds
	open       atomic.Int64 // outputs not yet closed
	writing    atomic.Int64 // output Write calls not yet returned
}

const root = "/memxfer"

// Start creates the files, starts the server and client, and registers the
// transfer, as TXFER would.
func Start(cfg Config) (_ *Env, err error) {
	e := &Env{cfg: cfg, st: store.NewStore(), served: make(chan error, 1)}
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

	serverOpts := ftcp.ServerOptions{Faults: cfg.Faults, Events: cfg.Events, RequireAuth: cfg.Mode.RequireAuth}
	if cfg.KeepAlive {
		serverOpts.KeepAliveTimeout = cfg.KeepAliveTimeout
		if serverOpts.KeepAliveTimeout <= 0 {
			serverOpts.KeepAliveTimeout = time.Minute
		}
	}
	clientOpts := []tx.ClientOption{tx.WithFaults(cfg.Faults), tx.WithComp(cfg.Comp)}
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
		}
	}
	e.conc = cfg.Concurrency
	if e.conc <= 0 {
		e.conc = 4
	}
	clientOpts = append(clientOpts, tx.WithConcurrency(e.conc))

	e.ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	serverOpts.Deps = deps{Deps: ftcp.NewRuntimeDeps(e.st), files: e.src}
	go func() { e.served <- ftcp.Serve(e.ln, serverOpts) }()
	e.client = tx.NewClient(e.ln.Addr().String(), clientOpts...)
	return e, e.register()
}

// deps serves file i from its memfd instead of opening a path.
type deps struct {
	ftcp.Deps
	files []*os.File
}

func (d deps) OpenFileRef(ref ftcp.FileRef) (*os.File, error) {
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
	tr, ok := e.st.GetTransfer(e.transferID)
	if !ok {
		return tx.StartFromManifestResponse{}, errors.New("transfer expired")
	}
	var entries []tx.ManifestEntry
	for _, id := range e.Pending() {
		entry := e.manifest.Entries[id-1]
		entry.Progress.AckBytes = tr.AckedSize[id]
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return tx.StartFromManifestResponse{}, nil
	}
	worker, stop := fsync.StartSyncWorker(1<<20, false, 0, io.Discard, fsync.WithFaults(e.cfg.Faults))
	defer stop(context.WithoutCancel(ctx))
	return e.client.StartFromManifest(ctx, tx.StartFromManifestRequest{
		Manifest: e.manifest,
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
			return countingWriter{WriteCloser: w, e: e}, worker.SyncOutput(fd, offset), nil
		},
		Concurrency:        e.conc,
		BatchMaxBytes:      1 << 20,
		SplitWindowWorkers: 2,
	})
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
	if e.client != nil {
		e.client.Close()
	}
	if e.ln != nil {
		_ = e.ln.Close()
		<-e.served
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
