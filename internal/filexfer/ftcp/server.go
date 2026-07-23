package ftcp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/pagecache"
)

const (
	maxCommandLineBytes = 4 * 1024 * 1024
)

func gentleLimiterBurstBytes(limiter *limit.Limiter) int64 {
	if limiter == nil {
		return 1 * 1024 * 1024
	}
	cfg := limiter.Config()
	if cfg.BurstBytes > 0 {
		return cfg.BurstBytes
	}
	return 1 * 1024 * 1024
}

type ServerOptions struct {
	RequireAuth            bool
	AllowedAuthTokens      []string // if non-empty, client AUTH must present a matching token
	ServerIdentity         *age.X25519Identity
	Deps                   Deps
	Limiter                *limit.Limiter
	GentleCPUPct           int
	GentleBWPct            int
	SocketWriteBufferBytes int
	SyncTimeout            time.Duration             // 0 = no timeout; bounds SYNC response write time
	RootDir                string                    // "/" or "" means unrestricted
	ProgressTargets        []filexfer.ProgressTarget // progress output targets
	ProgressInterval       time.Duration             // tick interval for progress writes (default 1s)
	DisableZeroCopy        bool                      // force buffered send path even when zero-copy is available
	TargetIODepth          int                       // target IO depth per CPU advertised in PROBE (default 4)
	ExitAfter              time.Duration             // 0 = disabled; exit this long after last transfer completes
	KeepAliveTimeout       time.Duration             // idle window for kept-alive connections; 0 = keep-alive disabled (close after every verb)
}

type HandlerFunc func(context.Context, Request, io.Writer, Deps) error

var handlers = map[Verb]HandlerFunc{
	VerbAUTH:   handleAUTHCommand,
	VerbTXFER:  handleTXFER,
	VerbSEND:   handleSEND,
	VerbACK:    handleACK,
	VerbCXSUM:  handleCXSUM,
	VerbSTATUS: handleSTATUS,
	VerbPROBE:  handlePROBECommand,
	VerbSYNC:   handleSYNCCommand,
}

func Serve(listener net.Listener, opts ServerOptions) error {
	if listener == nil {
		return errors.New("nil listener")
	}
	deps := opts.Deps
	if deps == nil {
		pool := pagecache.NewRestoreWorkerPool(0, 0)
		defer pool.Close()
		deps = NewRuntimeDepsWithPool(opts.RootDir, pool)
	}

	var onTransferCreated func(string)
	if len(opts.ProgressTargets) > 0 {
		interval := opts.ProgressInterval
		if interval <= 0 {
			interval = time.Second
		}
		var activeTransferID atomic.Value
		onTransferCreated = func(id string) { activeTransferID.Store(id) }
		stopProgress := filexfer.StartProgressFileWriter(
			context.Background(), opts.ProgressTargets, interval, func() filexfer.ProgressStatus {
				id, _ := activeTransferID.Load().(string)
				if id == "" {
					return filexfer.ProgressStatus{Source: "server"}
				}
				t, ok := deps.GetTransfer(id)
				if !ok {
					return filexfer.ProgressStatus{Source: "server", TxID: id}
				}
				return filexfer.ProgressStatus{
					Source:     "server",
					TxID:       id,
					DoneFiles:  t.Done,
					TotalFiles: uint64(t.NumFiles),
					DoneBytes:  t.DoneSize,
					TotalBytes: t.TotalSize,
				}
			})
		defer stopProgress(true)
	}

	var onClientActivity func()
	if opts.ExitAfter > 0 {
		log.Printf("exit-after: server will exit %s after the last activity", opts.ExitAfter)
		var (
			timerMu   sync.Mutex
			exitTimer *time.Timer
		)
		resetExitTimer := func() {
			timerMu.Lock()
			defer timerMu.Unlock()
			if exitTimer != nil {
				exitTimer.Stop()
				exitTimer = time.AfterFunc(opts.ExitAfter, func() {
					log.Printf("exit-after: %s elapsed since last transfer completed, shutting down", opts.ExitAfter)
					listener.Close()
				})
			}
		}
		cancelExitTimer := func() {
			timerMu.Lock()
			defer timerMu.Unlock()
			if exitTimer != nil {
				exitTimer.Stop()
				exitTimer = nil
			}
		}
		startExitTimer := func(string) {
			timerMu.Lock()
			defer timerMu.Unlock()
			if exitTimer != nil {
				exitTimer.Stop()
			}
			exitTimer = time.AfterFunc(opts.ExitAfter, func() {
				log.Printf("exit-after: %s elapsed since last transfer completed, shutting down", opts.ExitAfter)
				listener.Close()
			})
			log.Printf("exit-after: transfer complete, will exit in %s unless new activity", opts.ExitAfter)
		}
		onClientActivity = resetExitTimer
		origOnCreated := onTransferCreated
		onTransferCreated = func(id string) {
			if origOnCreated != nil {
				origOnCreated(id)
			}
			cancelExitTimer()
		}
		deps = &exitAfterDeps{Deps: deps, onComplete: startExitTimer}
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if _, ok := err.(net.Error); ok {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		go handleConn(conn, opts, deps, onTransferCreated, onClientActivity)
	}
}

type exitAfterDeps struct {
	Deps
	onComplete func(string)
	notified   sync.Map
}

func (d *exitAfterDeps) MaybeLogTransferComplete(txferID string) {
	d.Deps.MaybeLogTransferComplete(txferID)
	if t, ok := d.Deps.GetTransfer(txferID); ok && t.CompleteLogged {
		if _, loaded := d.notified.LoadOrStore(txferID, struct{}{}); !loaded {
			d.onComplete(txferID)
		}
	}
}

type connSession struct {
	conn                   net.Conn
	requireAuth            bool
	allowedAuthTokens      []string
	serverID               *age.X25519Identity
	matchedAuthToken       string
	deps                   Deps
	limiter                *limit.Limiter
	gentleCPUPct           int
	gentleBWPct            int
	socketWriteBufferBytes int
	syncTimeout            time.Duration
	disableZeroCopy        bool
	targetIODepth          int
	respOut                io.Writer
	cmdBufReader           *bufio.Reader // reused per-command decrypt reader for encrypted kept-alive sessions
	onTransferCreated      func(string)
	onClientActivity       func()
	keepAliveTimeout       time.Duration
	keepAlive              bool
	clientRecipient        age.Recipient
	responseCipher         aead.Algorithm
	encryptedRequests      bool
}

func handleConn(conn net.Conn, opts ServerOptions, deps Deps, onTransferCreated func(string), onClientActivity func()) {
	defer conn.Close()
	keepAliveTimeout := opts.KeepAliveTimeout
	if keepAliveTimeout < 0 {
		keepAliveTimeout = 0
	}
	s := &connSession{
		conn:                   conn,
		requireAuth:            opts.RequireAuth,
		allowedAuthTokens:      opts.AllowedAuthTokens,
		serverID:               opts.ServerIdentity,
		deps:                   deps,
		limiter:                opts.Limiter,
		gentleCPUPct:           opts.GentleCPUPct,
		gentleBWPct:            opts.GentleBWPct,
		socketWriteBufferBytes: opts.SocketWriteBufferBytes,
		syncTimeout:            opts.SyncTimeout,
		disableZeroCopy:        opts.DisableZeroCopy,
		targetIODepth:          opts.TargetIODepth,
		respOut:                conn,
		onTransferCreated:      onTransferCreated,
		onClientActivity:       onClientActivity,
		keepAliveTimeout:       keepAliveTimeout,
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		if s.socketWriteBufferBytes > 0 {
			_ = tc.SetWriteBuffer(s.socketWriteBufferBytes)
		}
	}
	if err := s.run(); err != nil {
		s.reportError(err)
	}
}

// newResponseWriter returns the writer for one command's response plus a
// close function that finalizes it. Encrypted sessions get a fresh AEAD
// stream per response so each response is self-delimiting — a requirement
// for kept-alive connections where responses follow each other on one
// socket.
func (s *connSession) newResponseWriter() (io.Writer, func() error, error) {
	if s.clientRecipient == nil {
		return s.respOut, func() error { return nil }, nil
	}
	encOut, err := aead.Encrypt(s.conn, s.clientRecipient, aead.Options{Algorithm: s.responseCipher})
	if err != nil {
		return nil, nil, err
	}
	return encOut, encOut.Close, nil
}

// reportError best-effort reports err to the client before the connection
// closes, riding the session's current response encryption (if any) so an
// established session never leaks a plaintext error frame. The connection is
// about to be torn down either way, so any error encountered while
// reporting is swallowed.
func (s *connSession) reportError(err error) {
	respOut, closeResp, wErr := s.newResponseWriter()
	if wErr != nil {
		return
	}
	_ = writeErrFrame(respOut, err)
	_ = closeResp()
}

// isBenignDisconnect reports whether err represents an expected, silent
// connection teardown — client EOF, an already-closed net.Conn, or the idle
// reaper's read deadline firing — as opposed to a protocol or decrypt
// failure worth reporting to the client.
func isBenignDisconnect(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// probeRequestsKeepAlive reports whether a PROBE's params ask for
// server-tuned connection reuse (keep-alive=auto).
func probeRequestsKeepAlive(p map[string]string) bool {
	return strings.EqualFold(strings.TrimSpace(p["keep-alive"]), "auto")
}

// isKeepAliveHeartbeat reports whether req is a pure keep-alive heartbeat
// (zero-payload PROBE requesting keep-alive). Heartbeats keep the
// connection's idle deadline fresh but must not count as client activity for
// --exit-after, or a lingering idle pool would block server shutdown
// indefinitely.
func isKeepAliveHeartbeat(req Request) bool {
	if req.Verb != VerbPROBE || len(req.Params) != 1 {
		return false
	}
	p := req.Params[0]
	return probeRequestsKeepAlive(p) && strings.TrimSpace(p["probe-bytes"]) == "0"
}

func (s *connSession) noteClientActivity(req Request) {
	if s.onClientActivity != nil && !isKeepAliveHeartbeat(req) {
		s.onClientActivity()
	}
}

// readCommand reads and parses the next command line. When idleTimeout > 0
// the read runs under a receive deadline: kept-alive connections whose
// client stops sending (commands or heartbeat PROBEs) are reaped instead of
// lingering as silently dead sockets. Encrypted sessions wrap each command
// in its own AEAD stream, mirroring newResponseWriter.
func (s *connSession) readCommand(br *bufio.Reader, idleTimeout time.Duration) (Request, *bufio.Reader, error) {
	if idleTimeout > 0 {
		_ = s.conn.SetReadDeadline(time.Now().Add(idleTimeout))
		defer func() { _ = s.conn.SetReadDeadline(time.Time{}) }()
	}
	cmdReader := br
	if s.encryptedRequests {
		decIn, decErr := aead.DecryptWithOptions(br, s.serverID, aead.Options{Algorithm: s.responseCipher})
		if decErr != nil {
			return Request{}, nil, decErr
		}
		// One goroutine per connection processes commands serially, and each
		// command's decrypt stream is a self-delimiting AEAD stream that
		// returns EOF exactly at its boundary without over-reading into the
		// next command's bytes on br. serveCommand fully completes before the
		// next readCommand, so resetting the reader (discarding its prior
		// buffer) is equivalent to allocating a fresh one, minus the ~4KB
		// per-command allocation on kept-alive encrypted sessions.
		if s.cmdBufReader == nil {
			s.cmdBufReader = bufio.NewReader(decIn)
		} else {
			s.cmdBufReader.Reset(decIn)
		}
		cmdReader = s.cmdBufReader
	}
	cmdPayload, err := readCommandLine(cmdReader, maxCommandLineBytes)
	if err != nil {
		return Request{}, nil, err
	}
	req, err := ParseRequest(cmdPayload)
	if err != nil {
		return Request{}, nil, err
	}
	return req, cmdReader, nil
}

func (s *connSession) run() error {
	br := bufio.NewReader(s.conn)
	firstPayload, err := readCommandLine(br, maxCommandLineBytes)
	if err != nil {
		return err
	}
	firstReq, err := ParseRequest(firstPayload)
	if err != nil {
		return err
	}

	cmdReq := firstReq
	cmdReader := br
	if firstReq.Verb == VerbAUTH {
		authRes, authErr := processAUTHRequest(firstReq, s.serverID, s.allowedAuthTokens)
		if authErr != nil {
			if errors.Is(authErr, errNotAuthorized) {
				return protocolErr{code: "NOT_AUTHORIZED", message: "authorization failed"}
			}
			return authErr
		}
		s.matchedAuthToken = authRes.matchedToken
		if authRes.keyExchange {
			// AUTH key — return the server's recommended cipher and public key.
			s.noteClientActivity(firstReq)
			return writeOKLine(s.respOut, string(aead.RecommendedCipher())+" "+s.serverID.Recipient().String())
		}
		if authRes.encryptedRequests && s.serverID == nil {
			return protocolErr{code: "NOT_AUTHORIZED", message: "server auth key unavailable"}
		}
		s.clientRecipient = authRes.recipient
		s.responseCipher = authRes.responseCipher
		s.encryptedRequests = authRes.encryptedRequests

		cmdReq, cmdReader, err = s.readCommand(br, 0)
		if err != nil {
			perr := protocolErr{code: "NOT_AUTHORIZED", message: "request decryption failed"}
			if !errors.As(err, &perr) && !s.encryptedRequests {
				return err
			}
			// Report through the session's response encryption so the
			// client can decode the failure, matching prior behavior.
			s.reportError(perr)
			return nil
		}
	} else if s.requireAuth {
		return protocolErr{code: "NOT_AUTHORIZED", message: "missing AUTH"}
	}

	cmdCtx, connTask := trace.NewTask(context.Background(), "tcp-connection")
	defer connTask.End()
	for {
		// A PROBE carrying keep-alive=auto upgrades the connection to a
		// kept-alive session: the server stops closing after each command
		// and instead reaps the connection when no bytes arrive within
		// keepAliveTimeout.
		if cmdReq.Verb == VerbPROBE && s.keepAliveTimeout > 0 &&
			len(cmdReq.Params) == 1 && probeRequestsKeepAlive(cmdReq.Params[0]) {
			s.keepAlive = true
		}

		s.noteClientActivity(cmdReq)
		if !s.serveCommand(cmdCtx, cmdReq, cmdReader) {
			return nil
		}

		if !s.keepAlive {
			return nil
		}
		var err error
		cmdReq, cmdReader, err = s.readCommand(br, s.keepAliveTimeout)
		if err != nil {
			// Parse-level and decrypt-level errors get reported before
			// closing; benign disconnects (idle timeout, EOF, an
			// already-closed conn) just close silently.
			if !isBenignDisconnect(err) {
				var perr protocolErr
				if !errors.As(err, &perr) {
					if s.encryptedRequests {
						perr = protocolErr{code: "NOT_AUTHORIZED", message: "request decryption failed"}
					} else {
						perr = protocolErr{code: "BAD_REQUEST", message: "invalid command"}
					}
				}
				s.reportError(perr)
			}
			return nil
		}
	}
}

// serveCommand runs one command to completion: it opens a response writer,
// dispatches to handleCommand, writes the verb-level OK terminator for
// streaming verbs, and closes the response. It reports whether the
// connection should stay open for another command (true) or be torn down
// (false).
//
// A handler error is written as an ERR frame on the same already-open
// respOut and the response is closed — s.reportError must not be used here,
// since that would open a *new* response writer while this one is
// mid-stream. A failure writing the OK line or closing the response means
// the socket is dead, so the connection is closed silently without
// attempting to report anything on it.
func (s *connSession) serveCommand(ctx context.Context, req Request, in *bufio.Reader) bool {
	respOut, closeResp, err := s.newResponseWriter()
	if err != nil {
		return false
	}
	if err := s.handleCommand(ctx, req, in, respOut); err != nil {
		_ = writeErrFrame(respOut, err)
		_ = closeResp()
		return false
	}
	if req.Verb == VerbTXFER || req.Verb == VerbSEND || req.Verb == VerbCXSUM || req.Verb == VerbPROBE || req.Verb == VerbSYNC {
		if err := writeOKLine(respOut, ""); err != nil {
			return false
		}
	}
	if err := closeResp(); err != nil {
		return false
	}
	return true
}

func (s *connSession) handleCommand(ctx context.Context, req Request, in io.Reader, out io.Writer) error {
	if req.Verb == VerbSEND {
		return handleSENDWithOptions(ctx, req, out, s.deps, s.limiter, s.disableZeroCopy, s.gentleBWPct)
	}
	if req.Verb == VerbPROBE {
		keepAliveMS := int64(0)
		if s.keepAlive {
			keepAliveMS = s.keepAliveTimeout.Milliseconds()
		}
		return handlePROBEWithInput(ctx, req, in, out, s.deps, s.targetIODepth, s.gentleCPUPct, s.gentleBWPct, gentleLimiterBurstBytes(s.limiter), keepAliveMS)
	}
	if req.Verb == VerbSYNC {
		if s.syncTimeout > 0 {
			_ = s.conn.SetWriteDeadline(time.Now().Add(s.syncTimeout))
			// The deadline must not outlive this command: on a kept-alive
			// connection a stale write deadline would fail every response
			// written after it expires.
			defer func() { _ = s.conn.SetWriteDeadline(time.Time{}) }()
		}
		return handleSYNCWithInput(ctx, req, in, out, s.deps, s.onTransferCreated)
	}
	if req.Verb == VerbTXFER {
		if len(s.matchedAuthToken) >= 2 {
			log.Printf("txfer: auth token %s...", s.matchedAuthToken[:2])
		}
		return handleTXFERWithCallback(ctx, req, out, s.deps, s.onTransferCreated)
	}
	handler, ok := handlers[req.Verb]
	if !ok || req.Verb == VerbUnknown {
		return protocolErr{code: "BAD_COMMAND", message: "unknown command"}
	}
	return handler(ctx, req, out, s.deps)
}

func readCommandLine(r *bufio.Reader, maxBytes int) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if maxBytes > 0 && len(line) > maxBytes {
		return nil, errors.New("command line too large")
	}
	if len(line) == 0 {
		return nil, errors.New("empty command")
	}
	if line[len(line)-1] != '\n' {
		return nil, errors.New("invalid line terminator")
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if len(line) == 0 {
		return nil, errors.New("empty command")
	}
	return line, nil
}
