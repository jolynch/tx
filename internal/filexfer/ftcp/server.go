package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"filippo.io/age"
	"github.com/jolynch/tx/internal/aead"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/filexfer/store"
	"github.com/jolynch/tx/internal/pagecache"
	"github.com/jolynch/tx/internal/utils"
)

const (
	// maxCommandLineBytes bounds a command line. Command lines carry headers
	// only — a verb, at most one path, and a few short options — since every
	// argument list that grows with the transfer now travels in a framed body.
	// This is a protocol limit, independent of filesystem path limits.
	maxCommandLineBytes = 64 * 1024

	// defaultMaxSyncBodyBytes bounds the prior manifest a SYNC may upload.
	//
	// Unlike the item bodies, this one cannot be streamed away: readOldManifest
	// must hold the whole prior manifest indexed before it can walk the tree
	// and decide what changed. The limit is therefore a memory bound on that
	// index, not a wire-format limit. At roughly 100 front-coded bytes per
	// entry this allows a manifest of about ten million files.
	defaultMaxSyncBodyBytes int64 = 1 << 30
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
	// RequireAuth rejects plaintext commands. With an empty allowlist it
	// accepts any age client that proves possession of its private key.
	RequireAuth bool
	// AllowedAuthTokens holds secret tokens, and AllowedClientRecipients
	// public client age recipients. Any entry in either implies RequireAuth
	// and restricts access to the listed credentials.
	AllowedAuthTokens       []string
	AllowedClientRecipients []string
	// ServerIdentity is the key the server proves in every handshake; nil
	// generates one at startup.
	ServerIdentity *age.X25519Identity
	// MaxUnauthenticatedConns bounds connections that have not completed
	// FINISH; zero selects 1024 and a negative value is invalid.
	MaxUnauthenticatedConns int
	Deps                    Deps
	Limiter                 *limit.Limiter
	GentleCPUPct            int
	GentleBWPct             int
	SocketWriteBufferBytes  int
	SyncTimeout             time.Duration             // 0 = no timeout; bounds SYNC response write time
	MaxSyncBodyBytes        int64                     // 0 selects defaultMaxSyncBodyBytes; bounds the prior manifest a SYNC may upload
	RootDir                 string                    // "/" or "" means unrestricted
	ProgressTargets         []filexfer.ProgressTarget // progress output targets
	ProgressInterval        time.Duration             // tick interval for progress writes (default 1s)
	DisableZeroCopy         bool                      // force buffered send path even when zero-copy is available
	TargetIODepth           int                       // target IO depth per CPU advertised in PROBE (default 4)
	ExitAfter               time.Duration             // 0 = disabled; exit this long after last transfer completes
	KeepAliveTimeout        time.Duration             // idle window for kept-alive connections; 0 = keep-alive disabled (close after every verb)
	Events                  *events.Sink              // nil disables the event timeline; also handed to the store Serve creates
	// Faults wraps every accepted connection and gates the server's file and
	// syscall sites; nil, the default, disables fault injection.
	Faults *faults.Injector

	// zeroCopyMinFrameBytes overrides defaultZeroCopyMinFrameBytes when > 0,
	// so tests can drive small frames through the zero-copy path.
	zeroCopyMinFrameBytes int64
}

// zeroCopyMinBytes is the smallest frame these options send zero-copy, or
// zeroCopyOff.
func (o ServerOptions) zeroCopyMinBytes() int64 {
	switch {
	case o.DisableZeroCopy:
		return zeroCopyOff
	case o.zeroCopyMinFrameBytes > 0:
		return o.zeroCopyMinFrameBytes
	default:
		return defaultZeroCopyMinFrameBytes
	}
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
	auth, err := newAuthPolicy(opts)
	if err != nil {
		return fmt.Errorf("auth configuration: %w", err)
	}
	deps := opts.Deps
	if deps == nil {
		pool := pagecache.NewRestoreWorkerPool(0, 0)
		defer pool.Close()
		st := store.NewStore(store.WithEvents(opts.Events))
		defer st.Close()
		deps = NewRuntimeDeps(st, WithRoot(opts.RootDir), WithPool(pool))
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
				t, ok := deps.GetTransferSummary(id)
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

	if opts.Faults != nil {
		clearFaults := pagecache.SetFaults(opts.Faults)
		defer clearFaults()
	}

	for {
		conn, err := listener.Accept()
		if err == nil {
			// An injected accept error takes the path of a real one; the
			// connection it replaces never reaches a handler.
			if err = opts.Faults.AcceptErr(); err != nil {
				_ = conn.Close()
			}
		}
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
		conn = opts.Faults.WrapConn(conn, faults.Server)
		// A server that requires auth admits a connection only with a
		// permit, held until FINISH. Admission never blocks the loop. Only
		// such a server answers busy, so only it gets busy faults.
		var release func()
		if auth.required {
			r, ok := auth.tryAcquire()
			if !ok || opts.Faults.Busy() {
				if ok {
					r()
				}
				go refuseBusy(conn, nil)
				continue
			}
			release = r
		}
		go handleConn(conn, opts, auth, release, deps, onTransferCreated, onClientActivity)
	}
}

type exitAfterDeps struct {
	Deps
	onComplete func(string)
	notified   sync.Map
}

func (d *exitAfterDeps) MaybeLogTransferComplete(txferID string) bool {
	if !d.Deps.MaybeLogTransferComplete(txferID) {
		return false
	}
	if _, loaded := d.notified.LoadOrStore(txferID, struct{}{}); !loaded {
		d.onComplete(txferID)
	}
	return true
}

type connSession struct {
	conn                   net.Conn
	auth                   *authPolicy
	acceptedAt             time.Time
	releasePermit          func() // admission permit, nil until held
	noise                  *aead.Session
	deps                   Deps
	limiter                *limit.Limiter
	gentleCPUPct           int
	gentleBWPct            int
	socketWriteBufferBytes int
	syncTimeout            time.Duration
	maxSyncBodyBytes       int64
	zeroCopyMinBytes       int64
	targetIODepth          int
	respOut                io.Writer
	faults                 *faults.Injector
	onTransferCreated      func(string)
	onClientActivity       func()
	keepAliveTimeout       time.Duration
	keepAlive              bool
	// grantKeepAlive is the keep-alive state a PROBE reply advertises: the
	// current grant, or the one this PROBE requests, committed only after
	// the command succeeds.
	grantKeepAlive bool
	events         events.Scope
}

// handleConn confines a panic to this connection and logs its stack. Panics in
// other goroutines are outside this recover's scope.
func handleConn(conn net.Conn, opts ServerOptions, auth *authPolicy, release func(), deps Deps, onTransferCreated func(string), onClientActivity func()) {
	defer conn.Close()
	defer func() {
		if r := recover(); r != nil {
			// The stream may be incomplete; close it without writing an ERR frame.
			log.Printf("PANIC serving %v: %v\n%s", conn.RemoteAddr(), r, debug.Stack())
		}
	}()
	keepAliveTimeout := opts.KeepAliveTimeout
	if keepAliveTimeout < 0 {
		keepAliveTimeout = 0
	}
	maxSyncBodyBytes := opts.MaxSyncBodyBytes
	if maxSyncBodyBytes <= 0 {
		maxSyncBodyBytes = defaultMaxSyncBodyBytes
	}
	var scope events.Scope
	if opts.Events.Enabled() {
		scope = events.Scope{Sink: opts.Events, Conn: opts.Events.NextConn()}
		opts.Events.Emit("accept", "", events.F("conn", scope.Conn), events.F("remote", conn.RemoteAddr().String()))
	}
	s := &connSession{
		events:                 scope,
		conn:                   conn,
		auth:                   auth,
		acceptedAt:             time.Now(),
		releasePermit:          release,
		deps:                   deps,
		limiter:                opts.Limiter,
		gentleCPUPct:           opts.GentleCPUPct,
		gentleBWPct:            opts.GentleBWPct,
		socketWriteBufferBytes: opts.SocketWriteBufferBytes,
		syncTimeout:            opts.SyncTimeout,
		maxSyncBodyBytes:       maxSyncBodyBytes,
		zeroCopyMinBytes:       opts.zeroCopyMinBytes(),
		targetIODepth:          opts.TargetIODepth,
		respOut:                conn,
		faults:                 opts.Faults,
		onTransferCreated:      onTransferCreated,
		onClientActivity:       onClientActivity,
		keepAliveTimeout:       keepAliveTimeout,
	}
	// Whatever path ends the connection returns its admission permit, taken
	// at accept or by runAuth; the release is idempotent.
	defer func() {
		if s.releasePermit != nil {
			s.releasePermit()
		}
	}()
	tuneServerSocket(conn, opts.Faults, s.socketWriteBufferBytes)
	err := s.run()
	if err != nil {
		s.reportError(err)
	}
	if scope.Sink.Enabled() {
		reason := "done"
		if err != nil {
			reason = err.Error()
		}
		scope.Sink.Emit("conn_close", "", events.F("conn", scope.Conn), events.F("reason", reason))
	}
}

// tuneServerSocket sets an accepted connection's socket options. They are
// best-effort, so an injected setsockopt failure skips them as a real one
// would.
func tuneServerSocket(conn net.Conn, inj *faults.Injector, writeBufferBytes int) {
	tc := rawTCPConn(conn)
	if tc == nil || inj.Sys(faults.SysSetsockopt) != nil {
		return
	}
	_ = tc.SetNoDelay(true)
	if writeBufferBytes > 0 {
		_ = tc.SetWriteBuffer(writeBufferBytes)
	}
}

// newResponseWriter returns the writer for one command's response plus a
// close function that finalizes it. On an authenticated session each
// response is one Noise message ending in END; a gentle SEND writes each
// record as soon as it is sealed so its rate limiter keeps its pacing.
func (s *connSession) newResponseWriter(req Request) (io.Writer, func() error) {
	if s.noise == nil {
		return s.respOut, func() error { return nil }
	}
	paced := false
	if req.Verb == VerbSEND && len(req.Params) == 1 {
		paced = strings.EqualFold(strings.TrimSpace(req.Params[0]["mode"]), loadStrategyGentle)
	}
	w := s.noise.Writer(paced)
	return w, w.EndFunc()
}

// reportError best-effort reports err to a plaintext client before the
// connection closes. Authenticated sessions report errors as responses or
// close without one, so this never writes plaintext onto a session.
func (s *connSession) reportError(err error) {
	if s.noise != nil {
		return
	}
	_ = writeErrFrame(s.respOut, err)
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

// readCommand reads and parses the next plaintext command line. When
// idleTimeout > 0 the read runs under a receive deadline: kept-alive
// connections whose client stops sending (commands or heartbeat PROBEs) are
// reaped instead of lingering as silently dead sockets.
func (s *connSession) readCommand(br *bufio.Reader, idleTimeout time.Duration) (Request, error) {
	if idleTimeout > 0 {
		_ = s.conn.SetReadDeadline(time.Now().Add(idleTimeout))
		defer func() { _ = s.conn.SetReadDeadline(time.Time{}) }()
	}
	cmdPayload, err := readCommandLine(br, maxCommandLineBytes)
	if err != nil {
		return Request{}, err
	}
	return ParseRequest(cmdPayload)
}

// isAuthLine reports whether a first line uses the AUTH verb, in any case
// and with or without arguments, so every form reaches runAuth.
func isAuthLine(line []byte) bool {
	if len(line) < 4 || !bytes.EqualFold(line[:4], []byte("AUTH")) {
		return false
	}
	return len(line) == 4 || line[4] == ' ' || line[4] == '\t' || line[4] == '\r' || line[4] == '\n'
}

func (s *connSession) run() error {
	br := bufio.NewReader(s.conn)
	lineLimit := maxCommandLineBytes
	if s.auth.required {
		// One absolute deadline from accept, for reads and writes, so a peer
		// cannot hold its admission permit past it; and since the first line
		// must be AUTH, it is capped at the preface size.
		_ = s.conn.SetDeadline(s.acceptedAt.Add(aead.HandshakeTimeout))
		lineLimit = aead.MaxPrefaceBytes
	}
	line, err := utils.ReadLineLimit(br, lineLimit)
	if err != nil {
		if errors.Is(err, utils.ErrLineTooLarge) {
			return protocolErr{code: "BAD_REQUEST", message: "command line too large"}
		}
		return err
	}
	if isAuthLine(line) {
		return s.runAuth(br, line)
	}
	if s.auth.required {
		return protocolErr{code: "NOT_AUTHORIZED", message: "missing AUTH"}
	}
	_ = s.conn.SetDeadline(time.Time{})
	payload, err := parseCommandLine(line)
	if err != nil {
		return err
	}
	cmdReq, err := ParseRequest(payload)
	if err != nil {
		return err
	}

	ctx, end := s.commandContext()
	defer end()
	for {
		if !s.serveOne(ctx, cmdReq, br) {
			return nil
		}
		cmdReq, err = s.readCommand(br, s.keepAliveTimeout)
		if err != nil {
			// Parse-level errors get reported before closing; benign
			// disconnects (idle timeout, EOF, an already-closed conn) just
			// close silently.
			if !isBenignDisconnect(err) {
				var perr protocolErr
				if !errors.As(err, &perr) {
					perr = protocolErr{code: "BAD_REQUEST", message: "invalid command"}
				}
				s.reportError(perr)
			}
			return nil
		}
	}
}

func (s *connSession) commandContext() (context.Context, func()) {
	cmdCtx, connTask := trace.NewTask(context.Background(), "tcp-connection")
	cmdCtx = faults.NewContext(cmdCtx, s.faults)
	return events.WithScope(cmdCtx, s.events), connTask.End
}

// serveOne serves one command and reports whether the connection stays open
// for another. A PROBE carrying keep-alive=auto upgrades the connection to a
// kept-alive session once it succeeds: the server then stops closing after
// each command and instead reaps the connection when no bytes arrive within
// keepAliveTimeout.
func (s *connSession) serveOne(ctx context.Context, req Request, in io.Reader) bool {
	grant := req.Verb == VerbPROBE && s.keepAliveTimeout > 0 &&
		len(req.Params) == 1 && probeRequestsKeepAlive(req.Params[0])
	s.grantKeepAlive = s.keepAlive || grant
	s.noteClientActivity(req)
	if !s.serveCommand(ctx, req, in) {
		return false
	}
	if grant {
		s.keepAlive = true
	}
	return s.keepAlive
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
func (s *connSession) serveCommand(ctx context.Context, req Request, in io.Reader) bool {
	if s.events.Sink.Enabled() {
		return s.serveCommandWithEvents(ctx, req, in)
	}
	return s.serveCommandInner(ctx, req, in) == nil
}

// serveCommandWithEvents brackets one command with cmd_start and cmd_end.
// Byte counts come from the kernel's TCP counters, so they include buffered
// read-ahead, encryption overhead, and zero-copy payloads; they are zero on
// transports without TCP_INFO.
func (s *connSession) serveCommandWithEvents(ctx context.Context, req Request, in io.Reader) bool {
	sink, conn := s.events.Sink, s.events.Conn
	if isKeepAliveHeartbeat(req) {
		sink.Emit("heartbeat", "", events.F("conn", conn))
	}
	start := time.Now()
	read0, wrote0 := tcpByteCounts(s.conn)
	verb := req.Verb.String()
	sink.Emit("cmd_start", "", events.F("verb", verb), events.F("conn", conn))
	err := s.serveCommandInner(ctx, req, in)
	errStr := ""
	if err != nil && err != errConnDone {
		errStr = err.Error()
	}
	read1, wrote1 := tcpByteCounts(s.conn)
	sink.Emit("cmd_end", "",
		events.F("verb", verb), events.F("conn", conn),
		events.F("req_bytes", read1-read0),
		events.F("resp_bytes", wrote1-wrote0),
		events.Dur("dur", time.Since(start)), events.F("err", errStr))
	return err == nil
}

// errConnDone ends a connection after a response that could not be completed
// without a handler error to report.
var errConnDone = errors.New("connection done")

func (s *connSession) serveCommandInner(ctx context.Context, req Request, in io.Reader) error {
	if req.Verb == VerbSYNC && s.syncTimeout > 0 {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.syncTimeout))
		// The deadline covers the whole response, including the final
		// buffered records and END, but must not outlive this command: on a
		// kept-alive connection a stale write deadline would fail every
		// response written after it expires.
		defer func() { _ = s.conn.SetWriteDeadline(time.Time{}) }()
	}
	respOut, closeResp := s.newResponseWriter(req)
	// A lost response: the handler applies the request but its response
	// goes nowhere, and the connection resets instead of replying.
	lose := s.faults.LoseResponse()
	if lose {
		respOut, closeResp = io.Discard, func() error { return nil }
	}
	nr, _ := in.(*noiseRequest)
	err := s.handleCommand(ctx, req, in, respOut)
	if lose {
		// Whatever the handler did, the client sees only a reset.
		if fc, ok := s.conn.(*faults.Conn); ok {
			fc.Reset()
		} else {
			_ = s.conn.Close()
		}
		return errConnDone
	}
	if err != nil {
		// An authenticated request is answered only once it is known to
		// have ended; a request that cannot be read to its END closes the
		// connection without a response.
		if nr != nil && (errors.Is(err, errUnfinishedRequest) || !drainRequest(nr, s.requestDrainLimit())) {
			_ = s.noise.Close()
			return errConnDone
		}
		_ = writeErrFrame(respOut, err)
		_ = closeResp()
		return err
	}
	if nr != nil && !nr.completed {
		_ = s.noise.Close()
		return errUnfinishedRequest
	}
	if req.Verb == VerbTXFER || req.Verb == VerbSEND || req.Verb == VerbCXSUM || req.Verb == VerbPROBE || req.Verb == VerbSYNC {
		if err := writeOKLine(respOut, ""); err != nil {
			return errConnDone
		}
	}
	if err := closeResp(); err != nil {
		return errConnDone
	}
	return nil
}

func (s *connSession) handleCommand(ctx context.Context, req Request, in io.Reader, out io.Writer) error {
	// SEND, ACK, and CXSUM carry their per-file item lists in a framed request
	// body, so each needs the per-command reader alongside the response writer.
	if req.Verb == VerbSEND {
		return handleSENDWithOptions(ctx, req, in, out, s.deps, s.limiter, s.zeroCopyMinBytes, s.gentleBWPct)
	}
	if req.Verb == VerbACK {
		return handleACKWithInput(ctx, req, in, out, s.deps)
	}
	if req.Verb == VerbCXSUM {
		return handleCXSUMWithInput(ctx, req, in, out, s.deps)
	}
	if req.Verb == VerbPROBE {
		keepAliveMS := int64(0)
		if s.grantKeepAlive {
			keepAliveMS = s.keepAliveTimeout.Milliseconds()
		}
		return handlePROBEWithInput(ctx, req, in, out, s.deps, s.targetIODepth, s.gentleCPUPct, s.gentleBWPct, gentleLimiterBurstBytes(s.limiter), keepAliveMS, s.maxSyncBodyBytes)
	}
	if req.Verb == VerbSYNC {
		return handleSYNCWithInput(ctx, req, in, out, s.deps, s.onTransferCreated, s.maxSyncBodyBytes)
	}
	if req.Verb == VerbTXFER {
		return handleTXFERWithCallback(ctx, req, out, s.deps, s.onTransferCreated)
	}
	handler, ok := handlers[req.Verb]
	if !ok || req.Verb == VerbUnknown {
		return protocolErr{code: "BAD_COMMAND", message: "unknown command"}
	}
	return handler(ctx, req, out, s.deps)
}

func readCommandLine(r *bufio.Reader, maxBytes int) ([]byte, error) {
	line, err := utils.ReadLineLimit(r, maxBytes)
	if err != nil {
		if errors.Is(err, utils.ErrLineTooLarge) {
			return nil, protocolErr{code: "BAD_REQUEST", message: "command line too large"}
		}
		return nil, err
	}
	return parseCommandLine(line)
}

// parseCommandLine checks a raw line's terminator and strips it.
func parseCommandLine(line []byte) ([]byte, error) {
	if len(line) == 0 || line[len(line)-1] != '\n' {
		return nil, errors.New("invalid line terminator")
	}
	line = utils.TrimLineTerminator(line)
	if len(line) == 0 {
		return nil, errors.New("empty command")
	}
	return line, nil
}
