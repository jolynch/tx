package ftcp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"runtime/trace"
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
	closeResp              func() error
	wroteBytes             bool
	onTransferCreated      func(string)
}

func handleConn(conn net.Conn, opts ServerOptions, deps Deps, onTransferCreated func(string), onClientActivity func()) {
	defer conn.Close()
	if onClientActivity != nil {
		onClientActivity()
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
		closeResp:              func() error { return nil },
		onTransferCreated:      onTransferCreated,
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		if s.socketWriteBufferBytes > 0 {
			_ = tc.SetWriteBuffer(s.socketWriteBufferBytes)
		}
	}
	if err := s.run(); err != nil {
		_ = writeErrFrame(s.respOut, err)
		_ = s.closeResp()
	}
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
			return writeOKLine(s.respOut, string(aead.RecommendedCipher())+" "+s.serverID.Recipient().String())
		}
		if authRes.recipient != nil {
			encOut, encErr := aead.Encrypt(s.conn, authRes.recipient, aead.Options{Algorithm: authRes.responseCipher})
			if encErr != nil {
				return encErr
			}
			s.respOut = encOut
			s.closeResp = encOut.Close
		}
		if authRes.encryptedRequests {
			if s.serverID == nil {
				return protocolErr{code: "NOT_AUTHORIZED", message: "server auth key unavailable"}
			}
			decIn, decErr := aead.DecryptWithOptions(br, s.serverID, aead.Options{Algorithm: authRes.responseCipher})
			if decErr != nil {
				return protocolErr{code: "NOT_AUTHORIZED", message: "request decryption failed"}
			}
			cmdReader = bufio.NewReader(decIn)
		}

		cmdPayload, cmdErr := readCommandLine(cmdReader, maxCommandLineBytes)
		if cmdErr != nil {
			return cmdErr
		}
		cmdReq, err = ParseRequest(cmdPayload)
		if err != nil {
			return err
		}
	} else if s.requireAuth {
		return protocolErr{code: "NOT_AUTHORIZED", message: "missing AUTH"}
	}

	cmdCtx, connTask := trace.NewTask(context.Background(), "tcp-connection")
	defer connTask.End()
	countingOut := &countingWriter{w: s.respOut}
	if err := s.handleCommand(cmdCtx, cmdReq, cmdReader, countingOut); err != nil {
		s.wroteBytes = countingOut.n > 0
		return err
	}
	if cmdReq.Verb == VerbTXFER || cmdReq.Verb == VerbSEND || cmdReq.Verb == VerbCXSUM || cmdReq.Verb == VerbPROBE || cmdReq.Verb == VerbSYNC {
		if err := writeOKLine(countingOut, ""); err != nil {
			s.wroteBytes = countingOut.n > 0
			return err
		}
	}
	s.wroteBytes = countingOut.n > 0
	return s.closeResp()
}

func (s *connSession) handleCommand(ctx context.Context, req Request, in io.Reader, out io.Writer) error {
	if req.Verb == VerbSEND {
		return handleSENDWithOptions(ctx, req, out, s.deps, s.limiter, s.disableZeroCopy, s.gentleBWPct)
	}
	if req.Verb == VerbPROBE {
		return handlePROBEWithInput(ctx, req, in, out, s.deps, s.targetIODepth, s.gentleCPUPct, s.gentleBWPct, gentleLimiterBurstBytes(s.limiter))
	}
	if req.Verb == VerbSYNC {
		if s.syncTimeout > 0 {
			_ = s.conn.SetWriteDeadline(time.Now().Add(s.syncTimeout))
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

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
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
