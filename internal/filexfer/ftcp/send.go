package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/jolynch/tx/internal/utils"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

const defaultFileFrameLogicalSize int64 = 4 * 1024 * 1024

// defaultZeroCopyMinFrameBytes is the smallest frame sent through tee/splice.
// Each zero-copy frame pays for two pipes, a socket dup, and their closes, so
// below about 1 MiB the buffered path is faster (BenchmarkSendFramePayload).
const defaultZeroCopyMinFrameBytes int64 = 1 << 20

// zeroCopyOff, as a zeroCopyMinBytes value, sends every frame buffered.
const zeroCopyOff int64 = 0

// sendOutputBufferBytes sizes the buffer one SEND response writes its frames
// through, so a small file's header, payload, and trailer, and many small
// files together, share one write syscall.
const sendOutputBufferBytes = 64 << 10
const defaultCompressedFrameBufferBytes = 4 * 1024 * 1024
const defaultMaxLinuxPipeSizeBytes int64 = 1 * 1024 * 1024
const maxCompressedFrameBufferPoolBytes = 32 * 1024 * 1024
const loadStrategyFast = "fast"
const loadStrategyGentle = "gentle"

var compressedFrameBufferPool sync.Pool
var pipeMaxSizeOnce sync.Once
var pipeMaxSizeBytes int64 = defaultMaxLinuxPipeSizeBytes

type sendItem struct {
	FileID uint64
	Offset int64
	Size   int64
	Comp   string
	Path   string
	Mode   string
}

// sendRequestRecord omits transfer-wide mode and stores the codec as one byte.
type sendRequestRecord struct {
	FileID uint64
	Offset int64
	Size   int64
	Path   string
	Codec  uint8
}

var sendRequestCodecs = [...]string{"adapt", "none", encoding.EncodingLz4, encoding.EncodingZstd}

func parseSENDRecord(raw map[string]string) (sendRequestRecord, error) {
	item, err := parseSENDItem(raw, "")
	if err != nil {
		return sendRequestRecord{}, err
	}
	var codec uint8
	for i, name := range sendRequestCodecs {
		if name == item.Comp {
			codec = uint8(i)
			break
		}
	}
	return sendRequestRecord{FileID: item.FileID, Offset: item.Offset, Size: item.Size, Path: item.Path, Codec: codec}, nil
}

func (r sendRequestRecord) item(mode string) sendItem {
	return sendItem{FileID: r.FileID, Offset: r.Offset, Size: r.Size, Path: r.Path, Comp: sendRequestCodecs[r.Codec], Mode: mode}
}

type sendHeader struct {
	TransferID string
	Mode       string
}

type frameStreamArgs struct {
	Ctx           context.Context
	FileID        uint64
	Offset        int64
	FrameSize     int64
	Comp          string
	Mode          string
	MaxWSizeHint  *int64
	HeaderTS      int64
	Next          int64
	IsTerminal    bool
	TerminalMD    *encoding.FileFrameMetadata
	WindowHasher  *xxh3.Hasher128
	Output        io.Writer
	OutputTCPConn *net.TCPConn
	PipeSizeBytes int
	DirectIO      bool
	// Faults gates the frame's file reads and syscalls; nil disables it.
	Faults *faults.Injector
	// Timed reports whether anything reads the frame's latencies: window
	// events, or the adaptive policy choosing the next frame's codec. A
	// frame nothing reads skips its clock reads.
	Timed bool
}

type frameStreamStats struct {
	LogicalSize    int64
	WireSize       int64
	PrepareLatency time.Duration
	// ReadLatency is the part of PrepareLatency spent reading the file; the
	// rest is compression.
	ReadLatency     time.Duration
	WriteLatency    time.Duration
	NextOffset      int64
	WindowHashToken string
}

// parseSENDHeader reads the command line, which now carries only
// transfer-level fields. The per-file items arrive in the framed request body.
// Send paths reported in window events: the zero-copy tee+splice path is
// reported as sendfile.
const (
	sendPathSendfile = "sendfile"
	sendPathBuffered = "buffered"
)

func emitWindow(scope events.Scope, txferID string, fileID uint64, off int64, comp, sendPath string, stats frameStreamStats) {
	scope.Sink.Emit("window", txferID,
		events.F("conn", scope.Conn),
		events.F("file", fileID),
		events.F("off", off),
		events.F("len", stats.LogicalSize),
		events.F("wire", stats.WireSize),
		events.F("codec", comp),
		events.F("send_path", sendPath),
		events.Dur("read_dur", stats.ReadLatency),
		events.Dur("comp_dur", stats.PrepareLatency-stats.ReadLatency),
		events.Dur("write_dur", stats.WriteLatency))
}

func parseSENDHeader(req Request) (sendHeader, error) {
	if req.Verb != VerbSEND {
		return sendHeader{}, protocolErr{code: "BAD_COMMAND", message: "not SEND"}
	}
	if len(req.Params) != 1 {
		return sendHeader{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND arguments"}
	}
	p := req.Params[0]
	txferID := strings.TrimSpace(p["txferid"])
	if txferID == "" {
		return sendHeader{}, protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
	}
	mode := strings.ToLower(strings.TrimSpace(p["mode"]))
	if mode == "" {
		mode = loadStrategyFast
	}
	switch mode {
	case loadStrategyFast, loadStrategyGentle:
	default:
		return sendHeader{}, protocolErr{code: "BAD_REQUEST", message: "unsupported SEND mode"}
	}
	return sendHeader{TransferID: txferID, Mode: mode}, nil
}

// parseSENDItem turns one framed body item into a sendItem. mode comes from the
// command line: it is a transfer-level property, so it is sent once rather than
// repeated on every file.
func parseSENDItem(p map[string]string, mode string) (sendItem, error) {
	fid, err := strconv.ParseUint(p["fid"], 10, 64)
	if err != nil {
		return sendItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND file id"}
	}
	offset := int64(0)
	if raw := strings.TrimSpace(p["offset"]); raw != "" {
		offset, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || offset < 0 {
			return sendItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND offset"}
		}
	}
	size := int64(0)
	if raw := strings.TrimSpace(p["size"]); raw != "" {
		size, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || size < 0 {
			return sendItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND size"}
		}
	}
	comp := strings.ToLower(strings.TrimSpace(p["comp"]))
	if comp == "" {
		comp = "adapt"
	}
	if comp == encoding.EncodingIdentity {
		comp = "none"
	}
	switch comp {
	case "adapt", "none", encoding.EncodingLz4, encoding.EncodingZstd:
	default:
		return sendItem{}, protocolErr{code: "UNSUPPORTED_COMP", message: "supported comp values: adapt, none, lz4, zstd"}
	}
	path := p["path"]
	if path == "" {
		return sendItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid SEND path"}
	}
	return sendItem{FileID: fid, Offset: offset, Size: size, Comp: comp, Path: path, Mode: mode}, nil
}

func handleSEND(ctx context.Context, req Request, out io.Writer, deps Deps) error {
	return protocolErr{code: "INTERNAL", message: "SEND requires a request body, use handleSENDWithOptions"}
}

// handleSENDWithOptions reads the whole item list, then streams one window per
// item. Reading first is required: responses are file data, and emitting any of
// it before the request body is drained would deadlock against a client that is
// still writing that body.
func handleSENDWithOptions(ctx context.Context, req Request, in io.Reader, out io.Writer, deps Deps, limiter *limit.Limiter, zeroCopyMinBytes int64, gentleBWPct int) error {
	header, err := parseSENDHeader(req)
	if err != nil {
		// Drain before replying so a client writing its body receives this error.
		drainItemBody(in)
		return err
	}
	gentleBWPct = limit.NormalizeGentleBWPct(gentleBWPct)

	records, err := parseRequestItemRecords(in, sendItemKeys, "SEND", parseSENDRecord)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return protocolErr{code: "BAD_REQUEST", message: "SEND requires at least one item"}
	}

	// Store calls take the transfer lock, which every connection of the
	// transfer shares, so take it once per request rather than once per file:
	// resolve every item and mark the resolved ones Running up front, then
	// record window hashes, and directories as Done, in one call each before
	// the OK or ERR line. The client ACKs only after reading the whole
	// response, so no ACK can arrive before its hash is recorded. A lookup
	// error is reported when its item's turn comes.
	lookups := make([]FileLookup, len(records))
	for i, record := range records {
		lookups[i] = FileLookup{FileID: record.FileID, Path: record.Path}
	}
	refs, refErrs := deps.GetFileRefs(header.TransferID, lookups)
	running := make([]uint64, 0, len(records))
	for i, record := range records {
		if refErrs[i] == nil {
			running = append(running, record.FileID)
		}
	}
	_ = deps.SetTransferFilesState(header.TransferID, running, TransferStateRunning)
	hashes := make([]WindowHash, 0, len(records))
	var done []uint64
	recordHashes := func() error {
		if len(done) > 0 {
			_ = deps.SetTransferFilesState(header.TransferID, done, TransferStateDone)
		}
		if len(hashes) > 0 && !deps.SetTransferFileWindowHashes(header.TransferID, hashes) {
			return protocolErr{code: "INTERNAL", message: "failed to store window hash state"}
		}
		return nil
	}

	// Fast requests write through one buffer, which must reach the client
	// before the ERR line the caller writes, and before its OK. Gentle
	// requests stay unbuffered, so their rate limiters pace the socket writes
	// themselves rather than writes into a buffer. A transport that batches
	// writes, an authenticated session, already buffers into whole records.
	inj := faults.FromContext(ctx)
	resp := unbufferedSendOutput(out)
	if header.Mode != loadStrategyGentle && transportFlusher(out) == nil {
		resp = newBufferedSendOutput(out)
		defer resp.release()
	}
	resp.faults = inj
	transfer, hasTransfer := deps.GetTransferSummary(header.TransferID)
	for i, record := range records {
		item := record.item(header.Mode)
		itemOut := resp
		if item.Mode == loadStrategyGentle {
			if hasTransfer && transfer.DeadlineMS > 0 {
				if err := checkTransferDeadline(deps, header.TransferID, transfer); err != nil {
					_ = recordHashes()
					return err
				}
			}
			limited := out
			gentleLimiter := deps.GetTransferGentleLimiter(header.TransferID, transfer.LinkMbps, gentleBWPct, gentleLimiterBurstBytes(limiter))
			if gentleLimiter != nil {
				limited = gentleLimiter.WrapRateLimitedWriter(limited, ctx)
			}
			if limiter != nil {
				limited = limiter.WrapRateLimitedWriter(limited, ctx)
			}
			itemOut = unbufferedSendOutput(limited)
			itemOut.frameFlush = transportFlusher(out)
			itemOut.faults = inj
		}
		var sent sentItem
		err := mapLookupError(refErrs[i])
		if err == nil {
			sent, err = streamSendItem(ctx, itemOut, deps, header.TransferID, item, refs[i], zeroCopyMinBytes)
		}
		if sent.hash.HashToken != "" {
			hashes = append(hashes, sent.hash)
		}
		if sent.done {
			done = append(done, item.FileID)
		}
		if err != nil {
			_ = recordHashes()
			_ = resp.Flush()
			return err
		}
	}
	if err := recordHashes(); err != nil {
		_ = resp.Flush()
		return err
	}
	return resp.Flush()
}

// sendOutput is where one SEND item writes its frames. Frames go to w, which
// is a buffer shared by the whole response or, unbuffered, the response
// writer itself. A zero-copy frame splices its payload straight to conn, so
// it flushes the buffer first and writes its header and trailer to conn too;
// nothing can then reach the socket ahead of bytes buffered before it.
type sendOutput struct {
	w    io.Writer
	buf  *bufio.Writer // nil when unbuffered
	conn *net.TCPConn  // the response as a raw socket, or nil (AEAD, limiters)
	// frameFlush, when set, sends a completed frame through a transport that
	// batches writes; gentle responses use it so frames keep their pacing.
	frameFlush func() error
	// hasher is reset and reused by each item, rather than allocating 1.2 KiB
	// per file.
	hasher xxh3.Hasher128
	// faults gates the request's file and syscall sites; nil disables it.
	faults *faults.Injector
}

var sendOutputBufferPool = sync.Pool{
	New: func() any { return bufio.NewWriterSize(nil, sendOutputBufferBytes) },
}

// newBufferedSendOutput takes a pooled buffer; release returns it.
func newBufferedSendOutput(out io.Writer) *sendOutput {
	buf := sendOutputBufferPool.Get().(*bufio.Writer)
	buf.Reset(out)
	return &sendOutput{w: buf, buf: buf, conn: extractTCPConn(out)}
}

func unbufferedSendOutput(out io.Writer) *sendOutput {
	return &sendOutput{w: out, conn: extractTCPConn(out)}
}

// Flush writes any buffered frames to the response.
func (o *sendOutput) Flush() error {
	if o.buf == nil {
		return nil
	}
	return o.buf.Flush()
}

// release returns a buffered output's buffer to the pool, discarding
// anything not flushed. The output must not be used afterwards.
func (o *sendOutput) release() {
	if o.buf == nil {
		return
	}
	o.buf.Reset(nil)
	sendOutputBufferPool.Put(o.buf)
	o.w, o.buf = nil, nil
}

const deadlineGracePeriod = 30 * time.Second

func checkTransferDeadline(deps Deps, txferID string, transfer Transfer) error {
	if transfer.TooSlow {
		return protocolErr{code: "TOO_SLOW", message: "transfer deadline exceeded"}
	}
	firstSend, ok := deps.RecordTransferFirstSend(txferID)
	if !ok || firstSend.IsZero() {
		return nil
	}
	elapsed := time.Since(firstSend)
	deadline := time.Duration(transfer.DeadlineMS) * time.Millisecond
	if elapsed > deadline {
		deps.MarkTransferTooSlow(txferID)
		return protocolErr{code: "TOO_SLOW", message: "transfer deadline exceeded"}
	}
	if elapsed > deadlineGracePeriod && transfer.DoneSize > 0 {
		rate := float64(transfer.DoneSize) / elapsed.Seconds()
		remaining := float64(transfer.TotalSize - transfer.DoneSize)
		timeLeft := (deadline - elapsed).Seconds()
		if rate > 0 && timeLeft > 0 && remaining/rate > timeLeft {
			deps.MarkTransferTooSlow(txferID)
			return protocolErr{code: "TOO_SLOW", message: "transfer will not finish within deadline"}
		}
	}
	return nil
}

// sentItem is what a sent item leaves for its SEND to record: the window
// hash, and for a directory, which is complete once sent, that it is Done.
type sentItem struct {
	hash WindowHash
	done bool
}

// streamSendItem sends one item as frames. Frames of at least zeroCopyMinBytes
// use the zero-copy path when the output allows it; zeroCopyOff disables it.
func streamSendItem(ctx context.Context, out *sendOutput, deps Deps, txferID string, item sendItem, fileRef FileRef, zeroCopyMinBytes int64) (sentItem, error) {
	ctx, windowTask := utils.NewTraceTask(ctx, "send-window")
	defer windowTask.End()
	scope := events.FromContext(ctx)
	var itemStart time.Time
	if scope.Sink.Enabled() {
		itemStart = time.Now()
	}
	inj := out.faults
	fd, usedDirectOpen, err := openSendFile(inj, deps, item, fileRef)
	if err != nil {
		return sentItem{}, mapLookupError(err)
	}
	if scope.Sink.Enabled() {
		scope.Sink.Emit("file_open", txferID,
			events.F("conn", scope.Conn), events.F("file", item.FileID), events.F("path", item.Path),
			events.F("len", fileRef.FileSize), events.Dur("dur", time.Since(itemStart)))
	}
	defer func() {
		if fd != nil {
			_ = fd.Close()
		}
	}()

	if err := inj.Sys(faults.SysFstat); err != nil {
		return sentItem{}, protocolErr{code: "INTERNAL", message: "failed to stat file"}
	}
	fileInfo, err := fd.Stat()
	if err != nil {
		return sentItem{}, protocolErr{code: "INTERNAL", message: "failed to stat file"}
	}
	entryType := fileRef.EntryType
	if entryType == 0 {
		entryType = encoding.EntryTypeFile
	}
	if fileInfo.IsDir() {
		if entryType != encoding.EntryTypeDir {
			return sentItem{}, protocolErr{code: "BAD_REQUEST", message: "SEND path is a directory but manifest entry is not"}
		}
		if item.Offset != 0 || item.Size != 0 {
			return sentItem{}, protocolErr{code: "BAD_REQUEST", message: "directory SEND must request offset=0 size=0"}
		}
		hash, err := streamSendMetadataOnly(ctx, out, item, fileRef, fileInfo)
		return sentItem{hash: hash, done: err == nil}, err
	}
	if entryType != encoding.EntryTypeFile {
		return sentItem{}, protocolErr{code: "BAD_REQUEST", message: "SEND metadata-only supports directories only for non-file entries"}
	}
	windowLen := fileInfo.Size()
	if windowLen < 0 {
		return sentItem{}, protocolErr{code: "INTERNAL", message: "invalid file size"}
	}
	if item.Offset > windowLen {
		return sentItem{}, protocolErr{code: "RANGE", message: "offset out of range"}
	}
	windowLen -= item.Offset
	if item.Size > 0 && item.Size < windowLen {
		windowLen = item.Size
	}
	// Never stream past the size registered at TXFER/SYNC, which is also
	// where ACK clamps. A file that grew since then would otherwise send
	// bytes the client did not plan for (an empty file's SEND carries no
	// size at all) and fail the client's whole batch.
	if fileRef.FileSize >= 0 {
		windowLen = min(windowLen, max(fileRef.FileSize-item.Offset, 0))
	}
	// In fast mode, advise sequential access for the whole window then spawn
	// a background goroutine that issues readahead(2) for the next frame
	// while we read and process the current one. Only useful when there are
	// at least two frames worth of data.
	var readaheadCh chan readaheadHint
	if item.Mode == loadStrategyFast {
		tryAdviseSequential(inj, fd, item.Offset, windowLen)
		if runtime.GOOS == "linux" && windowLen > defaultFileFrameLogicalSize {
			done := make(chan struct{})
			var stopReadahead func()
			readaheadCh, stopReadahead = startFrameReadahead(inj, fd, done)
			defer func() { close(done); stopReadahead() }()
		}
	}

	cursor := item.Offset

	firstFrameLogical := min(windowLen, defaultFileFrameLogicalSize)
	maxWSizeHint, err := maxFrameWireHint(item.Comp, firstFrameLogical)
	if err != nil {
		return sentItem{}, protocolErr{code: "INTERNAL", message: "failed to compute max frame size hint"}
	}
	pipeSizeBytes := desiredPipeSizeBytes(windowLen, firstFrameLogical)
	firstFrame := true

	adaptive := item.Comp == "adapt"
	currentMode := initialCompressionMode(item.Comp)
	compressPolicy := policy.NewCompressionPolicy()
	windowHasher := &out.hasher
	windowHasher.Reset()

	// Zero-byte files: emit one terminal frame with no payload so the client
	// always receives a frame stream rather than a bare OK response.
	if windowLen == 0 {
		md := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
		zeroArgs := frameStreamArgs{
			Ctx:          ctx,
			FileID:       item.FileID,
			Offset:       0,
			FrameSize:    0,
			Comp:         "none",
			Mode:         item.Mode,
			HeaderTS:     time.Now().UnixMilli(),
			Next:         0,
			IsTerminal:   true,
			TerminalMD:   &md,
			WindowHasher: windowHasher,
			Output:       out.w,
		}
		if err := writeFrameHeader(out.w, zeroArgs, 0, nil); err != nil {
			return sentItem{}, err
		}
		windowHashToken, err := writeFrameTrailer(out.w, zeroArgs, nil)
		if err != nil {
			return sentItem{}, err
		}
		recorded := WindowHash{FileID: item.FileID, EndBytes: 0, HashToken: windowHashToken}
		if scope.Sink.Enabled() {
			scope.Sink.Emit("file_done", txferID, events.F("conn", scope.Conn), events.F("file", item.FileID), events.Dur("dur", time.Since(itemStart)))
		}
		// The file stays Running until its ACK, which is where every
		// regular file, empty or not, is marked Done and counted.
		return sentItem{hash: recorded}, nil
	}

	var recorded WindowHash
	for remaining := windowLen; remaining > 0; {
		frameSize := min(remaining, defaultFileFrameLogicalSize)
		nextOffset := cursor + frameSize
		isTerminal := nextOffset == item.Offset+windowLen
		nextValue := nextOffset
		if isTerminal {
			nextValue = 0
		}
		var maxHint *int64
		if firstFrame && maxWSizeHint > 0 {
			maxHint = &maxWSizeHint
		}

		frameComp := policy.FrameCompTokenForMode(currentMode)
		var terminalMD *encoding.FileFrameMetadata
		if isTerminal {
			md := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
			terminalMD = &md
		}

		frameArgs := frameStreamArgs{
			Ctx:           ctx,
			FileID:        item.FileID,
			Offset:        cursor,
			FrameSize:     frameSize,
			Comp:          frameComp,
			Mode:          item.Mode,
			MaxWSizeHint:  maxHint,
			HeaderTS:      time.Now().UnixMilli(),
			Next:          nextValue,
			IsTerminal:    isTerminal,
			TerminalMD:    terminalMD,
			WindowHasher:  windowHasher,
			Output:        out.w,
			OutputTCPConn: out.conn,
			PipeSizeBytes: pipeSizeBytes,
			DirectIO:      usedDirectOpen,
			Faults:        inj,
			// The policy is per item, so a terminal frame's latencies would
			// only choose a codec for a frame that never comes.
			Timed: scope.Sink.Enabled() || (adaptive && !isTerminal),
		}

		if readaheadCh != nil && !isTerminal {
			nextFrameSize := min(remaining-frameSize, defaultFileFrameLogicalSize)
			if nextFrameSize > 0 {
				sendReadaheadHint(readaheadCh, readaheadHint{
					offset: nextOffset,
					length: nextFrameSize,
				})
			}
		}

		frameOffset := cursor
		var stats frameStreamStats
		sendPath := sendPathBuffered
		if zeroCopyMinBytes > 0 && frameSize >= zeroCopyMinBytes && canZeroCopy(frameArgs) {
			// The splice bypasses out.w, so flush it and send this frame's
			// header and trailer to the socket as well, in order.
			if err := out.Flush(); err != nil {
				return sentItem{}, err
			}
			frameArgs.Output = out.conn
			sendPath = sendPathSendfile
			stats, err = streamFramePayloadZeroCopy(fd, &frameOffset, frameArgs)
		} else {
			stats, err = streamFramePayloadBuffered(fd, &frameOffset, frameArgs)
		}
		if err != nil {
			if usedDirectOpen && isDirectIOReadError(err) {
				if frameOffset != cursor {
					_ = fd.Close()
					return sentItem{}, err
				}
				_ = fd.Close()
				fd = nil
				// Reopen without direct I/O, through the same open gate.
				fd, _, err = openSendFile(inj, deps, sendItem{Mode: loadStrategyFast}, fileRef)
				if err != nil {
					return sentItem{}, mapLookupError(err)
				}
				usedDirectOpen = false
				remaining = item.Offset + windowLen - cursor
				if remaining < 0 {
					return sentItem{}, protocolErr{code: "INTERNAL", message: "invalid resume offset after direct I/O retry"}
				}
				if cursor == item.Offset {
					if item.Mode == loadStrategyFast {
						tryAdviseSequential(inj, fd, item.Offset, windowLen)
					}
					firstFrame = true
					currentMode = initialCompressionMode(item.Comp)
					compressPolicy = policy.NewCompressionPolicy()
					windowHasher.Reset()
				} else {
					if item.Mode == loadStrategyFast {
						tryAdviseSequential(inj, fd, cursor, remaining)
					}
				}
				continue
			}
			return sentItem{}, err
		}

		if scope.Sink.Enabled() {
			emitWindow(scope, txferID, item.FileID, cursor, frameComp, sendPath, stats)
		}
		cursor = stats.NextOffset
		remaining -= frameSize
		firstFrame = false
		if out.frameFlush != nil {
			if err := out.frameFlush(); err != nil {
				return sentItem{}, err
			}
		}
		if isTerminal && scope.Sink.Enabled() && cursor >= fileRef.FileSize {
			scope.Sink.Emit("file_done", txferID, events.F("conn", scope.Conn), events.F("file", item.FileID), events.Dur("dur", time.Since(itemStart)))
		}

		if isTerminal {
			if stats.WindowHashToken == "" {
				return sentItem{}, protocolErr{code: "INTERNAL", message: "failed to finalize window hash"}
			}
			recorded = WindowHash{FileID: item.FileID, EndBytes: cursor, HashToken: stats.WindowHashToken}
		}

		if adaptive && !isTerminal {
			// WriteLatency times writes into the response buffer, much as
			// unbuffered writes timed the kernel socket buffer, not the wire.
			// When the buffer fills, the flush is charged to the frame that
			// forced it, which is the backpressure signal the policy wants.
			decision := compressPolicy.Decide(currentMode, policy.CompressionMetrics{
				LogicalSize:    stats.LogicalSize,
				WireSize:       stats.WireSize,
				PrepareLatency: stats.PrepareLatency,
				WriteLatency:   stats.WriteLatency,
			})
			if decision.Next != currentMode {
				prevComp := policy.FrameCompTokenForMode(currentMode)
				nextComp := policy.FrameCompTokenForMode(decision.Next)
				log.Printf(
					"txfer-frame: tid=%s fid=%d switching compression %s->%s reason=%s ratio=%.3f read_over_write=%.3f",
					txferID,
					item.FileID,
					prevComp,
					nextComp,
					decision.Reason,
					decision.Ratio,
					decision.ReadOverWrite,
				)
				currentMode = decision.Next
			}
		}
	}

	return sentItem{hash: recorded}, nil
}

// openSendFile opens an item's resolved file, with direct I/O in gentle mode
// when the filesystem allows it, and reports whether it did. The open fault
// gate comes first, so an injected open error stops either kind of open.
func openSendFile(inj *faults.Injector, deps Deps, item sendItem, fileRef FileRef) (*os.File, bool, error) {
	if err := inj.OpenErr(faults.SiteSendOpen); err != nil {
		return nil, false, err
	}
	if item.Mode == loadStrategyGentle {
		if fd, err := os.OpenFile(fileRef.Path, os.O_RDONLY|unix.O_DIRECT, 0); err == nil {
			return fd, true, nil
		}
	}
	fd, err := deps.OpenFileRef(fileRef)
	return fd, false, err
}

func isDirectIOReadError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, unix.EINVAL)
}

// canZeroCopy reports whether a frame can use the tee+splice zero-copy send
// path, which avoids copying payload bytes into userspace for the network
// write. The path requires all of:
//   - Linux (tee/splice are Linux-only syscalls)
//   - No compression (compressed data must be assembled in userspace)
//   - No O_DIRECT (direct I/O requires aligned userspace buffers, not pipes)
//   - A raw TCP socket (no encryption or rate-limit wrapper over the conn)
func canZeroCopy(args frameStreamArgs) bool {
	return runtime.GOOS == "linux" &&
		args.Comp == "none" &&
		!args.DirectIO &&
		args.OutputTCPConn != nil
}

// extractTCPConn returns the raw socket a SEND response may splice into, or
// nil. The exact-type check comes first, so a raw socket pays nothing for the
// fault-injection case after it; other writers pay one more type check per
// SEND output.
func extractTCPConn(w io.Writer) *net.TCPConn {
	if tc, ok := w.(*net.TCPConn); ok {
		return tc
	}
	if zc, ok := w.(zeroCopyConn); ok {
		return zc.ZeroCopyConn()
	}
	return nil
}

// zeroCopyConn is a wrapped connection, such as a faults.Conn, that may still
// let a SEND splice into its socket.
type zeroCopyConn interface {
	ZeroCopyConn() *net.TCPConn
}

// rawTCPConn returns the TCP socket under conn, looking through one
// fault-injection wrapper, or nil for other transports. Socket options,
// half-close, and TCP_INFO go to the socket itself, whatever the wrapper's
// fault schedule; data never does. extractTCPConn, by contrast, asks the
// wrapper whether a SEND may splice past it.
func rawTCPConn(conn net.Conn) *net.TCPConn {
	if tc, ok := conn.(*net.TCPConn); ok {
		return tc
	}
	if fc, ok := conn.(*faults.Conn); ok {
		tc, _ := fc.Unwrap().(*net.TCPConn)
		return tc
	}
	return nil
}

func initialCompressionMode(comp string) policy.CompressionMode {
	switch comp {
	case encoding.EncodingLz4:
		return policy.CompressionModeLz4
	case encoding.EncodingZstd:
		return policy.CompressionModeZstdLevel1
	default:
		return policy.CompressionModeNone
	}
}

func streamSendMetadataOnly(ctx context.Context, out *sendOutput, item sendItem, fileRef FileRef, fileInfo os.FileInfo) (WindowHash, error) {
	md := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
	md.Size = 0
	windowHasher := &out.hasher
	windowHasher.Reset()
	args := frameStreamArgs{
		Ctx:          ctx,
		FileID:       item.FileID,
		Offset:       0,
		FrameSize:    0,
		Comp:         "none",
		Mode:         item.Mode,
		HeaderTS:     time.Now().UnixMilli(),
		Next:         0,
		IsTerminal:   true,
		TerminalMD:   &md,
		WindowHasher: windowHasher,
		Output:       out.w,
	}
	if err := writeFrameHeader(out.w, args, 0, nil); err != nil {
		return WindowHash{}, err
	}
	windowHashToken, err := writeFrameTrailer(out.w, args, nil)
	if err != nil {
		return WindowHash{}, err
	}
	return WindowHash{FileID: item.FileID, EndBytes: 0, HashToken: windowHashToken}, nil
}

func maxFrameWireHint(comp string, logicalSize int64) (int64, error) {
	if logicalSize <= 0 {
		return 0, nil
	}
	switch comp {
	case "adapt":
		maxHint := int64(0)
		for _, candidate := range []string{"none", encoding.EncodingLz4, encoding.EncodingZstd} {
			hint, err := encoding.MaxFrameWireSizeHintBytes(candidate, logicalSize)
			if err != nil {
				return 0, err
			}
			if hint > maxHint {
				maxHint = hint
			}
		}
		return maxHint, nil
	default:
		return encoding.MaxFrameWireSizeHintBytes(comp, logicalSize)
	}
}

func desiredPipeSizeBytes(windowLen int64, frameSize int64) int {
	maxPipeSize := bestEffortPipeMaxSizeBytes()
	const minPipeSize = 64 * 1024
	if frameSize <= 0 {
		return minPipeSize
	}
	if windowLen > maxPipeSize {
		return int(maxPipeSize)
	}
	return int(max(minPipeSize, min(frameSize, maxPipeSize)))
}

func bestEffortPipeMaxSizeBytes() int64 {
	pipeMaxSizeOnce.Do(func() {
		raw, err := os.ReadFile("/proc/sys/fs/pipe-max-size")
		if err != nil {
			return
		}
		value, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil || value <= 0 {
			return
		}
		pipeMaxSizeBytes = value
	})
	return pipeMaxSizeBytes
}

func growPipeBestEffort(inj *faults.Injector, pipeFD *os.File, sizeBytes int) {
	if sizeBytes <= 0 || inj.Sys(faults.SysFcntl) != nil {
		return
	}
	_, _ = unix.FcntlInt(pipeFD.Fd(), unix.F_SETPIPE_SZ, sizeBytes)
}

// frameLineBytes is the pooled buffer a frame header or trailer line is
// formatted into. Lines are usually under 300 bytes; a longer one grows the
// slice rather than failing.
const frameLineBytes = 4 << 10

func appendFrameHeaderLine(dst []byte, fileID uint64, offset int64, size int64, wireSize int64, comp string, maxWSizeHint *int64, ts int64) []byte {
	dst = append(dst, "FX/1 "...)
	dst = strconv.AppendUint(dst, fileID, 10)
	dst = append(dst, " offset="...)
	dst = strconv.AppendInt(dst, offset, 10)
	dst = append(dst, " size="...)
	dst = strconv.AppendInt(dst, size, 10)
	dst = append(dst, " wsize="...)
	dst = strconv.AppendInt(dst, wireSize, 10)
	dst = append(dst, " comp="...)
	dst = append(dst, comp...)
	if maxWSizeHint != nil {
		dst = append(dst, " max-wsize="...)
		dst = strconv.AppendInt(dst, *maxWSizeHint, 10)
	}
	dst = append(dst, " ts="...)
	dst = strconv.AppendInt(dst, ts, 10)
	return append(dst, '\n')
}

func appendFrameTrailerLine(dst []byte, fileID uint64, ts int64, next int64, windowHashToken string, metadata *encoding.FileFrameMetadata) []byte {
	dst = append(dst, "FXT/1 "...)
	dst = strconv.AppendUint(dst, fileID, 10)
	dst = append(dst, " status=ok ts="...)
	dst = strconv.AppendInt(dst, ts, 10)
	if windowHashToken != "" {
		dst = append(dst, " file-hash="...)
		dst = append(dst, windowHashToken...)
	}
	dst = append(dst, " next="...)
	dst = strconv.AppendInt(dst, next, 10)
	if metadata != nil {
		dst = appendMetadataTrailerTokens(dst, metadata)
	}
	return append(dst, '\n')
}

// appendMetadataTrailerTokens appends the space-prefixed meta: tokens of a
// terminal trailer. Spaces in user and group names become underscores, since
// tokens are space-separated.
func appendMetadataTrailerTokens(dst []byte, metadata *encoding.FileFrameMetadata) []byte {
	dst = append(dst, " meta:size="...)
	dst = strconv.AppendInt(dst, metadata.Size, 10)
	dst = append(dst, " meta:mtime_ns="...)
	dst = strconv.AppendInt(dst, metadata.MtimeNS, 10)
	dst = append(dst, " meta:mode="...)
	dst = append(dst, metadata.Mode...)
	dst = append(dst, " meta:uid="...)
	dst = append(dst, metadata.UID...)
	dst = append(dst, " meta:gid="...)
	dst = append(dst, metadata.GID...)
	dst = append(dst, " meta:user="...)
	dst = appendSpacesAsUnderscores(dst, metadata.User)
	dst = append(dst, " meta:group="...)
	return appendSpacesAsUnderscores(dst, metadata.Group)
}

func appendSpacesAsUnderscores(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			dst = append(dst, '_')
		} else {
			dst = append(dst, s[i])
		}
	}
	return dst
}

func streamFramePayloadBuffered(fd *os.File, fileOffset *int64, args frameStreamArgs) (frameStreamStats, error) {
	readBufPtr, err := bufpool.Get(bufpool.BucketSize(args.FrameSize))
	if err != nil {
		return frameStreamStats{}, err
	}
	defer bufpool.Put(readBufPtr)
	readBuf := *readBufPtr

	writeLatency := time.Duration(0)
	var timedWrites *time.Duration
	if args.Timed {
		timedWrites = &writeLatency
	}
	isCompressed := args.Comp != "none"

	// For compressed: collect into frameBuf so wireSize is known before writing the header.
	// For none: normally stream directly, but when using direct I/O retry path we stage
	// payload in-memory until the read succeeds to avoid writing partial frame headers.
	payloadWriter := io.Writer(args.Output)
	var frameBuf *bytes.Buffer
	var closeCompWriter func() error
	var stagingBuf *bytes.Buffer
	stagedDirectWrite := args.DirectIO && !isCompressed

	if isCompressed {
		frameBuf = acquireCompressedFrameBuffer(args.FrameSize, args.Comp)
		defer releaseCompressedFrameBuffer(frameBuf)
		var compWriter io.Writer
		var selected string
		compWriter, closeCompWriter, selected, err = encoding.WrapCompressedWriter(frameBuf, args.Comp, args.Mode)
		if err != nil {
			return frameStreamStats{}, err
		}
		if selected != args.Comp {
			return frameStreamStats{}, errors.New("compression mode negotiation mismatch")
		}
		payloadWriter = compWriter
	} else {
		if stagedDirectWrite {
			staging, releaseStaging, stageErr := bufpool.Acquire(bufpool.BucketSize(args.FrameSize))
			if stageErr != nil {
				return frameStreamStats{}, stageErr
			}
			defer releaseStaging()
			stagingBuf = bytes.NewBuffer(staging[:0])
			payloadWriter = stagingBuf
		} else {
			if err := writeFrameHeader(args.Output, args, args.FrameSize, timedWrites); err != nil {
				return frameStreamStats{}, err
			}
		}
	}

	timing := timeNothing
	if args.Timed {
		timing = timeReads
		if isCompressed {
			timing = timeReadsAndHandling
		}
	}
	timeChunkWrites := args.Timed && !isCompressed && !stagedDirectWrite
	readRegion := trace.StartRegion(args.Ctx, "frame-read")
	var src io.ReaderAt = fd
	if args.Faults != nil {
		src = args.Faults.WrapReaderAt(args.Ctx, faults.SiteSendRead, fd)
	}
	prepareLatency, readLatency, err := streamBufferedRead(src, fileOffset, args.FrameSize, readBuf, timing, func(chunk []byte) error {
		_, _ = args.WindowHasher.Write(chunk)
		var writeStart time.Time
		if timeChunkWrites {
			writeStart = time.Now()
		}
		written, writeErr := payloadWriter.Write(chunk)
		if timeChunkWrites {
			writeLatency += time.Since(writeStart)
		}
		if writeErr != nil {
			return writeErr
		}
		if written != len(chunk) {
			return io.ErrShortWrite
		}
		return nil
	})
	readRegion.End()
	if err != nil {
		return frameStreamStats{}, err
	}

	wireSize := args.FrameSize
	if isCompressed {
		compRegion := trace.StartRegion(args.Ctx, "frame-compress")
		if err := closeCompWriter(); err != nil {
			compRegion.End()
			return frameStreamStats{}, err
		}
		wireSize = int64(frameBuf.Len())
		if err := writeFrameHeader(args.Output, args, wireSize, timedWrites); err != nil {
			compRegion.End()
			return frameStreamStats{}, err
		}
		if err := timedWrite(args.Output, frameBuf.Bytes(), timedWrites); err != nil {
			compRegion.End()
			return frameStreamStats{}, err
		}
		compRegion.End()
	} else if stagedDirectWrite {
		if err := writeFrameHeader(args.Output, args, wireSize, timedWrites); err != nil {
			return frameStreamStats{}, err
		}
		if err := timedWrite(args.Output, stagingBuf.Bytes(), timedWrites); err != nil {
			return frameStreamStats{}, err
		}
	}

	windowHashToken, err := writeFrameTrailer(args.Output, args, timedWrites)
	if err != nil {
		return frameStreamStats{}, err
	}

	return frameStreamStats{
		LogicalSize:     args.FrameSize,
		WireSize:        wireSize,
		PrepareLatency:  prepareLatency,
		ReadLatency:     readLatency,
		WriteLatency:    writeLatency,
		NextOffset:      *fileOffset,
		WindowHashToken: windowHashToken,
	}, nil
}

func dupConnFD(conn *net.TCPConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, err
	}
	dupFD := -1
	var dupErr error
	if err := raw.Control(func(fd uintptr) {
		dupFD, dupErr = unix.Dup(int(fd))
	}); err != nil {
		return -1, err
	}
	if dupErr != nil {
		return -1, dupErr
	}
	if dupFD < 0 {
		return -1, errors.New("failed to duplicate connection fd")
	}
	return dupFD, nil
}

const zeroCopyPollTimeoutMs = 30_000 // 30s per poll wait

// zeroCopySyscalls contains the raw operations used by the zero-copy loop.
// Keeping this per invocation lets tests reproduce short and interrupted
// operations without mutable package-level syscall hooks.
type zeroCopySyscalls struct {
	splice func(int, *int64, int, *int64, int, int) (int64, error)
	tee    func(int, int, int, int) (int64, error)
	poll   func([]unix.PollFd, int) (int, error)
}

func kernelZeroCopySyscalls() zeroCopySyscalls {
	return zeroCopySyscalls{
		splice: unix.Splice,
		tee:    unix.Tee,
		poll:   unix.Poll,
	}
}

func waitSocketWritable(fd int, syscalls zeroCopySyscalls) error {
	pollFDs := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
	for {
		n, err := syscalls.poll(pollFDs, zeroCopyPollTimeoutMs)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return errors.New("zero-copy splice timed out waiting for socket writable")
		}
		if pollFDs[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return errors.New("socket error during zero-copy splice")
		}
		if pollFDs[0].Revents&unix.POLLOUT != 0 {
			return nil
		}
		return errors.New("socket did not become writable during zero-copy splice")
	}
}

func streamFramePayloadZeroCopy(fd *os.File, fileOffset *int64, args frameStreamArgs) (frameStreamStats, error) {
	syscalls := kernelZeroCopySyscalls()
	if args.Faults != nil {
		syscalls = faultZeroCopySyscalls(args.Faults, syscalls)
	}
	return streamFramePayloadZeroCopyWithSyscalls(fd, fileOffset, args, syscalls)
}

// faultZeroCopySyscalls puts inj's gates in front of each zero-copy syscall.
func faultZeroCopySyscalls(inj *faults.Injector, sc zeroCopySyscalls) zeroCopySyscalls {
	return zeroCopySyscalls{
		splice: inj.WrapSplice(sc.splice),
		tee:    inj.WrapTee(sc.tee),
		poll:   inj.WrapPoll(sc.poll),
	}
}

func streamFramePayloadZeroCopyWithSyscalls(fd *os.File, fileOffset *int64, args frameStreamArgs, syscalls zeroCopySyscalls) (frameStreamStats, error) {
	if err := args.Faults.Sys(faults.SysPipe); err != nil {
		return frameStreamStats{}, err
	}
	srcR, srcW, err := os.Pipe()
	if err != nil {
		return frameStreamStats{}, err
	}
	defer srcR.Close()
	defer srcW.Close()
	growPipeBestEffort(args.Faults, srcR, args.PipeSizeBytes)
	growPipeBestEffort(args.Faults, srcW, args.PipeSizeBytes)

	copyBufSize := bufpool.BucketSize(int64(args.PipeSizeBytes))
	copyBuf, releaseCopyBuf, err := bufpool.Acquire(copyBufSize)
	if err != nil {
		return frameStreamStats{}, err
	}
	defer releaseCopyBuf()

	if err := args.Faults.Sys(faults.SysDup); err != nil {
		return frameStreamStats{}, err
	}
	outFD, err := dupConnFD(args.OutputTCPConn)
	if err != nil {
		return frameStreamStats{}, err
	}
	defer unix.Close(outFD)

	// Hash pipe: tee duplicates pipe1 data here for hashing, while pipe1
	// data is spliced directly to the socket without entering userspace.
	if err := args.Faults.Sys(faults.SysPipe); err != nil {
		return frameStreamStats{}, err
	}
	hashPipeR, hashPipeW, err := os.Pipe()
	if err != nil {
		return frameStreamStats{}, err
	}
	defer hashPipeR.Close()
	defer hashPipeW.Close()
	growPipeBestEffort(args.Faults, hashPipeR, args.PipeSizeBytes)
	growPipeBestEffort(args.Faults, hashPipeW, args.PipeSizeBytes)

	writeLatency := time.Duration(0)
	if err := writeFrameHeader(args.Output, args, args.FrameSize, &writeLatency); err != nil {
		return frameStreamStats{}, err
	}

	prepareLatency := time.Duration(0)
	remaining := args.FrameSize

	// Fd() puts these into blocking mode — required for raw tee/splice syscalls.
	srcRFD := int(srcR.Fd())
	hashPipeWFD := int(hashPipeW.Fd())

	readRegion := trace.StartRegion(args.Ctx, "frame-read-zerocopy")
	for remaining > 0 {
		step := remaining
		if step > int64(args.PipeSizeBytes) {
			step = int64(args.PipeSizeBytes)
		}

		spliceStart := time.Now()
		splicedIn, spliceErr := syscalls.splice(int(fd.Fd()), fileOffset, int(srcW.Fd()), nil, int(step), unix.SPLICE_F_MOVE)
		prepareLatency += time.Since(spliceStart)
		if spliceErr != nil {
			if errors.Is(spliceErr, unix.EINTR) {
				continue
			}
			readRegion.End()
			return frameStreamStats{}, spliceErr
		}
		if splicedIn <= 0 {
			readRegion.End()
			return frameStreamStats{}, io.ErrUnexpectedEOF
		}

		// tee(2) does not consume src bytes. If tee returns a partial
		// duplicate, we must consume exactly that amount from src before
		// calling tee again, otherwise we'd duplicate the same prefix
		// repeatedly.
		sourceRemaining := splicedIn
		for sourceRemaining > 0 {
			teeStart := time.Now()
			teed, teeErr := syscalls.tee(srcRFD, hashPipeWFD, int(sourceRemaining), 0)
			prepareLatency += time.Since(teeStart)
			if teeErr != nil {
				if errors.Is(teeErr, unix.EINTR) {
					continue
				}
				readRegion.End()
				return frameStreamStats{}, teeErr
			}
			if teed <= 0 {
				readRegion.End()
				return frameStreamStats{}, io.ErrUnexpectedEOF
			}

			// splice: consume teed bytes from pipe1 → socket (zero-copy)
			writeStart := time.Now()
			moveRemaining := teed
			for moveRemaining > 0 {
				moved, moveErr := syscalls.splice(srcRFD, nil, outFD, nil, int(moveRemaining), unix.SPLICE_F_MOVE)
				if moveErr != nil {
					if errors.Is(moveErr, unix.EINTR) {
						continue
					}
					if errors.Is(moveErr, unix.EAGAIN) {
						if waitErr := waitSocketWritable(outFD, syscalls); waitErr != nil {
							readRegion.End()
							return frameStreamStats{}, waitErr
						}
						continue
					}
					readRegion.End()
					return frameStreamStats{}, moveErr
				}
				if moved <= 0 {
					readRegion.End()
					return frameStreamStats{}, io.ErrUnexpectedEOF
				}
				moveRemaining -= moved
			}
			writeLatency += time.Since(writeStart)

			// read from hash pipe + hash (the only userspace copy)
			readStart := time.Now()
			n, readErr := io.ReadFull(hashPipeR, copyBuf[:teed])
			prepareLatency += time.Since(readStart)
			if n > 0 {
				_, _ = args.WindowHasher.Write(copyBuf[:n])
			}
			if readErr != nil {
				readRegion.End()
				return frameStreamStats{}, readErr
			}
			sourceRemaining -= teed
		}
		remaining -= splicedIn
	}
	readRegion.End()

	windowHashToken, err := writeFrameTrailer(args.Output, args, &writeLatency)
	if err != nil {
		return frameStreamStats{}, err
	}

	return frameStreamStats{
		LogicalSize:     args.FrameSize,
		WireSize:        args.FrameSize,
		PrepareLatency:  prepareLatency,
		ReadLatency:     prepareLatency,
		WriteLatency:    writeLatency,
		NextOffset:      *fileOffset,
		WindowHashToken: windowHashToken,
	}, nil
}

func writeFrameHeader(out io.Writer, args frameStreamArgs, wireSize int64, writeLatency *time.Duration) error {
	buf, err := bufpool.Get(frameLineBytes)
	if err != nil {
		return err
	}
	line := appendFrameHeaderLine((*buf)[:0], args.FileID, args.Offset, args.FrameSize, wireSize, args.Comp, args.MaxWSizeHint, args.HeaderTS)
	err = timedWrite(out, line, writeLatency)
	bufpool.Put(buf)
	return err
}

// timedWrite writes p, adding the write's duration to latency when latency
// is not nil.
func timedWrite(out io.Writer, p []byte, latency *time.Duration) error {
	if latency == nil {
		_, err := out.Write(p)
		return err
	}
	start := time.Now()
	_, err := out.Write(p)
	*latency += time.Since(start)
	return err
}

func writeFrameTrailer(out io.Writer, args frameStreamArgs, writeLatency *time.Duration) (string, error) {
	windowHashToken := ""
	if args.IsTerminal {
		windowHashToken = encoding.FormatXXH128HashToken(args.WindowHasher.Sum128())
	}
	buf, err := bufpool.Get(frameLineBytes)
	if err != nil {
		return "", err
	}
	// One clock read stamps the trailer and starts the write timing.
	now := time.Now()
	line := appendFrameTrailerLine((*buf)[:0], args.FileID, now.UnixMilli(), args.Next, windowHashToken, args.TerminalMD)
	_, err = out.Write(line)
	bufpool.Put(buf)
	if err != nil {
		return "", err
	}
	if writeLatency != nil {
		*writeLatency += time.Since(now)
	}
	return windowHashToken, nil
}

// readTiming selects which latencies streamBufferedRead measures.
type readTiming uint8

const (
	timeNothing readTiming = iota
	timeReads
	// timeReadsAndHandling also charges handle to prepare latency, for
	// frames whose handler compresses.
	timeReadsAndHandling
)

func streamBufferedRead(
	src io.ReaderAt,
	fileOffset *int64,
	frameSize int64,
	buf []byte,
	timing readTiming,
	handle func([]byte) error,
) (prepareLatency, readLatency time.Duration, err error) {
	remaining := frameSize
	for remaining > 0 {
		readSize := len(buf)
		if int64(readSize) > remaining {
			readSize = int(remaining)
		}
		var start time.Time
		if timing != timeNothing {
			start = time.Now()
		}
		n, readErr := src.ReadAt(buf[:readSize], *fileOffset)
		if timing != timeNothing {
			readLatency += time.Since(start)
		}
		if n > 0 {
			*fileOffset += int64(n)
			remaining -= int64(n)
			if timing == timeReadsAndHandling {
				start = time.Now()
			}
			if err := handle(buf[:n]); err != nil {
				return 0, 0, err
			}
			if timing == timeReadsAndHandling {
				prepareLatency += time.Since(start)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && remaining == 0 {
				break
			}
			return 0, 0, readErr
		}
	}
	if remaining != 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return prepareLatency + readLatency, readLatency, nil
}

func acquireCompressedFrameBuffer(logicalSize int64, comp string) *bytes.Buffer {
	maxEncoded, err := encoding.MaxEncodedFrameSizeBytes(comp, logicalSize)
	if err != nil || maxEncoded <= 0 {
		maxEncoded = defaultCompressedFrameBufferBytes
	}
	capacity := int(maxEncoded)
	if capacity <= 0 {
		capacity = defaultCompressedFrameBufferBytes
	}

	if raw := compressedFrameBufferPool.Get(); raw != nil {
		if buf, ok := raw.(*bytes.Buffer); ok && buf != nil {
			buf.Reset()
			if buf.Cap() >= capacity {
				return buf
			}
			return bytes.NewBuffer(make([]byte, 0, capacity))
		}
	}
	return bytes.NewBuffer(make([]byte, 0, capacity))
}

func releaseCompressedFrameBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > maxCompressedFrameBufferPoolBytes {
		return
	}
	buf.Reset()
	compressedFrameBufferPool.Put(buf)
}

// minSequentialAdviceBytes is the smallest window worth advising. The kernel's
// default readahead (128 KiB) already covers a read this small, so the advice
// would be a syscall per file that changes nothing.
const minSequentialAdviceBytes = 128 << 10

func tryAdviseSequential(inj *faults.Injector, fd *os.File, offset int64, length int64) {
	if fd == nil || offset < 0 || length < minSequentialAdviceBytes || inj.Sys(faults.SysFadvise) != nil {
		return
	}
	_ = unix.Fadvise(int(fd.Fd()), offset, length, unix.FADV_SEQUENTIAL)
}

// readaheadHint describes a file region for the background readahead goroutine
// to prefetch into the page cache.
type readaheadHint struct {
	offset int64
	length int64
}

// tryReadAheadRange issues the readahead(2) syscall to asynchronously populate
// the page cache for the given byte range.
func tryReadAheadRange(inj *faults.Injector, fd *os.File, offset int64, length int64) {
	if fd == nil || offset < 0 || length <= 0 || inj.Sys(faults.SysReadahead) != nil {
		return
	}
	_, _, _ = unix.Syscall(unix.SYS_READAHEAD, fd.Fd(), uintptr(offset), uintptr(length))
}

// startFrameReadahead spawns a background goroutine that issues readahead(2)
// for file regions received on the returned channel. The goroutine exits when
// done is closed or the channel is closed. Call the returned stop function to
// ensure the goroutine has exited before closing fd.
func startFrameReadahead(inj *faults.Injector, fd *os.File, done <-chan struct{}) (chan readaheadHint, func()) {
	ch := make(chan readaheadHint, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case hint, ok := <-ch:
				if !ok {
					return
				}
				tryReadAheadRange(inj, fd, hint.offset, hint.length)
			case <-done:
				return
			}
		}
	}()
	stop := func() {
		close(ch)
		wg.Wait()
	}
	return ch, stop
}

// sendReadaheadHint sends a readahead hint to the background goroutine,
// discarding any stale hint already in the channel so that the goroutine
// always processes the most recent request.
func sendReadaheadHint(ch chan readaheadHint, hint readaheadHint) {
	select {
	case <-ch:
	default:
	}
	ch <- hint
}
