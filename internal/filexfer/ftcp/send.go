package ftcp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/limit"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/jolynch/tx/internal/sysio"
	"github.com/jolynch/tx/internal/utils"
	"github.com/zeebo/xxh3"
)

const defaultFileFrameLogicalSize int64 = 4 * 1024 * 1024

// defaultZeroCopyMinFrameBytes is the smallest frame sent through tee/splice.
// Each zero-copy frame pays for two pipes, a socket dup, and their closes, so
// below about 1 MiB the buffered path is faster (BenchmarkSendFramePayload).
const defaultZeroCopyMinFrameBytes int64 = 1 << 20

// zeroCopyOff, as a zeroCopyMinBytes value, sends every frame buffered.
const zeroCopyOff int64 = 0

// sendSniffEvery spaces background samples by file frame index. A pending
// comparison asks each frame instead; busy workers always skip the request.
const sendSniffEvery = 8

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
	// Sample copies already-read bytes for a nonblocking background job.
	Sample *sendSampleJob
}

type frameStreamStats struct {
	LogicalSize    int64
	WireSize       int64
	PrepareLatency time.Duration
	// ReadLatency is the part of PrepareLatency spent reading the file; the
	// rest is hashing, compression, and copying a background sample.
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
	// Only files whose window learned something leave compression state, so
	// most requests never take a buffer from the pool.
	var comps *compUpdates
	defer func() { comps.release() }()
	var rates RateSummary
	var ratedFiles []uint64
	recordHashes := func() error {
		if len(done) > 0 {
			_ = deps.SetTransferFilesState(header.TransferID, done, TransferStateDone)
		}
		if compList := comps.list(); len(hashes) > 0 || len(compList) > 0 || rates.Windows > 0 {
			if !deps.RecordTransferWindows(header.TransferID, WindowRecords{Hashes: hashes, Compression: compList, Rates: rates, RatedFiles: ratedFiles}) {
				return protocolErr{code: "INTERNAL", message: "failed to store window hash state"}
			}
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
	resp.recoveryFlush = transportFlusher(out)
	if header.Mode != loadStrategyGentle && resp.recoveryFlush == nil {
		resp = newBufferedSendOutput(out)
		defer resp.release()
		resp.recoveryFlush = resp.Flush
	}
	resp.faults = inj
	resp.ownRate.src = sendRateSourceFor(ctx, out)
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
			itemOut.rate = resp.rate
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
		if sent.hasComp {
			if comps == nil {
				comps = compUpdatesPool.Get().(*compUpdates)
			}
			comps.s = append(comps.s, sent.comp)
		}
		if sent.decided {
			rates.Add(sent.rate)
			if sent.rate.FirstWindow {
				ratedFiles = append(ratedFiles, sent.rate.FileID)
			}
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

// sendFrameMetricsHook, when set by a test, sees the metrics of every frame
// handed to the compression policy, as DecideSampled sees them. Production leaves
// it nil.
var sendFrameMetricsHook func(policy.CompressionMetrics)

// compUpdates holds a request's compression updates in a reusable slice.
type compUpdates struct {
	s []FileCompressionUpdate
}

var compUpdatesPool = sync.Pool{New: func() any { return new(compUpdates) }}

// list returns the updates; a nil receiver has none.
func (c *compUpdates) list() []FileCompressionUpdate {
	if c == nil {
		return nil
	}
	return c.s
}

// release empties the updates and returns them to the pool. A nil receiver
// does nothing. The updates must not be used afterwards.
func (c *compUpdates) release() {
	if c == nil {
		return
	}
	c.s = c.s[:0]
	compUpdatesPool.Put(c)
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
	// recoveryFlush, when set, sends a fast response's buffered frames to
	// the socket; frames measured by a recovery check use it.
	recoveryFlush func() error
	// hasher is reset and reused by each item, rather than allocating 1.2 KiB
	// per file.
	hasher xxh3.Hasher128
	// faults gates the request's file and syscall sites; nil disables it.
	faults *faults.Injector
	// rate turns measured writes into the write times the compression
	// policy sees; it points at ownRate unless the response shares one
	// across outputs.
	rate    *sendRate
	ownRate sendRate
}

var sendOutputBufferPool = sync.Pool{
	New: func() any { return bufio.NewWriterSize(nil, sendOutputBufferBytes) },
}

// newBufferedSendOutput takes a pooled buffer; release returns it.
func newBufferedSendOutput(out io.Writer) *sendOutput {
	buf := sendOutputBufferPool.Get().(*bufio.Writer)
	buf.Reset(out)
	o := &sendOutput{w: buf, buf: buf, conn: extractTCPConn(out)}
	o.rate = &o.ownRate
	return o
}

func unbufferedSendOutput(out io.Writer) *sendOutput {
	o := &sendOutput{w: out, conn: extractTCPConn(out)}
	o.rate = &o.ownRate
	return o
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
// When another window of the file follows, comp is the compression state it
// starts from, and hasComp is set; an error exit still reports what the
// frames already sent taught the policy. When a frame reached the policy,
// decided is set and rate holds what the window showed about the delivery
// rate, for the transfer's completion log.
type sentItem struct {
	hash    WindowHash
	done    bool
	comp    FileCompressionUpdate
	hasComp bool
	decided bool
	rate    WindowRate
}

// streamSendItem sends one item as frames. Frames of at least zeroCopyMinBytes
// use the zero-copy path when the output allows it; zeroCopyOff disables it.
func streamSendItem(ctx context.Context, out *sendOutput, deps Deps, txferID string, item sendItem, fileRef FileRef, zeroCopyMinBytes int64) (sent sentItem, _ error) {
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
	// A file sent in several windows keeps its adaptive state between them,
	// so a learned mode and the policy's learned state survive each window
	// boundary.
	// This window works on its own copy; windows of one file that run
	// concurrently each save at their end, and the last save wins. A
	// single-window file never touches the store.
	//
	// State is loaded only when an earlier window preceded this one, and
	// saved only when a later one follows within the smaller of the
	// manifest size and the size on disk. The last window, a resumed tail
	// window therefore leave nothing behind that no ACK would free. A window
	// saved before the file shrank stays until the transfer is reaped, one
	// entry per file.
	startState := policy.FileCompression{Mode: currentMode, Policy: *compressPolicy}
	if adaptive && item.Offset > 0 {
		if st, ok := deps.LoadFileCompression(txferID, item.FileID); ok {
			currentMode = st.Mode
			*compressPolicy = st.Policy
			startState = st
		}
	}
	if adaptive {
		// A file with no state of its own starts from what the connection
		// measured about its link; its codec evidence stays its own.
		out.rate.seedLink(compressPolicy)
	}
	fileEnd := fileInfo.Size()
	if fileRef.FileSize >= 0 {
		fileEnd = min(fileEnd, fileRef.FileSize)
	}
	saveState := adaptive && item.Offset+windowLen < fileEnd
	framesSent := 0
	var sampler sendSampler
	defer sampler.close()
	// windowRate is what the window's decided frames showed about the
	// delivery rate, reported when one reached the policy, even on an error
	// exit.
	windowRate := WindowRate{FileID: item.FileID, FirstWindow: item.Offset == 0}
	decidedFrame := false
	if adaptive {
		defer func() {
			if decidedFrame {
				sent.decided, sent.rate = true, windowRate
			}
		}()
	}
	if saveState {
		// Also runs on an error exit, so what the frames already sent
		// taught the policy is not lost. The caller applies it with the
		// request's window hashes.
		defer func() {
			// A window that learned nothing leaves the stored state as is.
			if end := (policy.FileCompression{Mode: currentMode, Policy: *compressPolicy}); framesSent > 0 && end != startState {
				sent.comp = FileCompressionUpdate{FileID: item.FileID, State: end}
				sent.hasComp = true
			}
		}()
	}
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
		// Every adaptive frame goes to the policy except a terminal frame
		// whose window leaves no state: its decision would choose a codec
		// for a frame that never comes. A saved terminal frame's decision
		// picks the next window's first codec. An active recovery still
		// observes a final frame so its loss reaches the connection budget.
		decide := adaptive && (!isTerminal || saveState || compressPolicy.RecoveryActive())
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
			Timed:         scope.Sink.Enabled() || decide,
		}

		if decide && (compressPolicy.BackgroundPending() || (cursor/defaultFileFrameLogicalSize)%sendSniffEvery == 0) {
			windowEnd := item.Offset + windowLen
			work := sampleWork{
				fileFrames: framesLeft(cursor, windowEnd, fileEnd),
				fileBytes:  fileEnd - cursor,
				// A terminal frame decides only when the window saves state.
				windowFrames: framesLeft(cursor, windowEnd, windowEnd),
				windowBytes:  min(windowEnd, fileEnd) - cursor,
			}
			if !saveState {
				work.windowFrames--
				if work.windowBytes > 0 {
					work.windowBytes -= (work.windowBytes-1)%defaultFileFrameLogicalSize + 1
				}
			}
			frameArgs.Sample = sampler.request(ctx, work, compressPolicy, out.rate.wallFrame())
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

		// Checks and their validation judge codecs by raw frame time, which
		// a frame left in a fast response's buffer would miss entirely in a
		// one-frame window: the request's final flush follows the decision.
		// Such frames flush, and only they give up batching. Output earlier
		// frames and items left buffered drains first, untimed, so the
		// frame's flush times its own bytes alone.
		timedFlush := out.frameFlush == nil && out.recoveryFlush != nil && decide && compressPolicy.RecoveryActive()
		if timedFlush {
			if err := out.recoveryFlush(); err != nil {
				return sentItem{}, err
			}
		}

		frameOffset := cursor
		var stats frameStreamStats
		sendPath := sendPathBuffered
		if zeroCopyMinBytes > 0 && frameSize >= zeroCopyMinBytes && canZeroCopy(frameArgs) {
			// The splice bypasses out.w, so flush it and send this frame's
			// header and trailer to the socket as well, in order.
			// The flush writes earlier frames' bytes, work this frame forced,
			// so it counts toward this frame's write time as a buffered
			// frame's forced flush does.
			var flushStart time.Time
			if frameArgs.Timed {
				flushStart = time.Now()
			}
			if err := out.Flush(); err != nil {
				return sentItem{}, err
			}
			var flushLatency time.Duration
			if frameArgs.Timed {
				flushLatency = time.Since(flushStart)
			}
			frameArgs.Output = out.conn
			sendPath = sendPathSendfile
			stats, err = streamFramePayloadZeroCopy(fd, &frameOffset, frameArgs)
			stats.WriteLatency += flushLatency
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
				fd, _, err = openSendFile(inj, deps, sendItem{FileID: item.FileID, Offset: item.Offset, Mode: loadStrategyFast}, fileRef)
				if err != nil {
					return sentItem{}, mapLookupError(err)
				}
				usedDirectOpen = false
				remaining = item.Offset + windowLen - cursor
				if remaining < 0 {
					return sentItem{}, protocolErr{code: "INTERNAL", message: "invalid resume offset after direct I/O retry"}
				}
				if cursor == item.Offset {
					// No frame was sent, so the policy has not run and
					// holds the state the window started with.
					if item.Mode == loadStrategyFast {
						tryAdviseSequential(inj, fd, item.Offset, windowLen)
					}
					firstFrame = true
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

		flush := out.frameFlush
		if timedFlush {
			flush = out.recoveryFlush
		}
		if flush != nil {
			// A compressed frame may fit entirely in the transport's record
			// buffer. Its flush wait is part of this frame's write cost.
			var flushStart time.Time
			if frameArgs.Timed {
				flushStart = time.Now()
			}
			if err := flush(); err != nil {
				return sentItem{}, err
			}
			if frameArgs.Timed {
				stats.WriteLatency += time.Since(flushStart)
			}
		}
		if scope.Sink.Enabled() {
			emitWindow(scope, txferID, item.FileID, cursor, frameComp, sendPath, stats)
		}
		cursor = stats.NextOffset
		remaining -= frameSize
		firstFrame = false
		framesSent++
		if isTerminal && scope.Sink.Enabled() && cursor >= fileRef.FileSize {
			scope.Sink.Emit("file_done", txferID, events.F("conn", scope.Conn), events.F("file", item.FileID), events.Dur("dur", time.Since(itemStart)))
		}

		if isTerminal {
			if stats.WindowHashToken == "" {
				return sentItem{}, protocolErr{code: "INTERNAL", message: "failed to finalize window hash"}
			}
			recorded = WindowHash{FileID: item.FileID, EndBytes: cursor, HashToken: stats.WindowHashToken}
		}

		if decide {
			// A write into the response buffer or the socket's send buffer
			// returns before the bytes leave; out.rate raises such a write
			// to its wire bytes over the connection's delivery rate. A
			// flush a frame forced is charged to that frame, which is the
			// backpressure signal the policy wants.
			metrics := policy.CompressionMetrics{
				LogicalSize:     stats.LogicalSize,
				WireSize:        stats.WireSize,
				PrepareLatency:  stats.PrepareLatency,
				ReadLatency:     stats.ReadLatency,
				WriteLatency:    out.rate.writeTime(stats.WireSize, stats.WriteLatency),
				FrameLatency:    stats.PrepareLatency + stats.WriteLatency,
				RecoveryAllowed: fileEnd-cursor >= policy.RecoveryFrames*defaultFileFrameLogicalSize,
				RecoveryReady:   out.rate.recoveryReady(),
				Bottleneck:      out.rate.bottleneck(),
			}
			out.rate.observeFrame(stats.LogicalSize, stats.PrepareLatency+stats.WriteLatency)
			var sample *encoding.CodecSamples
			// A completed sample is already paid for; it was admitted only
			// while it could still change the codec.
			if result, ready := sampler.take(); ready {
				sample = &result
				if result.Has(encoding.EncodingLz4) {
					metrics.SniffRatio = result.Ratio(encoding.EncodingLz4)
				}
				windowRate.Sampled = true
			}
			windowRate.KernelBps, windowRate.UserBps = out.rate.readings()
			decidedFrame = true
			if sendFrameMetricsHook != nil {
				sendFrameMetricsHook(metrics)
			}
			decision := compressPolicy.DecideSampled(currentMode, metrics, sample)
			out.rate.noteRecovery(decision.RecoveryLoss, stats.PrepareLatency+stats.WriteLatency)
			out.rate.saveLink(compressPolicy)
			if decision.Next != currentMode || decision.Reason == "recovery-won" {
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
	if err := openFault(inj, faults.SiteSendOpen, item.FileID, item.Offset); err != nil {
		return nil, false, err
	}
	if item.Mode == loadStrategyGentle {
		if fd, err := os.OpenFile(fileRef.Path, os.O_RDONLY|sysio.O_DIRECT, 0); err == nil { //nolint:forbidigo // OpenErr above; frame reads use WrapReaderAt or gated splice.
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
	return errors.Is(err, sysio.EINVAL)
}

// canZeroCopy reports whether a frame can use the tee+splice zero-copy send
// path, which avoids copying payload bytes into userspace for the network
// write. The path requires all of:
//   - Linux (tee/splice are Linux-only syscalls)
//   - No compression (compressed data must be assembled in userspace)
//   - No O_DIRECT (direct I/O requires aligned userspace buffers, not pipes)
//   - A raw TCP socket (no encryption or rate-limit wrapper over the conn)
//   - No fault injection that must see the payload (read.flip)
func canZeroCopy(args frameStreamArgs) bool {
	return runtime.GOOS == "linux" &&
		args.Comp == "none" &&
		!args.DirectIO &&
		args.OutputTCPConn != nil &&
		args.Faults.ZeroCopyReads()
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
