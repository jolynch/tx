package ftcp

import (
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
const defaultMaxLinuxPipeSizeBytes int64 = 1 * 1024 * 1024
const loadStrategyFast = "fast"
const loadStrategyGentle = "gentle"

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
	// CompressWide lets a compressed frame use more than one encoder
	// goroutine when the core budget has them free. The window's
	// encoding.FrameWidth sets it.
	CompressWide bool
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
	// CompressLatency and CompressConcurrency are the encoder's wall time
	// and goroutines. Both are zero for an uncompressed frame.
	CompressLatency     time.Duration
	CompressConcurrency int
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
		events.F("enc_conc", stats.CompressConcurrency),
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
	chains := requestChains(deps, header.TransferID, header.Mode, records, refs, refErrs)
	defer chains.release()
	hashes := make([]WindowHash, 0, len(records))
	var done []uint64
	var rates RateSummary
	var ratedFiles []uint64
	recordHashes := func() error {
		if len(done) > 0 {
			_ = deps.SetTransferFilesState(header.TransferID, done, TransferStateDone)
		}
		if len(hashes) > 0 || rates.Windows > 0 {
			if !deps.RecordTransferWindows(header.TransferID, WindowRecords{Hashes: hashes, Rates: rates, RatedFiles: ratedFiles}) {
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
	// The server's --bwlimit paces a fast request's buffer flushes, and
	// turns off its zero-copy frames, which would bypass the limiter.
	inj := faults.FromContext(ctx)
	fastOut := out
	if header.Mode != loadStrategyGentle && limiter.Config().RateBps > 0 {
		fastOut = limiter.WrapRateLimitedWriter(out, ctx)
	}
	resp := unbufferedSendOutput(fastOut)
	resp.recoveryFlush = transportFlusher(out)
	if header.Mode != loadStrategyGentle && resp.recoveryFlush == nil {
		resp = newBufferedSendOutput(fastOut)
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
			sent, err = streamSendWindow(ctx, itemOut, deps, header.TransferID, item, refs[i], chains.chain(i), zeroCopyMinBytes)
		}
		if sent.hash.HashToken != "" {
			hashes = append(hashes, sent.hash)
		}
		if sent.done {
			done = append(done, item.FileID)
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
// When a frame reached the policy, decided is set and rate holds what the
// window showed about the delivery rate, for the transfer's completion log.
type sentItem struct {
	hash    WindowHash
	done    bool
	decided bool
	rate    WindowRate
}

// streamSendWindow sends one item as frames. Frames of at least
// zeroCopyMinBytes use the zero-copy path when the output allows it;
// zeroCopyOff disables it. chain is the file's shared adaptive state, which
// the request resolved (requestChains), or nil.
func streamSendWindow(ctx context.Context, out *sendOutput, deps Deps, txferID string, item sendItem, fileRef FileRef, chain *policy.FileChain, zeroCopyMinBytes int64) (sent sentItem, _ error) {
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
	fileEnd := fileInfo.Size()
	if fileRef.FileSize >= 0 {
		fileEnd = min(fileEnd, fileRef.FileSize)
	}
	saveState := adaptive && item.Offset+windowLen < fileEnd
	// local is this window's view of the file's adaptive state; currentMode
	// is the codec of its next frame.
	local := policy.FileCompression{Mode: initialCompressionMode(item.Comp)}
	compressPolicy := &local.Policy
	// The windows of a file sent in several share one chain, so their
	// frames advance one policy in the order they finish, as one stream's
	// would, however many are in flight. Each decided frame holds the chain
	// just long enough to decide and copy the result into local, the
	// snapshot the window reads between decisions (decideChained). Which
	// windows get a chain is windowChain's.
	// A window that finds the chain held starts from a fresh state in the
	// chain's steady codec.
	if chain != nil {
		if st := chain.TryHold(); st != nil {
			local = *st
			chain.Release()
		} else {
			local.Mode = chain.SteadyMode()
		}
	}
	// claim is the check frame the window's current frame holds. One the
	// window exits holding goes back to the chain.
	var claim policy.CheckClaim
	defer func() {
		if chain != nil {
			chain.ReturnCheckFrame(claim)
		}
	}()
	currentMode := local.Mode
	if adaptive {
		// A file with no state of its own starts from what the connection
		// measured about its link; its codec evidence stays its own.
		// A chain's state is seeded when its first frame is decided.
		out.rate.seedLink(compressPolicy)
	}
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
	width := encoding.FrameWidth{Gentle: item.Mode == loadStrategyGentle}
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

		// recovering is whether this window's view of the policy measures
		// the frame for a recovery check or its validation. A chained
		// window sends a check's candidate codec only on a check frame it
		// claims from the chain, so a check sends a bounded number of
		// frames however many windows are in flight; without one, the
		// frame goes in home's codec, unmeasured.
		// A frame retried after a direct I/O fallback keeps its claim.
		recovering := compressPolicy.RecoveryActive()
		if chain != nil && !claim.Held() && compressPolicy.CheckRunning() && currentMode != compressPolicy.Home() {
			if claim = chain.ClaimCheckFrame(); !claim.Held() {
				currentMode, recovering = compressPolicy.Home(), false
			}
		}
		frameComp := policy.FrameCompTokenForMode(currentMode)
		// Every adaptive frame goes to the policy except a terminal frame
		// whose window leaves no state: its decision would choose a codec
		// for a frame that never comes. A saved terminal frame's decision
		// picks the next window's first codec. An active recovery still
		// observes a final frame so its loss reaches the connection budget.
		decide := adaptive && (!isTerminal || saveState || recovering)
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
			CompressWide:  width.Wide(frameComp),
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
		timedFlush := out.frameFlush == nil && out.recoveryFlush != nil && decide && recovering
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
		width.Observe(frameComp, stats.LogicalSize, stats.CompressConcurrency, stats.CompressLatency)
		firstFrame = false
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
			windowRate.Classes[metrics.Bottleneck]++
			var sample *encoding.CodecSamples
			// A completed sample is already paid for; it was admitted only
			// while it could still change the codec.
			if result, ready := sampler.take(); ready {
				sample = &result
				if result.Has(encoding.EncodingLz4) {
					metrics.SniffRatio = result.Ratio(encoding.EncodingLz4)
				}
			}
			windowRate.KernelBps, windowRate.UserBps = out.rate.readings()
			decidedFrame = true
			if sendFrameMetricsHook != nil {
				sendFrameMetricsHook(metrics)
			}
			var decision policy.CompressionDecision
			fed := true
			if chain == nil {
				decision = compressPolicy.DecideSampled(currentMode, metrics, sample)
			} else {
				decision, fed = decideChained(chain, &local, out.rate, chainedFrame{
					Mode: currentMode, Recovering: recovering, Metrics: metrics, Sample: sample})
			}
			loss := settleCheckFrame(chain, &claim, fed, compressPolicy, decision.RecoveryLoss, metrics)
			out.rate.noteRecovery(loss, stats.PrepareLatency+stats.WriteLatency)
			if !fed {
				// Another window decided this frame's place in the file's
				// policy: follow the codec the chain chose.
				currentMode = local.Mode
			} else {
				// Only a fed frame used its sample.
				windowRate.Sampled = windowRate.Sampled || sample != nil
				out.rate.saveLink(compressPolicy)
			}
			if fed && (decision.Next != currentMode || decision.Reason == "recovery-won") {
				prevComp := policy.FrameCompTokenForMode(currentMode)
				nextComp := policy.FrameCompTokenForMode(decision.Next)
				log.Printf(
					"txfer-frame: tid=%s fid=%d switching compression %s->%s reason=%s bottleneck=%s ratio=%.3f read_over_write=%.3f",
					txferID,
					item.FileID,
					prevComp,
					nextComp,
					decision.Reason,
					metrics.Bottleneck,
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
