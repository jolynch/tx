package ftcp

// Frame streaming: pipe sizing, frame header and trailer lines, buffered and
// zero-copy payloads, read timing, compressed-frame buffers, and readahead.

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/sysio"
)

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
		raw, err := os.ReadFile("/proc/sys/fs/pipe-max-size") //nolint:forbidigo // Kernel limit lookup; pipe setup uses the pipe and fcntl gates.
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
	sysio.SetPipeSize(inj, pipeFD, sizeBytes)
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
		src = args.Faults.WrapReaderAt(args.Ctx, faults.SiteSendRead, args.FileID, fd)
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
	// The caller applies the dup gate before entering this helper.
	dupFD, err := sysio.DupConn(nil, conn)
	if err != nil {
		return -1, err
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
	poll   func([]sysio.PollFd, int) (int, error)
}

func kernelZeroCopySyscalls() zeroCopySyscalls {
	splice, tee, poll := sysio.ZeroCopyCalls(nil)
	return zeroCopySyscalls{splice: splice, tee: tee, poll: poll}
}

func waitSocketWritable(fd int, syscalls zeroCopySyscalls) error {
	pollFDs := []sysio.PollFd{{Fd: int32(fd), Events: sysio.POLLOUT}}
	for {
		n, err := syscalls.poll(pollFDs, zeroCopyPollTimeoutMs)
		if err != nil {
			if errors.Is(err, sysio.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return errors.New("zero-copy splice timed out waiting for socket writable")
		}
		if pollFDs[0].Revents&(sysio.POLLERR|sysio.POLLHUP|sysio.POLLNVAL) != 0 {
			return errors.New("socket error during zero-copy splice")
		}
		if pollFDs[0].Revents&sysio.POLLOUT != 0 {
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
	srcR, srcW, err := sysio.Pipe(args.Faults)
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
	defer sysio.Close(outFD)

	// Hash pipe: tee duplicates pipe1 data here for hashing, while pipe1
	// data is spliced directly to the socket without entering userspace.
	hashPipeR, hashPipeW, err := sysio.Pipe(args.Faults)
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
		splicedIn, spliceErr := syscalls.splice(int(fd.Fd()), fileOffset, int(srcW.Fd()), nil, int(step), sysio.SPLICE_F_MOVE)
		prepareLatency += time.Since(spliceStart)
		if spliceErr != nil {
			if errors.Is(spliceErr, sysio.EINTR) {
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
				if errors.Is(teeErr, sysio.EINTR) {
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
				moved, moveErr := syscalls.splice(srcRFD, nil, outFD, nil, int(moveRemaining), sysio.SPLICE_F_MOVE)
				if moveErr != nil {
					if errors.Is(moveErr, sysio.EINTR) {
						continue
					}
					if errors.Is(moveErr, sysio.EAGAIN) {
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
	if fd == nil || offset < 0 || length < minSequentialAdviceBytes {
		return
	}
	_ = sysio.Fadvise(inj, fd.Fd(), offset, length, sysio.FADV_SEQUENTIAL)
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
	if fd == nil || offset < 0 || length <= 0 {
		return
	}
	_ = sysio.Readahead(inj, fd.Fd(), offset, length)
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
