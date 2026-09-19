package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

func TestParseSENDRequestCompDefaultsAndModes(t *testing.T) {
	req, err := ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt"`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseSENDRequest(req)
	if err != nil {
		t.Fatalf("parseSENDRequest failed: %v", err)
	}
	if parsed.Items[0].Comp != "adapt" {
		t.Fatalf("expected default comp adapt, got %q", parsed.Items[0].Comp)
	}

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "none", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=none`, want: "none"},
		{name: "identity", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=identity`, want: "none"},
		{name: "lz4", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=lz4`, want: encoding.EncodingLz4},
		{name: "zstd", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=zstd`, want: encoding.EncodingZstd},
		{name: "adapt", raw: `SEND tx1 fd=1 "/tmp/a.txt" comp=adapt`, want: "adapt"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			parsed, err := parseSENDRequest(req)
			if err != nil {
				t.Fatalf("parseSENDRequest failed: %v", err)
			}
			if got := parsed.Items[0].Comp; got != tc.want {
				t.Fatalf("expected comp %q, got %q", tc.want, got)
			}
		})
	}

	req, err = ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt" comp=snappy`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	_, err = parseSENDRequest(req)
	if err == nil {
		t.Fatalf("expected unsupported comp error")
	}
	var pe protocolErr
	if !errors.As(err, &pe) || pe.code != "UNSUPPORTED_COMP" {
		t.Fatalf("expected UNSUPPORTED_COMP, got %v", err)
	}
}

func TestBuildFrameHeaderLineOmitsPlaceholderHash(t *testing.T) {
	header := buildFrameHeaderLine(7, 0, 5, 5, "none", nil, 1000)
	if strings.Contains(header, " hash=") {
		t.Fatalf("expected FTCP SEND header to omit placeholder hash, got %q", header)
	}
	if !strings.Contains(header, " comp=none ts=1000") {
		t.Fatalf("unexpected header contents: %q", header)
	}
}

func TestParseSENDRequestModeDefaultsAndValidation(t *testing.T) {
	req, err := ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt"`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parseSENDRequest(req)
	if err != nil {
		t.Fatalf("parseSENDRequest failed: %v", err)
	}
	if got := parsed.Items[0].Mode; got != loadStrategyFast {
		t.Fatalf("expected default mode %q, got %q", loadStrategyFast, got)
	}

	req, err = ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt" mode=gentle`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err = parseSENDRequest(req)
	if err != nil {
		t.Fatalf("parseSENDRequest failed: %v", err)
	}
	if got := parsed.Items[0].Mode; got != loadStrategyGentle {
		t.Fatalf("expected mode %q, got %q", loadStrategyGentle, got)
	}

	req, err = ParseRequest([]byte(`SEND tx1 fd=1 "/tmp/a.txt" mode=slow`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	_, err = parseSENDRequest(req)
	if err == nil {
		t.Fatalf("expected mode validation error")
	}
}

func TestStreamSendItemRoundTripCompressionModes(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz012345"), 8192)

	for _, comp := range []string{"none", encoding.EncodingLz4, encoding.EncodingZstd} {
		t.Run(comp, func(t *testing.T) {
			tmp := writeTempSendFile(t, data)
			deps := &mockDeps{filePath: tmp}
			var out bytes.Buffer

			err := streamSendItem(context.Background(), &out, deps, "tx1", sendItem{FileID: 7, Offset: 0, Size: 0, Comp: comp, Path: tmp}, false)
			if err != nil {
				t.Fatalf("streamSendItem failed: %v", err)
			}

			frames, err := decodeFrameStream(out.Bytes())
			if err != nil {
				t.Fatalf("decodeFrameStream failed: %v", err)
			}
			if len(frames) != 1 {
				t.Fatalf("expected one frame, got %d", len(frames))
			}
			if frames[0].Header.Comp != comp {
				t.Fatalf("expected comp %q, got %q", comp, frames[0].Header.Comp)
			}
			if !bytes.Equal(frames[0].Logical, data) {
				t.Fatalf("decoded logical payload mismatch")
			}

			expectedHash := encoding.FormatXXH128HashToken(xxh3.Hash128(data))
			if deps.windowHash != expectedHash {
				t.Fatalf("unexpected stored window hash: got=%q want=%q", deps.windowHash, expectedHash)
			}
			if deps.windowHashEnd != int64(len(data)) {
				t.Fatalf("unexpected window hash end: got=%d want=%d", deps.windowHashEnd, len(data))
			}
			if deps.setWindowCalls != 1 {
				t.Fatalf("expected one window hash set call, got %d", deps.setWindowCalls)
			}
		})
	}
}

func TestStreamSendItemAdaptiveUpgradesFromNone(t *testing.T) {
	// Give adaptive mode enough runway that even slower CI machines should
	// switch away from "none" before the stream is exhausted.
	size := (12 * defaultFileFrameLogicalSize) + 1
	data := bytes.Repeat([]byte("compress-me-"), int(size/int64(len("compress-me-")))+1)
	data = data[:size]
	tmp := writeTempSendFile(t, data)
	deps := &mockDeps{filePath: tmp}

	var rawOut bytes.Buffer
	slowOut := delayedWriter{w: &rawOut, delay: 25 * time.Millisecond}
	err := streamSendItem(context.Background(), &slowOut, deps, "tx-adapt", sendItem{FileID: 9, Offset: 0, Size: 0, Comp: "adapt", Path: tmp}, false)
	if err != nil {
		t.Fatalf("streamSendItem failed: %v", err)
	}

	comps, err := frameComps(rawOut.Bytes())
	if err != nil {
		t.Fatalf("frameComps failed: %v", err)
	}
	if len(comps) < 10 {
		t.Fatalf("expected >=10 frames for adaptive test, got %d", len(comps))
	}
	if comps[0] != "none" {
		t.Fatalf("expected first frame to start at none, got %q", comps[0])
	}
	sawCompressed := false
	for _, comp := range comps[1:] {
		if comp == encoding.EncodingLz4 || comp == encoding.EncodingZstd {
			sawCompressed = true
			break
		}
	}
	if !sawCompressed {
		t.Fatalf("expected adaptive mode to upgrade to a compressed frame, comps=%v", comps)
	}
}

func TestStreamSendItemDirectoryMetadataOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "meta-dir")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := os.Chmod(dir, 0o3750); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	wantMode := encoding.FormatManifestMode(info.Mode())
	deps := &mockDeps{filePath: dir, entryType: encoding.EntryTypeDir}

	var out bytes.Buffer
	if err := streamSendItem(context.Background(), &out, deps, "tx-dir", sendItem{FileID: 11, Offset: 0, Size: 0, Comp: "adapt", Path: dir}, false); err != nil {
		t.Fatalf("streamSendItem directory failed: %v", err)
	}

	frames, err := decodeFrameStream(out.Bytes())
	if err != nil {
		t.Fatalf("decodeFrameStream failed: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected one metadata frame, got %d", len(frames))
	}
	if frames[0].Header.FileID != 11 || frames[0].Header.Size != 0 || frames[0].Header.WireSize != 0 || frames[0].Header.Comp != "none" {
		t.Fatalf("unexpected metadata header: %+v", frames[0].Header)
	}
	if len(frames[0].Logical) != 0 {
		t.Fatalf("metadata frame payload len = %d, want 0", len(frames[0].Logical))
	}
	if frames[0].Trailer.Next == nil || *frames[0].Trailer.Next != 0 {
		t.Fatalf("metadata trailer next = %v, want 0", frames[0].Trailer.Next)
	}
	expectedHash := encoding.FormatXXH128HashToken(xxh3.Hash128(nil))
	if frames[0].Trailer.FileHashToken != expectedHash {
		t.Fatalf("metadata file hash = %q, want %q", frames[0].Trailer.FileHashToken, expectedHash)
	}
	raw := out.String()
	for _, token := range []string{"meta:size=0", "meta:mtime_ns=", "meta:mode=" + wantMode, "meta:uid=", "meta:gid="} {
		if !strings.Contains(raw, token) {
			t.Fatalf("metadata trailer missing %q in %q", token, raw)
		}
	}
	if deps.windowHash != expectedHash || deps.windowHashEnd != 0 || deps.setWindowCalls != 1 {
		t.Fatalf("unexpected stored metadata hash state hash=%q end=%d calls=%d", deps.windowHash, deps.windowHashEnd, deps.setWindowCalls)
	}
}

type decodedFrame struct {
	Header  encoding.FileFrameMeta
	Logical []byte
	Trailer encoding.FrameTrailer
}

func decodeFrameStream(raw []byte) ([]decodedFrame, error) {
	br := bufio.NewReader(bytes.NewReader(raw))
	frames := make([]decodedFrame, 0, 4)
	for {
		headerLine, err := br.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}
		headerTrimmed := strings.TrimRight(headerLine, "\r\n")
		header, err := encoding.ParseFXHeader(headerTrimmed)
		if err != nil {
			return nil, fmt.Errorf("parse header: %w", err)
		}
		if header.WireSize < 0 {
			return nil, errors.New("negative wire size")
		}
		payload := make([]byte, header.WireSize)
		if _, err := io.ReadFull(br, payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
		decodedReader, err := encoding.DecodePayloadReaderByComp(bytes.NewReader(payload), header.Comp)
		if err != nil {
			return nil, fmt.Errorf("decode payload: %w", err)
		}
		logical, readErr := io.ReadAll(decodedReader)
		closeErr := decodedReader.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read decoded payload: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close decoded payload: %w", closeErr)
		}

		trailerLine, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read trailer: %w", err)
		}
		trailer, err := encoding.ParseFXTrailer(strings.TrimRight(trailerLine, "\r\n"))
		if err != nil {
			return nil, fmt.Errorf("parse trailer: %w", err)
		}
		frames = append(frames, decodedFrame{Header: header, Logical: logical, Trailer: trailer})
	}
	return frames, nil
}

func frameComps(raw []byte) ([]string, error) {
	br := bufio.NewReader(bytes.NewReader(raw))
	comps := make([]string, 0, 8)
	for {
		headerLine, err := br.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		header, err := encoding.ParseFXHeader(strings.TrimRight(headerLine, "\r\n"))
		if err != nil {
			return nil, err
		}
		comps = append(comps, header.Comp)
		if _, err := io.CopyN(io.Discard, br, header.WireSize); err != nil {
			return nil, err
		}
		if _, err := br.ReadString('\n'); err != nil {
			return nil, err
		}
	}
	return comps, nil
}

func writeTempSendFile(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

type delayedWriter struct {
	w     io.Writer
	delay time.Duration
}

func (d *delayedWriter) Write(p []byte) (int, error) {
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	return d.w.Write(p)
}

func TestHandleSENDBasic(t *testing.T) {
	data := []byte("hello send")
	tmp := writeTempSendFile(t, data)
	deps := &mockDeps{filePath: tmp}
	payload := []byte(`SEND tx1 fd=1 ` + strconv.Quote(tmp))
	req, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	var out bytes.Buffer
	if err := handleSEND(context.Background(), req, &out, deps); err != nil {
		t.Fatalf("handleSEND failed: %v", err)
	}
	frames, err := decodeFrameStream(out.Bytes())
	if err != nil {
		t.Fatalf("decodeFrameStream failed: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected one frame, got %d", len(frames))
	}
	if !bytes.Equal(frames[0].Logical, data) {
		t.Fatalf("unexpected logical bytes")
	}
}

func TestStreamFramePayloadZeroCopyHandlesShortInterruptedAndBackpressuredSyscalls(t *testing.T) {
	data := zeroCopyPayload(160 * 1024)
	path := writeTempSendFile(t, data)
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()

	server, client := newTCPPair(t)
	defer server.Close()
	defer client.Close()
	if err := server.SetWriteBuffer(4096); err != nil {
		t.Fatalf("SetWriteBuffer: %v", err)
	}
	if err := client.SetReadBuffer(4096); err != nil {
		t.Fatalf("SetReadBuffer: %v", err)
	}
	releaseReader := make(chan struct{})
	var releaseReaderOnce sync.Once
	release := func() { releaseReaderOnce.Do(func() { close(releaseReader) }) }
	defer release()

	syscalls := kernelZeroCopySyscalls()
	interruptedSource, interruptedTee := true, true
	sawBackpressure := false
	pollCalls := 0
	closedFDs := make(map[int]unix.Stat_t)
	observeFD := func(fd int) {
		if _, seen := closedFDs[fd]; !seen {
			var stat unix.Stat_t
			if err := unix.Fstat(fd, &stat); err != nil {
				panic(err) // A live syscall descriptor must be valid.
			}
			closedFDs[fd] = stat
		}
	}
	syscalls.splice = func(src int, srcOffset *int64, dst int, dstOffset *int64, length int, flags int) (int64, error) {
		if srcOffset != nil {
			observeFD(dst)
			if interruptedSource {
				interruptedSource = false
				return 0, unix.EINTR
			}
		} else {
			observeFD(src)
			observeFD(dst)
		}
		n, err := unix.Splice(src, srcOffset, dst, dstOffset, min(length, 701), flags)
		if errors.Is(err, unix.EAGAIN) {
			sawBackpressure = true
			release()
		}
		return n, err
	}
	syscalls.tee = func(src int, dst int, length int, flags int) (int64, error) {
		observeFD(src)
		observeFD(dst)
		if interruptedTee {
			interruptedTee = false
			return 0, unix.EINTR
		}
		return unix.Tee(src, dst, min(length, 353), flags)
	}
	syscalls.poll = func(fds []unix.PollFd, timeout int) (int, error) {
		if timeout != zeroCopyPollTimeoutMs {
			return 0, fmt.Errorf("poll timeout = %d, want %d", timeout, zeroCopyPollTimeoutMs)
		}
		pollCalls++
		return unix.Poll(fds, timeout)
	}

	type result struct {
		stats frameStreamStats
		err   error
	}
	resultCh := make(chan result, 1)
	rawCh := make(chan []byte, 1)
	readErrCh := make(chan error, 1)
	go func() {
		<-releaseReader
		raw, readErr := io.ReadAll(client)
		rawCh <- raw
		readErrCh <- readErr
	}()
	go func() {
		offset := int64(0)
		stats, streamErr := streamFramePayloadZeroCopyWithSyscalls(file, &offset, frameStreamArgs{
			Ctx:           context.Background(),
			FileID:        1,
			FrameSize:     int64(len(data)),
			Comp:          "none",
			HeaderTS:      1,
			IsTerminal:    true,
			WindowHasher:  xxh3.New128(),
			Output:        server,
			OutputTCPConn: server,
			PipeSizeBytes: 4096,
		}, syscalls)
		_ = server.Close()
		resultCh <- result{stats: stats, err: streamErr}
	}()
	var got result
	select {
	case got = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("zero-copy stream did not finish")
	}
	if got.err != nil {
		t.Fatalf("zero-copy stream: %v", got.err)
	}
	if got.stats.NextOffset != int64(len(data)) {
		t.Fatalf("next offset = %d, want %d", got.stats.NextOffset, len(data))
	}
	if got.stats.WindowHashToken != encoding.FormatXXH128HashToken(xxh3.Hash128(data)) {
		t.Fatalf("window hash = %q", got.stats.WindowHashToken)
	}
	if interruptedSource || interruptedTee {
		t.Fatal("zero-copy stream did not retry an interrupted syscall")
	}
	if !sawBackpressure || pollCalls == 0 {
		t.Fatalf("zero-copy stream did not resume from real socket backpressure (EAGAIN=%t polls=%d)", sawBackpressure, pollCalls)
	}
	// srcR, srcW, hashPipeW, and the duplicated socket FD all cross the
	// syscall seam. hashPipeR is only used by io.ReadFull and is covered by
	// the function's deferred Close rather than an observable raw operation.
	if len(closedFDs) != 4 {
		t.Fatalf("observed %d temporary descriptors, want 4", len(closedFDs))
	}
	for fd, original := range closedFDs {
		var current unix.Stat_t
		err := unix.Fstat(fd, &current)
		if err == nil && current.Dev == original.Dev && current.Ino == original.Ino {
			t.Errorf("temporary zero-copy fd %d was not closed", fd)
		} else if err != nil && !errors.Is(err, unix.EBADF) {
			t.Errorf("stat temporary fd %d: %v", fd, err)
		}
	}

	var raw []byte
	select {
	case raw = <-rawCh:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not receive zero-copy stream")
	}
	if err := <-readErrCh; err != nil {
		t.Fatalf("read zero-copy stream: %v", err)
	}
	frames, err := decodeFrameStream(raw)
	if err != nil {
		t.Fatalf("decode zero-copy stream: %v", err)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0].Logical, data) {
		t.Fatalf("zero-copy payload did not survive short syscall results")
	}
}

func TestStreamFramePayloadZeroCopyShortSourceDoesNotFinalizeHash(t *testing.T) {
	data := zeroCopyPayload(1024)
	file, err := os.Open(writeTempSendFile(t, data))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	server, client := newTCPPair(t)
	defer client.Close()
	defer server.Close()

	offset := int64(0)
	stats, err := streamFramePayloadZeroCopy(file, &offset, frameStreamArgs{
		Ctx:           context.Background(),
		FileID:        1,
		FrameSize:     int64(len(data) + 1),
		Comp:          "none",
		HeaderTS:      1,
		IsTerminal:    true,
		WindowHasher:  xxh3.New128(),
		Output:        server,
		OutputTCPConn: server,
		PipeSizeBytes: 4096,
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short source error = %v, want ErrUnexpectedEOF", err)
	}
	if stats.WindowHashToken != "" {
		t.Fatalf("short source finalized hash %q", stats.WindowHashToken)
	}
}

func TestStreamSendItemZeroCopyDisconnectDoesNotRecordHash(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("tee/splice zero-copy is Linux-only")
	}
	data := zeroCopyPayload(int(defaultFileFrameLogicalSize + 64*1024))
	path := writeTempSendFile(t, data)
	deps := &mockDeps{filePath: path}
	server, client := newTCPPair(t)
	defer server.Close()
	defer client.Close()
	closed := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(client)
		if _, err := reader.ReadString('\n'); err != nil {
			closed <- err
			return
		}
		if _, err := io.CopyN(io.Discard, reader, 1024); err != nil {
			closed <- err
			return
		}
		_ = client.SetLinger(0)
		closed <- client.Close()
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- streamSendItem(context.Background(), server, deps, "tx1", sendItem{
			FileID: 1,
			Comp:   "none",
			Path:   path,
			Mode:   loadStrategyFast,
		}, false)
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("receiver did not disconnect during payload: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not read and disconnect")
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("zero-copy SEND unexpectedly succeeded after receiver disconnect")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("zero-copy SEND did not terminate after receiver disconnect")
	}
	if deps.setWindowCalls != 0 || deps.windowHash != "" {
		t.Fatalf("failed SEND recorded terminal hash: calls=%d hash=%q", deps.setWindowCalls, deps.windowHash)
	}
}

func TestWaitSocketWritableRejectsNonWritableEvents(t *testing.T) {
	type pollResult struct {
		n       int
		err     error
		revents int16
	}
	tests := []struct {
		name    string
		results []pollResult
		wantErr bool
	}{
		{name: "writable", results: []pollResult{{n: 1, revents: unix.POLLOUT}}},
		{name: "interrupted then writable", results: []pollResult{{err: unix.EINTR}, {n: 1, revents: unix.POLLOUT}}},
		{name: "timeout", results: []pollResult{{}}, wantErr: true},
		{name: "error", results: []pollResult{{n: 1, revents: unix.POLLERR | unix.POLLOUT}}, wantErr: true},
		{name: "hangup", results: []pollResult{{n: 1, revents: unix.POLLHUP | unix.POLLOUT}}, wantErr: true},
		{name: "invalid descriptor", results: []pollResult{{n: 1, revents: unix.POLLNVAL | unix.POLLOUT}}, wantErr: true},
		{name: "readable only", results: []pollResult{{n: 1, revents: unix.POLLIN}}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			syscalls := kernelZeroCopySyscalls()
			syscalls.poll = func(fds []unix.PollFd, timeout int) (int, error) {
				if timeout != zeroCopyPollTimeoutMs {
					t.Fatalf("timeout = %d, want %d", timeout, zeroCopyPollTimeoutMs)
				}
				if calls >= len(tc.results) {
					t.Fatal("unexpected extra poll")
				}
				result := tc.results[calls]
				calls++
				fds[0].Revents = result.revents
				return result.n, result.err
			}
			err := waitSocketWritable(1, syscalls)
			if (err != nil) != tc.wantErr {
				t.Fatalf("waitSocketWritable error = %v, want error=%t", err, tc.wantErr)
			}
			if calls != len(tc.results) {
				t.Fatalf("poll calls = %d, want %d", calls, len(tc.results))
			}
		})
	}
}

func newTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	defer listener.Close()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatalf("DialTCP: %v", err)
	}
	server, err := listener.AcceptTCP()
	if err != nil {
		_ = client.Close()
		t.Fatalf("AcceptTCP: %v", err)
	}
	return server, client
}

func zeroCopyPayload(size int) []byte {
	payload := make([]byte, size)
	for offset := 0; offset < len(payload); {
		block := sha256.Sum256([]byte(strconv.Itoa(offset)))
		offset += copy(payload[offset:], block[:])
	}
	return payload
}
