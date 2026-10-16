package ftcp

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jolynch/tx/internal/benchio"
	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
)

const checksumReadBufferSize int64 = 1 * 1024 * 1024

type cxsumItem struct {
	FileID     uint64
	Path       string
	Offset     int64
	Size       int64
	HasSize    bool
	Algorithms []string
}

// The low two flag bits select hashes; the third records an explicit size.
const (
	checksumXXH128 uint8 = 1 << iota
	checksumXXH64
	checksumHasSize
)

type checksumRequestRecord struct {
	FileID uint64
	Offset int64
	Size   int64
	Path   string
	Flags  uint8
}

func parseCXSUMRecord(raw map[string]string) (checksumRequestRecord, error) {
	item, err := parseCXSUMItem(raw)
	if err != nil {
		return checksumRequestRecord{}, err
	}
	r := checksumRequestRecord{FileID: item.FileID, Offset: item.Offset, Size: item.Size, Path: item.Path}
	if item.HasSize {
		r.Flags |= checksumHasSize
	}
	for _, algo := range item.Algorithms {
		switch algo {
		case "xxh128":
			r.Flags |= checksumXXH128
		case "xxh64":
			r.Flags |= checksumXXH64
		}
	}
	return r, nil
}

var checksumRequestAlgorithms = [...][]string{
	nil, {"xxh128"}, {"xxh64"}, {"xxh128", "xxh64"},
}

func (r checksumRequestRecord) item() cxsumItem {
	return cxsumItem{FileID: r.FileID, Offset: r.Offset, Size: r.Size, Path: r.Path,
		HasSize: r.Flags&checksumHasSize != 0, Algorithms: checksumRequestAlgorithms[r.Flags&(checksumXXH128|checksumXXH64)]}
}

type cxsumRequest struct {
	TransferID string
	Items      []cxsumItem
}

// parseCXSUMItem turns one framed body item into a cxsumItem.
func parseCXSUMItem(p map[string]string) (cxsumItem, error) {
	fileID, err := strconv.ParseUint(p["fid"], 10, 64)
	if err != nil {
		return cxsumItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid file id"}
	}
	path := p["path"]
	if path == "" {
		return cxsumItem{}, protocolErr{code: "BAD_REQUEST", message: "missing path"}
	}
	offset := int64(0)
	if raw := strings.TrimSpace(p["offset"]); raw != "" {
		offset, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || offset < 0 {
			return cxsumItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid checksum offset"}
		}
	}
	size := int64(0)
	hasSize := false
	if raw := strings.TrimSpace(p["size"]); raw != "" {
		size, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || size < 0 {
			return cxsumItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid checksum size"}
		}
		hasSize = true
	}
	algorithms, err := parseRequestedChecksums([]string{p["algo"]})
	if err != nil {
		return cxsumItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid checksum parameter"}
	}
	return cxsumItem{
		FileID:     fileID,
		Path:       path,
		Offset:     offset,
		Size:       size,
		HasSize:    hasSize,
		Algorithms: algorithms,
	}, nil
}

func cxsumTransferID(req Request) (string, error) {
	if req.Verb != VerbCXSUM {
		return "", protocolErr{code: "BAD_COMMAND", message: "not CXSUM"}
	}
	if len(req.Params) != 1 {
		return "", protocolErr{code: "BAD_REQUEST", message: "invalid CXSUM arguments"}
	}
	txferID := strings.TrimSpace(req.Params[0]["txferid"])
	if txferID == "" {
		return "", protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
	}
	return txferID, nil
}

func handleCXSUM(_ context.Context, req Request, out io.Writer, deps Deps) error {
	return protocolErr{code: "INTERNAL", message: "CXSUM requires a request body, use handleCXSUMWithInput"}
}

// handleCXSUMWithInput validates the complete body before emitting checksums.
func handleCXSUMWithInput(ctx context.Context, req Request, in io.Reader, out io.Writer, deps Deps) error {
	txferID, err := cxsumTransferID(req)
	if err != nil {
		return err
	}
	records, err := parseRequestItemRecords(in, cxsumItemKeys, "CXSUM", parseCXSUMRecord)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return protocolErr{code: "BAD_REQUEST", message: "CXSUM requires at least one item"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Resolve under one transfer lock; report lookup errors only when their
	// item is reached, after writing the preceding checksum frames.
	lookups := make([]FileLookup, len(records))
	for i, record := range records {
		lookups[i] = FileLookup{FileID: record.FileID, Path: record.Path}
	}
	refs, refErrs := deps.GetFileRefs(txferID, lookups)

	buf, err := bufpool.Get(checksumBufferSize(records))
	if err != nil {
		return err
	}
	defer bufpool.Put(buf)

	// Buffer the small frames so each costs no write syscall. Flush before
	// any return, so an ERR line written after an error follows the frames.
	// A transport that batches writes already buffers, so it gets no second
	// buffer; it is flushed at most every cxsumProgressInterval, at a frame
	// boundary, so checksums arrive as they finish without one record per
	// frame.
	flush := transportFlusher(out)
	w := out
	var bw *bufio.Writer
	if flush == nil {
		bw = bufio.NewWriterSize(out, 64<<10)
		w = bw
	}
	flushBuffer := func() error {
		if bw == nil {
			return nil
		}
		return bw.Flush()
	}
	line, err := bufpool.Get(frameLineBytes)
	if err != nil {
		return err
	}
	defer bufpool.Put(line)
	scratch := checksumScratch{buf: *buf, line: line}
	lastFlush := time.Now()
	for i, record := range records {
		err := ctx.Err()
		if err == nil {
			err = mapLookupError(refErrs[i])
		}
		if err == nil {
			err = streamChecksumItem(ctx, w, deps, refs[i], record.item(), &scratch)
		}
		if err != nil {
			_ = flushBuffer()
			return err
		}
		if flush != nil && time.Since(lastFlush) >= cxsumProgressInterval {
			if err := flush(); err != nil {
				return err
			}
			lastFlush = time.Now()
		}
	}
	return flushBuffer()
}

// checksumBufferSize sizes the request's read buffer for the largest range
// in the request, up to checksumReadBufferSize, so a request of small files
// needs only a small read buffer.
func checksumBufferSize(records []checksumRequestRecord) int {
	var largest int64
	for _, r := range records {
		if r.Flags&checksumHasSize == 0 {
			return int(checksumReadBufferSize)
		}
		largest = max(largest, r.Size)
	}
	return bufpool.BucketSize(min(max(largest, 1), checksumReadBufferSize))
}

type checksumScratch struct {
	buf     []byte
	line    *[]byte
	hashers checksumHashers
}

func streamChecksumItem(ctx context.Context, out io.Writer, deps Deps, fileRef FileRef, item cxsumItem, scratch *checksumScratch) error {
	fd, err := deps.OpenFileRef(fileRef)
	if err != nil {
		return mapLookupError(err)
	}

	fileInfo, statErr := fd.Stat()
	if statErr != nil {
		_ = fd.Close()
		return protocolErr{code: "INTERNAL", message: "failed to stat file"}
	}
	fileSize := fileInfo.Size()
	if item.Offset > fileSize {
		_ = fd.Close()
		return protocolErr{code: "BAD_REQUEST", message: "checksum offset beyond eof"}
	}
	rangeSize := fileSize - item.Offset
	if item.HasSize {
		if item.Size > fileSize-item.Offset {
			_ = fd.Close()
			return protocolErr{code: "BAD_REQUEST", message: "checksum range beyond eof"}
		}
		rangeSize = item.Size
	}
	fileHashes, hashErr := hashChecksumRange(benchio.WithReadLatency(ctx, "server", fd), item.Offset, rangeSize, item.Algorithms, scratch.buf, &scratch.hashers)
	_ = fd.Close()
	if hashErr != nil {
		return protocolErr{code: "INTERNAL", message: "failed to checksum file range"}
	}

	metadata := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
	*scratch.line = appendChecksumFrame((*scratch.line)[:0], item.FileID, item.Offset, rangeSize, fileHashes, time.Now().UnixMilli(), &metadata)
	_, err = out.Write(*scratch.line)
	return err
}

// appendChecksumFrame appends a CXSUM frame, a header and its terminal
// trailer with no payload between them, as encoding.WriteFrame formats it.
// With no payload, the frame hash covers exactly the header line and the
// trailer prefix, which sit side by side in dst.
func appendChecksumFrame(dst []byte, fileID uint64, offset, size int64, fileHashes []string, ts int64, metadata *encoding.FileFrameMetadata) []byte {
	start := len(dst)
	headerHash := "none:0"
	if len(fileHashes) > 0 {
		headerHash = fileHashes[0]
	}
	dst = append(dst, "FX/1 "...)
	dst = strconv.AppendUint(dst, fileID, 10)
	dst = append(dst, " offset="...)
	dst = strconv.AppendInt(dst, offset, 10)
	dst = append(dst, " size="...)
	dst = strconv.AppendInt(dst, size, 10)
	dst = append(dst, " wsize=0 comp=none hash="...)
	dst = append(dst, headerHash...)
	dst = append(dst, " ts="...)
	dst = strconv.AppendInt(dst, ts, 10)
	dst = append(dst, "\nFXT/1 "...)
	dst = strconv.AppendUint(dst, fileID, 10)
	dst = append(dst, " status=ok ts="...)
	dst = strconv.AppendInt(dst, ts, 10)
	for _, token := range fileHashes {
		if token != "" {
			dst = append(dst, " file-hash="...)
			dst = append(dst, token...)
		}
	}
	dst = append(dst, " next=0"...)
	if metadata != nil {
		dst = appendMetadataTrailerTokens(dst, metadata)
	}
	var frameHash [8]byte
	binary.BigEndian.PutUint64(frameHash[:], xxh3.Hash(dst[start:]))
	dst = append(dst, " hash=xxh64:"...)
	dst = hex.AppendEncode(dst, frameHash[:])
	return append(dst, '\n')
}

// checksumHashers holds the hashers one CXSUM request reuses for every
// range: each is about 1 KiB, too much to allocate per item. A hasher is
// created on first use, so a request asks only for what it hashes.
type checksumHashers struct {
	h128 *xxh3.Hasher128
	h64  *xxh3.Hasher
}

// hashChecksumRange hashes the range with only the requested algorithms, and
// reads nothing when none is requested.
func hashChecksumRange(fd io.ReaderAt, offset int64, size int64, algorithms []string, buf []byte, h *checksumHashers) ([]string, error) {
	var full128 *xxh3.Hasher128
	var full64 *xxh3.Hasher
	for _, name := range algorithms {
		switch name {
		case "xxh128":
			if h.h128 == nil {
				h.h128 = xxh3.New128()
			}
			full128 = h.h128
			full128.Reset()
		case "xxh64":
			if h.h64 == nil {
				h.h64 = xxh3.New()
			}
			full64 = h.h64
			full64.Reset()
		}
	}
	if size == 0 || (full128 == nil && full64 == nil) {
		return finalChecksumTokens(algorithms, full128, full64), nil
	}
	if len(buf) == 0 {
		pooled, release, err := bufpool.Acquire(int(checksumReadBufferSize))
		if err != nil {
			return nil, err
		}
		defer release()
		buf = pooled
	}
	reader := io.NewSectionReader(fd, offset, size)
	remaining := size
	for remaining > 0 {
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		n, err := io.ReadFull(reader, buf[:chunk])
		if n > 0 {
			part := buf[:n]
			if full128 != nil {
				_, _ = full128.Write(part)
			}
			if full64 != nil {
				_, _ = full64.Write(part)
			}
			remaining -= int64(n)
		}
		if err == nil {
			continue
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		return nil, err
	}
	if remaining != 0 {
		return nil, fmt.Errorf("short checksum read")
	}
	return finalChecksumTokens(algorithms, full128, full64), nil
}

func parseRequestedChecksums(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return []string{"xxh128"}, nil
	}
	set := make(map[string]struct{})
	for _, token := range raw {
		for _, part := range strings.Split(token, ",") {
			name := strings.ToLower(strings.TrimSpace(part))
			if name == "" {
				continue
			}
			switch name {
			case "none", "xxh128", "xxh64":
				set[name] = struct{}{}
			default:
				return nil, fmt.Errorf("unsupported checksum")
			}
		}
	}
	if len(set) == 0 {
		return []string{"xxh128"}, nil
	}
	if _, ok := set["none"]; ok && len(set) == 1 {
		return nil, nil
	}
	delete(set, "none")

	out := make([]string, 0, len(set))
	if _, ok := set["xxh128"]; ok {
		out = append(out, "xxh128")
	}
	if _, ok := set["xxh64"]; ok {
		out = append(out, "xxh64")
	}
	return out, nil
}

func finalChecksumTokens(algorithms []string, full128 *xxh3.Hasher128, full64 *xxh3.Hasher) []string {
	tokens := make([]string, 0, len(algorithms))
	for _, name := range algorithms {
		switch name {
		case "xxh128":
			tokens = append(tokens, encoding.FormatXXH128HashToken(full128.Sum128()))
		case "xxh64":
			tokens = append(tokens, encoding.FormatXXH64HashToken(full64.Sum64()))
		}
	}
	return tokens
}
