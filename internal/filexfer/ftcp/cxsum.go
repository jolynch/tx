package ftcp

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

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
func handleCXSUMWithInput(_ context.Context, req Request, in io.Reader, out io.Writer, deps Deps) error {
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

	buf, release, err := bufpool.Acquire(int(checksumReadBufferSize))
	if err != nil {
		return err
	}
	defer release()

	for _, record := range records {
		if err := streamChecksumItem(out, deps, txferID, record.item(), buf); err != nil {
			return err
		}
	}
	return nil
}

func streamChecksumItem(out io.Writer, deps Deps, txferID string, item cxsumItem, buf []byte) error {
	fd, fileRef, err := deps.GetFile(txferID, item.FileID, item.Path)
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
		if item.Offset+item.Size > fileSize {
			_ = fd.Close()
			return protocolErr{code: "BAD_REQUEST", message: "checksum range beyond eof"}
		}
		rangeSize = item.Size
	}
	fileHashes, hashErr := hashChecksumRange(fd, item.Offset, rangeSize, item.Algorithms, buf)
	_ = fd.Close()
	if hashErr != nil {
		return protocolErr{code: "INTERNAL", message: "failed to checksum file range"}
	}

	headerHash := "none:0"
	if len(fileHashes) > 0 {
		headerHash = fileHashes[0]
	}
	metadata := encoding.CollectFileFrameMetadata(fileRef.Path, fileInfo)
	headerTS := time.Now().UnixMilli()
	trailerTS := time.Now().UnixMilli()
	_, err = encoding.WriteFrame(out, encoding.WriteArgs{
		FileID:     item.FileID,
		Offset:     item.Offset,
		Size:       rangeSize,
		WSize:      0,
		Comp:       "none",
		HeaderHash: headerHash,
		HeaderTS:   headerTS,
		TrailerTS:  trailerTS,
		FileHashes: fileHashes,
		Next:       0,
		Metadata:   &metadata,
	})
	return err
}

func hashChecksumRange(fd io.ReaderAt, offset int64, size int64, algorithms []string, buf []byte) ([]string, error) {
	full128 := xxh3.New128()
	full64 := xxh3.New()
	if size == 0 {
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
			_, _ = full128.Write(part)
			_, _ = full64.Write(part)
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
