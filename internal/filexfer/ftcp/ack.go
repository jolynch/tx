package ftcp

import (
	"context"
	"fmt"
	"io"
	"runtime/trace"
	"strconv"
	"strings"

	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

type ackItem struct {
	TransferID string
	FileID     uint64
	AckToken   string
	Path       string

	// Receiver telemetry: how long the client spent receiving this window and
	// fsyncing it, plus the bytes it newly wrote. The server validates these
	// but does not yet act on them. They are the natural input for a
	// receiver-aware compression policy — CompressionPolicy.Decide currently
	// sees only the server's own PrepareLatency/WriteLatency from
	// frameStreamStats, so it can tell that compressing is expensive to send
	// but not that the receiver is the bottleneck. Kept on the wire so that
	// wiring stays available.
	DeltaBytes int64
	RecvMS     int64
	SyncMS     int64
}

// parseACKItem turns one framed body item into an ackItem.
func parseACKItem(p map[string]string, txferID string) (ackItem, error) {
	fid, err := strconv.ParseUint(p["fid"], 10, 64)
	if err != nil {
		return ackItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid file id"}
	}
	ackToken := p["ack-token"]
	if strings.TrimSpace(ackToken) == "" {
		return ackItem{}, protocolErr{code: "BAD_REQUEST", message: "missing ack token"}
	}
	deltaBytes, recvMS, syncMS, err := parseAckTelemetryFields(p["delta-bytes"], p["recv-ms"], p["sync-ms"])
	if err != nil {
		return ackItem{}, protocolErr{code: "BAD_REQUEST", message: "invalid ACK telemetry"}
	}
	path := p["path"]
	if path == "" {
		return ackItem{}, protocolErr{code: "BAD_REQUEST", message: "missing path"}
	}
	return ackItem{
		TransferID: txferID,
		FileID:     fid,
		AckToken:   ackToken,
		Path:       path,
		DeltaBytes: deltaBytes,
		RecvMS:     recvMS,
		SyncMS:     syncMS,
	}, nil
}

func ackTransferID(req Request) (string, error) {
	if req.Verb != VerbACK {
		return "", protocolErr{code: "BAD_COMMAND", message: "not ACK"}
	}
	if len(req.Params) != 1 {
		return "", protocolErr{code: "BAD_REQUEST", message: "invalid ACK arguments"}
	}
	txferID := strings.TrimSpace(req.Params[0]["txferid"])
	if txferID == "" {
		return "", protocolErr{code: "BAD_REQUEST", message: "missing transfer id"}
	}
	return txferID, nil
}

func handleACK(ctx context.Context, req Request, out io.Writer, deps Deps) error {
	return protocolErr{code: "INTERNAL", message: "ACK requires a request body, use handleACKWithInput"}
}

type ackRequestRecord struct {
	FileID     uint64
	AckBytes   int64
	DeltaBytes int64
	Path       string
	HashToken  string
}

// Validate every acknowledgment before applying any. This does not lock the
// store against concurrent requests or make multiple requests transactional.
func handleACKWithInput(ctx context.Context, req Request, in io.Reader, out io.Writer, deps Deps) error {
	txferID, err := ackTransferID(req)
	if err != nil {
		return err
	}
	records, err := parseRequestItemRecords(in, ackItemKeys, "ACK", func(raw map[string]string) (ackRequestRecord, error) {
		item, err := parseACKItem(raw, txferID)
		if err != nil {
			return ackRequestRecord{}, err
		}
		ackBytes, _, hashToken, provided, err := parseAckToken(item.AckToken)
		if err != nil || !provided {
			return ackRequestRecord{}, protocolErr{code: "BAD_REQUEST", message: "invalid ack token"}
		}
		return ackRequestRecord{FileID: item.FileID, AckBytes: ackBytes, DeltaBytes: item.DeltaBytes, Path: item.Path, HashToken: hashToken}, nil
	})
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return protocolErr{code: "BAD_REQUEST", message: "ACK requires at least one item"}
	}
	if err := validateACKRecords(txferID, records, deps); err != nil {
		return err
	}
	// Apply the whole request under one store lock, then check progress and
	// completion once: both take the transfer lock, so doing either per record
	// serializes every concurrent SEND behind each ACKed file.
	_, task := trace.NewTask(ctx, "ack")
	entries := make([]AckEntry, len(records))
	progressed := false
	for i, record := range records {
		entries[i] = AckEntry{TxferID: txferID, FileID: record.FileID, AckBytes: record.AckBytes}
		progressed = progressed || record.AckBytes >= 0
	}
	// Records that failed do not undo the rest, so check progress and
	// completion either way.
	ok := deps.AcknowledgeTransferFiles(entries)
	if progressed {
		deps.MaybeLogTransferProgress(txferID)
	}
	deps.MaybeLogTransferComplete(txferID)
	task.End()
	if !ok {
		return protocolErr{code: "INTERNAL", message: "failed to acknowledge file progress"}
	}
	if scope := events.FromContext(ctx); scope.Sink.Enabled() {
		var delta int64
		for _, r := range records {
			delta += max(r.DeltaBytes, 0)
		}
		scope.Sink.Emit("ack", txferID, events.F("conn", scope.Conn), events.F("files", len(records)), events.F("bytes", delta))
	}
	return writeOKLine(out, "")
}

// validateACKRecords checks every record against the store without mutating
// it, with one store call to resolve the files and one to check the hashes:
// the transfer lock is shared by every connection, so per-record calls would
// serialize them. Parsing has already rejected any malformed item; of the
// store checks, it reports the first failing record's error, in order.
func validateACKRecords(txferID string, records []ackRequestRecord, deps Deps) error {
	lookups := make([]FileLookup, len(records))
	for i, r := range records {
		lookups[i] = FileLookup{FileID: r.FileID, Path: r.Path}
	}
	refs, errs := deps.GetFileRefs(txferID, lookups)
	hashes := make([]WindowHash, 0, len(records))
	for i, r := range records {
		if errs[i] == nil && r.AckBytes >= 0 && r.HashToken != "" {
			end := max(min(r.AckBytes, refs[i].FileSize), 0)
			hashes = append(hashes, WindowHash{FileID: r.FileID, EndBytes: end, HashToken: r.HashToken})
		}
	}
	verified := deps.VerifyTransferFileWindowHashes(txferID, hashes)
	next := 0
	for i, r := range records {
		if errs[i] != nil {
			return mapLookupError(errs[i])
		}
		if r.AckBytes < 0 {
			continue
		}
		if r.HashToken == "" {
			return protocolErr{code: "BAD_REQUEST", message: "missing window ack hash token"}
		}
		if !verified[next] {
			return protocolErr{code: "CONFLICT", message: "window ack hash token mismatch"}
		}
		next++
	}
	return nil
}

func parseAckToken(raw string) (ackBytes int64, ackTS int64, ackHashToken string, provided bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, "", false, nil
	}
	if raw == "-1" {
		return -1, 0, "", true, nil
	}
	parts := strings.SplitN(raw, "@", 3)
	if len(parts) < 2 {
		return 0, 0, "", true, fmt.Errorf("invalid ack format")
	}
	ackBytes, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ackBytes < 0 {
		return 0, 0, "", true, fmt.Errorf("invalid ack bytes")
	}
	ackTS, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || ackTS < 0 {
		return 0, 0, "", true, fmt.Errorf("invalid ack timestamp")
	}
	if len(parts) == 3 {
		ackHashToken = strings.TrimSpace(parts[2])
		if !encoding.ValidHashToken(ackHashToken) {
			return 0, 0, "", true, fmt.Errorf("invalid ack hash token")
		}
	}
	return ackBytes, ackTS, ackHashToken, true, nil
}

func parseAckTelemetryFields(deltaRaw string, recvRaw string, syncRaw string) (deltaBytes int64, recvMS int64, syncMS int64, err error) {
	deltaRaw = strings.TrimSpace(deltaRaw)
	recvRaw = strings.TrimSpace(recvRaw)
	syncRaw = strings.TrimSpace(syncRaw)
	if deltaRaw == "" {
		deltaBytes = 0
	} else {
		deltaBytes, err = strconv.ParseInt(deltaRaw, 10, 64)
		if err != nil || deltaBytes < 0 {
			return 0, 0, 0, fmt.Errorf("invalid delta-bytes")
		}
	}
	if recvRaw == "" {
		recvMS = 0
	} else {
		recvMS, err = strconv.ParseInt(recvRaw, 10, 64)
		if err != nil || recvMS < 0 {
			return 0, 0, 0, fmt.Errorf("invalid recv-ms")
		}
	}
	if syncRaw == "" {
		syncMS = 0
	} else {
		syncMS, err = strconv.ParseInt(syncRaw, 10, 64)
		if err != nil || syncMS < 0 {
			return 0, 0, 0, fmt.Errorf("invalid sync-ms")
		}
	}
	return deltaBytes, recvMS, syncMS, nil
}
