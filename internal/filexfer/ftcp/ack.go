package ftcp

import (
	"context"
	"fmt"
	"io"
	"runtime/trace"
	"strconv"
	"strings"

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
	FileID   uint64
	AckBytes int64
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
		ackBytes, err := validateACKItem(item, deps)
		return ackRequestRecord{FileID: item.FileID, AckBytes: ackBytes}, err
	})
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return protocolErr{code: "BAD_REQUEST", message: "ACK requires at least one item"}
	}
	for _, record := range records {
		_, task := trace.NewTask(ctx, "ack")
		ok := deps.AcknowledgeTransferFile(txferID, record.FileID, record.AckBytes)
		if ok {
			if record.AckBytes >= 0 {
				deps.MaybeLogTransferProgress(txferID)
			}
			deps.MaybeLogTransferComplete(txferID)
		}
		task.End()
		if !ok {
			return protocolErr{code: "INTERNAL", message: "failed to acknowledge file progress"}
		}
	}
	return writeOKLine(out, "")
}

// validateACKItem checks one item against the store without mutating it,
// returning the ack byte count the apply pass will use.
func validateACKItem(item ackItem, deps Deps) (int64, error) {
	ackBytes, _, ackHashToken, ackProvided, err := parseAckToken(item.AckToken)
	if err != nil || !ackProvided {
		return 0, protocolErr{code: "BAD_REQUEST", message: "invalid ack token"}
	}

	fileRef, err := deps.GetFileRef(item.TransferID, item.FileID, item.Path)
	if err != nil {
		return 0, mapLookupError(err)
	}

	ackTarget := min(ackBytes, fileRef.FileSize)
	if ackTarget < 0 {
		ackTarget = 0
	}
	if ackBytes >= 0 {
		if ackHashToken == "" {
			return 0, protocolErr{code: "BAD_REQUEST", message: "missing window ack hash token"}
		}
		if !deps.VerifyTransferFileWindowHash(item.TransferID, item.FileID, ackTarget, ackHashToken) {
			return 0, protocolErr{code: "CONFLICT", message: "window ack hash token mismatch"}
		}
	}
	return ackBytes, nil
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
