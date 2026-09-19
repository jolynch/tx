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

type validatedAckItem struct {
	item         ackItem
	ackBytes     int64
	ackTS        int64
	ackHashToken string
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

// handleACKWithInput applies a batch of acks atomically: every item is validated
// before any is applied, so a bad token in the middle of a batch cannot leave
// the transfer half-acked.
func handleACKWithInput(ctx context.Context, req Request, in io.Reader, out io.Writer, deps Deps) error {
	txferID, err := ackTransferID(req)
	if err != nil {
		return err
	}

	rawItems, err := readItemBody(in, ackItemKeys, "ACK")
	if err != nil {
		return err
	}
	if len(rawItems) == 0 {
		return protocolErr{code: "BAD_REQUEST", message: "ACK requires at least one item"}
	}
	items := make([]ackItem, 0, len(rawItems))
	for _, raw := range rawItems {
		item, itemErr := parseACKItem(raw, txferID)
		if itemErr != nil {
			return itemErr
		}
		items = append(items, item)
	}

	validated := make([]validatedAckItem, 0, len(items))
	for _, item := range items {
		ackBytes, ackTS, ackHashToken, ackProvided, err := parseAckToken(item.AckToken)
		if err != nil || !ackProvided {
			return protocolErr{code: "BAD_REQUEST", message: "invalid ack token"}
		}
		if ackBytes == -1 {
			item.DeltaBytes, item.RecvMS, item.SyncMS = 0, 0, 0
		}

		fileRef, err := deps.GetFileRef(item.TransferID, item.FileID, item.Path)
		if err != nil {
			return mapLookupError(err)
		}

		maxAck := fileRef.FileSize
		ackTarget := ackBytes
		if ackTarget > maxAck {
			ackTarget = maxAck
		}
		if ackTarget < 0 {
			ackTarget = 0
		}
		if ackBytes >= 0 {
			if ackHashToken == "" {
				return protocolErr{code: "BAD_REQUEST", message: "missing window ack hash token"}
			}
			if !deps.VerifyTransferFileWindowHash(item.TransferID, item.FileID, ackTarget, ackHashToken) {
				return protocolErr{code: "CONFLICT", message: "window ack hash token mismatch"}
			}
		}
		validated = append(validated, validatedAckItem{
			item:         item,
			ackBytes:     ackBytes,
			ackTS:        ackTS,
			ackHashToken: ackHashToken,
		})
	}

	for _, v := range validated {
		_, ackTask := trace.NewTask(ctx, "ack")
		if ok := deps.AcknowledgeTransferFile(v.item.TransferID, v.item.FileID, v.ackBytes); !ok {
			ackTask.End()
			return protocolErr{code: "INTERNAL", message: "failed to acknowledge file progress"}
		}
		if v.ackBytes >= 0 {
			deps.MaybeLogTransferProgress(v.item.TransferID)
		}
		deps.MaybeLogTransferComplete(v.item.TransferID)
		ackTask.End()
	}
	return writeOKLine(out, "")
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
