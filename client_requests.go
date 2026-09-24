package tx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	intencoding "github.com/jolynch/tx/internal/filexfer/encoding"
)

// requestChunk borrows one encoded body until the visitor returns.
type requestChunk struct {
	lo, hi int
	encodedRequest
}

// encodedRequest keeps the budget captured during construction with its bytes.
type encodedRequest struct {
	body     []byte
	maxBytes int64
}

func (r encodedRequest) validate() error {
	if r.maxBytes <= 0 || r.maxBytes > intencoding.MaxRequestBytes {
		return errors.New("invalid request byte budget")
	}
	if int64(len(r.body)) > r.maxBytes {
		return fmt.Errorf("request metadata exceeds maximum %d bytes", r.maxBytes)
	}
	return nil
}

// visitEncodedRequests builds and consumes one bounded request at a time.
func visitEncodedRequests[T any](items []T, limits clientRequestLimits, encode func(T) ([]byte, error), visit func(requestChunk) error) error {
	if len(items) == 0 {
		return errors.New("no request items")
	}
	var body []byte
	lo := 0
	flush := func(hi int) error {
		if err := visit(requestChunk{lo: lo, hi: hi, encodedRequest: encodedRequest{body: body, maxBytes: limits.max}}); err != nil {
			return err
		}
		body, lo = body[:0], hi
		return nil
	}
	for i, item := range items {
		encoded, err := encode(item)
		if err != nil {
			return err
		}
		if int64(len(encoded)) > limits.max {
			return fmt.Errorf("request item metadata is %d bytes, exceeding maximum %d", len(encoded), limits.max)
		}
		if len(body) > 0 && int64(len(encoded)) > limits.max-int64(len(body)) {
			if err := flush(i); err != nil {
				return err
			}
		}
		body = append(body, encoded...)
		if int64(len(body)) >= limits.target {
			if err := flush(i + 1); err != nil {
				return err
			}
		}
	}
	if len(body) > 0 {
		return flush(len(items))
	}
	return nil
}

// encodeRequest rejects oversized single requests before dialing or retaining
// more than their budget. It shares the batching path's item encoders.
func encodeRequest[T any](items []T, maxBytes int64, encode func(T) ([]byte, error)) (encodedRequest, error) {
	if len(items) == 0 {
		return encodedRequest{}, errors.New("no request items")
	}
	var body []byte
	for _, item := range items {
		encoded, err := encode(item)
		if err != nil {
			return encodedRequest{}, err
		}
		if int64(len(encoded)) > maxBytes-int64(len(body)) {
			return encodedRequest{}, fmt.Errorf("request metadata exceeds maximum %d bytes", maxBytes)
		}
		body = append(body, encoded...)
	}
	return encodedRequest{body: body, maxBytes: maxBytes}, nil
}

func fetchTargetItemBytes(target FetchFileTarget) ([]byte, error) {
	var item itemWriter
	item.begin(target.FileID, target.FullPath)
	if target.Comp != "" {
		item.field("comp", target.Comp)
	}
	if target.Offset != 0 {
		item.fieldInt("offset", target.Offset)
	}
	if target.Size > 0 {
		item.fieldInt("size", target.Size)
	}
	return item.bytes()
}

func checksumTargetItemBytes(target ChecksumTarget) ([]byte, error) {
	var item itemWriter
	item.begin(target.FileID, target.FullPath)
	if target.Offset > 0 {
		item.fieldInt("offset", target.Offset)
	}
	if target.Size > 0 {
		item.fieldInt("size", target.Size)
	}
	if algo := strings.TrimSpace(target.Algo); algo != "" {
		item.field("algo", algo)
	}
	return item.bytes()
}

func acknowledgeItemBytes(command acknowledgeFileProgressCommand) ([]byte, error) {
	request := command.request
	var item itemWriter
	item.begin(request.FileID, request.FullPath)
	item.field("ack-token", command.ackToken)
	if request.AckBytes >= 0 {
		item.fieldInt("delta-bytes", request.DeltaBytes)
		item.fieldInt("recv-ms", request.RecvMS)
		item.fieldInt("sync-ms", request.SyncMS)
	}
	return item.bytes()
}

// ChecksumBatchOptions can tighten the advertised target and bound each request.
// Zero values use the advertised target and the parent context's deadline.
type ChecksumBatchOptions struct {
	TargetRequestBytes int64
	RequestTimeout     time.Duration
}

// VisitChecksumBatches sends ordered, bounded CXSUM requests without re-encoding
// targets. consume must read each response synchronously; its targets and reader
// must not be retained or modified. The client closes the reader after consume,
// and stops on the first request or callback error. Earlier batches stay complete.
func (c *Client) VisitChecksumBatches(ctx context.Context, request GetChecksumRequest, options ChecksumBatchOptions, consume func([]ChecksumTarget, GetChecksumResponse) error) error {
	if c == nil {
		return errors.New("nil client")
	}
	limits := c.currentRequestLimits()
	if err := validateChecksumRequest(request); err != nil {
		return err
	}
	if options.TargetRequestBytes < 0 || options.RequestTimeout < 0 {
		return errors.New("negative checksum batch option")
	}
	if consume == nil {
		return errors.New("missing checksum response consumer")
	}
	if options.TargetRequestBytes > 0 {
		limits.target = min(limits.target, options.TargetRequestBytes)
	}
	return visitEncodedRequests(request.Targets, limits, checksumTargetItemBytes, func(chunk requestChunk) error {
		requestCtx := ctx
		if options.RequestTimeout > 0 {
			var cancel context.CancelFunc
			requestCtx, cancel = context.WithTimeout(ctx, options.RequestTimeout)
			defer cancel()
		}
		reader, err := c.getChecksumBodyTCP(requestCtx, request.TransferID, chunk.encodedRequest)
		if err != nil {
			return err
		}
		defer reader.Close()
		return consume(request.Targets[chunk.lo:chunk.hi], GetChecksumResponse{Reader: reader})
	})
}

func validateChecksumRequest(request GetChecksumRequest) error {
	if request.TransferID == "" {
		return errors.New("missing transfer id")
	}
	if len(request.Targets) == 0 {
		return errors.New("missing checksum targets")
	}
	for _, target := range request.Targets {
		if target.FullPath == "" {
			return errors.New("missing full path")
		}
		if target.Offset < 0 {
			return errors.New("invalid checksum offset")
		}
		if target.Size < 0 {
			return errors.New("invalid checksum size")
		}
	}
	return nil
}
