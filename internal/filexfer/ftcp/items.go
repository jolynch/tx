package ftcp

import (
	"bytes"
	"io"
	"strings"

	"github.com/jolynch/tx/internal/bufpool"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/utils"
)

// maxItemLineBytes bounds one item line in a framed request body. An item is a
// file id, one path, and a handful of short tokens; PATH_MAX is 4096, so this
// is roughly double the largest legal item.
const maxItemLineBytes = 8 * 1024

// itemKeys is the set of per-item option keys a verb accepts. Anything else is
// ignored for forward compatibility, matching the old inline behavior.
var (
	sendItemKeys  = map[string]bool{"offset": true, "size": true, "comp": true}
	ackItemKeys   = map[string]bool{"ack-token": true, "delta-bytes": true, "recv-ms": true, "sync-ms": true}
	cxsumItemKeys = map[string]bool{"offset": true, "size": true, "algo": true}
)

// ReadRequestItems collects maps for test harnesses. Production handlers use
// parseRequestItemRecords to retain compact records instead.
func ReadRequestItems(in io.Reader, verb Verb) ([]map[string]string, error) {
	allowed, name, err := requestItemKind(verb)
	if err != nil {
		return nil, err
	}
	body, err := readRequestItemBody(in, name)
	if err != nil {
		return nil, err
	}
	defer body.Release()
	items := make([]map[string]string, 0)
	err = visitItemBody(body.Bytes(), allowed, name, func(item map[string]string) error {
		items = append(items, item)
		return nil
	})
	return items, err
}

func requestItemKind(verb Verb) (map[string]bool, string, error) {
	switch verb {
	case VerbSEND:
		return sendItemKeys, "SEND", nil
	case VerbACK:
		return ackItemKeys, "ACK", nil
	case VerbCXSUM:
		return cxsumItemKeys, "CXSUM", nil
	default:
		return nil, "", protocolErr{code: "BAD_COMMAND", message: "verb has no request body"}
	}
}

// VerbHasRequestItems reports whether verb carries a framed item body.
func VerbHasRequestItems(verb Verb) bool {
	return verb == VerbSEND || verb == VerbACK || verb == VerbCXSUM
}

// readRequestItemBody drains and verifies a framed request into one bounded
// pooled buffer before any item is applied.
func readRequestItemBody(in io.Reader, verb string) (*bufpool.Growing, error) {
	if in == nil {
		return nil, protocolErr{code: "BAD_REQUEST", message: "missing " + verb + " request body"}
	}
	body, err := bufpool.NewGrowing(0)
	if err != nil {
		return nil, err
	}
	framed := encoding.NewFramedBodyReader(in, encoding.FramedBodyReaderOpts{
		MaxLogicalBytes: encoding.MaxRequestBytes,
	})
	// ReadFrom returns only on verified terminal-frame completion: a stream
	// that ends early surfaces as ErrUnexpectedEOF from the framing layer
	// rather than a clean EOF, so a truncated body cannot read as a short but
	// complete item list.
	if _, err := body.ReadFrom(framed); err != nil {
		body.Release()
		return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " request body: " + err.Error()}
	}
	return body, nil
}

// parseRequestItemRecords reads, verifies, and parses a request body once.
// Records must own their strings: the decoded body is released before return.
// A cheap line count sizes the slice without retaining temporary maps.
func parseRequestItemRecords[T any](in io.Reader, allowed map[string]bool, verb string, parse func(map[string]string) (T, error)) ([]T, error) {
	body, err := readRequestItemBody(in, verb)
	if err != nil {
		return nil, err
	}
	defer body.Release()

	count, err := countRequestItemLines(body.Bytes(), verb)
	if err != nil {
		return nil, err
	}
	records := make([]T, 0, count)
	err = visitItemBody(body.Bytes(), allowed, verb, func(item map[string]string) error {
		record, err := parse(item)
		if err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// Count nonblank lines and reject impossible sizes before reserving records.
func countRequestItemLines(body []byte, verb string) (int, error) {
	count := 0
	for len(body) > 0 {
		end := bytes.IndexByte(body, '\n')
		var line []byte
		if end < 0 {
			line, body = body, nil
		} else {
			line, body = body[:end+1], body[end+1:]
		}
		if len(line) > maxItemLineBytes {
			return 0, protocolErr{code: "BAD_REQUEST", message: verb + " item line too large"}
		}
		trimmed := utils.TrimLineTerminator(line)
		if len(trimmed) == 0 {
			continue
		}
		// Even the shortest valid item needs a file ID and nonempty path.
		if len(trimmed) < len("fd=0 1:x") {
			return 0, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " item"}
		}
		count++
	}
	return count, nil
}

// visitItemBody parses each line in body without retaining individual items.
// Blank lines still consume the request's byte budget.
func visitItemBody(body []byte, allowed map[string]bool, verb string, visit func(map[string]string) error) error {
	for len(body) > 0 {
		end := bytes.IndexByte(body, '\n')
		var line []byte
		if end < 0 {
			line, body = body, nil
		} else {
			line, body = body[:end+1], body[end+1:]
		}
		if len(line) > maxItemLineBytes {
			return protocolErr{code: "BAD_REQUEST", message: verb + " item line too large"}
		}
		if trimmed := utils.TrimLineTerminator(line); len(trimmed) > 0 {
			item, err := parseItemLine(trimmed, allowed, verb)
			if err != nil {
				return err
			}
			if err := visit(item); err != nil {
				return err
			}
		}
	}
	return nil
}

// parseItemLine parses one `fd=<id> <path> [key=value...]` item from a framed
// request body. It is the single item grammar: SEND, ACK, and CXSUM differ only
// in which option keys they keep.
func parseItemLine(line []byte, allowed map[string]bool, verb string) (map[string]string, error) {
	c := newCursor(line)
	if c.eof() {
		return nil, protocolErr{code: "BAD_REQUEST", message: "empty " + verb + " item"}
	}
	fdToken, err := c.readToken()
	if err != nil || !strings.HasPrefix(fdToken, "fd=") {
		return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " file id"}
	}
	fid := strings.TrimSpace(strings.TrimPrefix(fdToken, "fd="))
	if fid == "" {
		return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " file id"}
	}
	path, err := c.readPathValue()
	if err != nil {
		return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " path"}
	}
	item := map[string]string{"fid": fid, "path": string(path)}
	for !c.eof() {
		tok, tokErr := c.readToken()
		if tokErr != nil {
			return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " item option"}
		}
		key, val, ok := strings.Cut(tok, "=")
		if !ok {
			return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " item option"}
		}
		if allowed[key] {
			item[key] = val
		}
	}
	return item, nil
}

// drainItemBody lets a peer finish writing before receiving a header error.
// The caller retains that error if draining fails.
func drainItemBody(in io.Reader) {
	if in == nil {
		return
	}
	_, _ = io.Copy(io.Discard, encoding.NewFramedBodyReader(in, encoding.FramedBodyReaderOpts{
		MaxLogicalBytes: encoding.MaxRequestBytes,
	}))
}
