package ftcp

import (
	"bufio"
	"errors"
	"io"
	"strings"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/utils"
)

// maxItemLineBytes bounds one item line in a framed request body. An item is a
// file id, one path, and a handful of short tokens; PATH_MAX is 4096, so this
// is roughly double the largest legal item.
const maxItemLineBytes = 8 * 1024

// maxBodyItems bounds how many items one framed request body may carry.
//
// Every verb reads its whole item list before writing any response, so the list
// is held in memory. That is not an implementation shortcut but a deadlock
// requirement: a server that answered items as it read them would be writing
// response bytes while the client was still writing request bytes, and once
// both socket buffers filled neither side could make progress. Draining the
// request first is what keeps the exchange strictly request-then-response.
//
// An item is ~100 bytes, so this bound is about 100 MiB of item lines for a
// tree of a million files — far past any real batch, which is the point: the
// limit exists to stop a hostile peer, not to shape legitimate requests.
const maxBodyItems = 1 << 20

// maxItemBodyBytes is the largest legal item list in decoded bytes.
const maxItemBodyBytes int64 = maxBodyItems * maxItemLineBytes

// itemKeys is the set of per-item option keys a verb accepts. Anything else is
// ignored for forward compatibility, matching the old inline behavior.
var (
	sendItemKeys  = map[string]bool{"offset": true, "size": true, "comp": true}
	ackItemKeys   = map[string]bool{"ack-token": true, "delta-bytes": true, "recv-ms": true, "sync-ms": true}
	cxsumItemKeys = map[string]bool{"offset": true, "size": true, "algo": true}
)

// ReadRequestItems reads the framed request body for verb and returns its item
// list. It is the parsing half of the request grammar, exported so that code
// standing in for a server (test harnesses, tools) decodes requests with the
// real parser instead of a second implementation that can drift from it.
func ReadRequestItems(in io.Reader, verb Verb) ([]map[string]string, error) {
	switch verb {
	case VerbSEND:
		return readItemBody(in, sendItemKeys, "SEND")
	case VerbACK:
		return readItemBody(in, ackItemKeys, "ACK")
	case VerbCXSUM:
		return readItemBody(in, cxsumItemKeys, "CXSUM")
	default:
		return nil, protocolErr{code: "BAD_COMMAND", message: "verb has no request body"}
	}
}

// VerbHasRequestItems reports whether verb carries a framed item body.
func VerbHasRequestItems(verb Verb) bool {
	return verb == VerbSEND || verb == VerbACK || verb == VerbCXSUM
}

// parseItemLine parses one `fd=<id> <path> [key=value...]` item from a framed
// request body. It is the single item grammar: SEND, ACK, and CXSUM differ only
// in which option keys they keep.
//
// The grammar is unchanged from when these items rode the command line — the
// same cursor handles `fd=`, quoted or length-prefixed paths, and key=value
// tokens — so moving them into a body did not fork the parser.
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
		// Unknown keys are ignored for forward compatibility.
	}
	return item, nil
}

// readItemBody reads a framed request body in full and returns its item list.
//
// The body is an FX/1 + FXT/1 stream (file_id=0) carrying one item per line, so
// a request's size is bounded by the transfer rather than by any line budget.
// The whole body is consumed before returning — see maxBodyItems for why the
// caller must not start responding earlier.
func readItemBody(in io.Reader, allowed map[string]bool, verb string) ([]map[string]string, error) {
	if in == nil {
		return nil, protocolErr{code: "BAD_REQUEST", message: "missing " + verb + " request body"}
	}
	// The byte cap also bounds input that produces no items.
	br := bufio.NewReader(encoding.NewFramedBodyReader(in, encoding.FramedBodyReaderOpts{
		MaxLogicalBytes: maxItemBodyBytes,
	}))

	var items []map[string]string
	// Count blank lines so they cannot bypass the item limit.
	lines := 0

	for {
		line, err := utils.ReadLineLimit(br, maxItemLineBytes)
		if err != nil && !errors.Is(err, io.EOF) {
			if errors.Is(err, utils.ErrLineTooLarge) {
				return nil, protocolErr{code: "BAD_REQUEST", message: verb + " item line too large"}
			}
			return nil, protocolErr{code: "BAD_REQUEST", message: "invalid " + verb + " request body: " + err.Error()}
		}
		lines++
		if lines > maxBodyItems {
			return nil, protocolErr{code: "BAD_REQUEST", message: "too many " + verb + " items"}
		}
		// A final item without a trailing newline is still a complete item.
		if trimmed := utils.TrimLineTerminator(line); len(trimmed) > 0 {
			item, parseErr := parseItemLine(trimmed, allowed, verb)
			if parseErr != nil {
				return nil, parseErr
			}
			items = append(items, item)
		}
		if errors.Is(err, io.EOF) {
			return items, nil
		}
	}
}

// drainItemBody lets a peer finish writing before receiving a header error.
// The caller retains that error if draining fails.
func drainItemBody(in io.Reader) {
	if in == nil {
		return
	}
	_, _ = io.Copy(io.Discard, encoding.NewFramedBodyReader(in, encoding.FramedBodyReaderOpts{
		MaxLogicalBytes: maxItemBodyBytes,
	}))
}
