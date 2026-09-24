package ftcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jolynch/tx/internal/filexfer/encoding"
)

func TestParseRequestSENDHeaderOnly(t *testing.T) {
	req, err := ParseRequest([]byte(`SEND tx1`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Verb != VerbSEND {
		t.Fatalf("verb=%v", req.Verb)
	}
	// Per-file items ride the framed body, so the command line yields exactly
	// one header param no matter how many files the request covers.
	if len(req.Params) != 1 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	if got := req.Params[0]["txferid"]; got != "tx1" {
		t.Fatalf("unexpected txferid: %q", got)
	}
}

func TestParseRequestSENDRejectsInlineItems(t *testing.T) {
	// The old inline form is no longer part of the grammar.
	if _, err := ParseRequest([]byte(`SEND tx1 fd=42 "/tmp/file.txt"`)); err == nil {
		t.Fatal("expected inline SEND items to be rejected")
	}
}

func TestParseRequestAUTHChaCha20(t *testing.T) {
	req, err := ParseRequest([]byte(`AUTH chacha20 abc123`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Verb != VerbAUTH {
		t.Fatalf("verb=%v", req.Verb)
	}
	if len(req.Params) != 1 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	if got := req.Params[0]["protocol"]; got != "chacha20" {
		t.Fatalf("unexpected protocol: %q", got)
	}
	if got := req.Params[0]["blob"]; got != "abc123" {
		t.Fatalf("unexpected blob: %q", got)
	}
}

func TestParseRequestAUTHRejectsUnexpectedArguments(t *testing.T) {
	if _, err := ParseRequest([]byte(`AUTH aes abc123 mode=chacha20`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseItemLineSENDMultipleItemsWithOptions(t *testing.T) {
	items, err := readItemBody(framedItemBody(t,
		`fd=42 "/tmp/a.txt" offset=10 size=20 comp=none foo=bar`,
		`fd=77 10:/tmp/b.txt size=99`,
	), sendItemKeys, "SEND")
	if err != nil {
		t.Fatalf("readItemBody err: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items len=%d", len(items))
	}
	first := items[0]
	if first["fid"] != "42" || first["path"] != "/tmp/a.txt" || first["offset"] != "10" || first["size"] != "20" || first["comp"] != "none" {
		t.Fatalf("unexpected first item: %#v", first)
	}
	if _, ok := first["foo"]; ok {
		t.Fatalf("unknown key should have been dropped: %#v", first)
	}
	second := items[1]
	if second["fid"] != "77" || second["path"] != "/tmp/b.txt" || second["size"] != "99" {
		t.Fatalf("unexpected second item: %#v", second)
	}
}

func TestParseItemLineSENDCompModes(t *testing.T) {
	for _, want := range []string{"adapt", "lz4", "zstd", "none"} {
		t.Run(want, func(t *testing.T) {
			items, err := readItemBody(framedItemBody(t, `fd=1 "/tmp/a.txt" comp=`+want), sendItemKeys, "SEND")
			if err != nil {
				t.Fatalf("readItemBody err: %v", err)
			}
			if len(items) != 1 {
				t.Fatalf("items len=%d", len(items))
			}
			if got := items[0]["comp"]; got != want {
				t.Fatalf("expected comp=%q got=%q", want, got)
			}
		})
	}
}

// mode is transfer-level and lives on the command line, so it is not an item key.
func TestParseRequestSENDMode(t *testing.T) {
	req, err := ParseRequest([]byte(`SEND tx1 mode=gentle`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if got := req.Params[0]["mode"]; got != "gentle" {
		t.Fatalf("expected mode=gentle got=%q", got)
	}
	items, err := readItemBody(framedItemBody(t, `fd=1 "/tmp/a.txt" mode=gentle`), sendItemKeys, "SEND")
	if err != nil {
		t.Fatalf("readItemBody err: %v", err)
	}
	if _, ok := items[0]["mode"]; ok {
		t.Fatalf("mode should not be accepted as an item key: %#v", items[0])
	}
}

func TestParseRequestACKHeaderOnly(t *testing.T) {
	req, err := ParseRequest([]byte(`ACK tx1`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Verb != VerbACK {
		t.Fatalf("verb=%v", req.Verb)
	}
	if len(req.Params) != 1 || req.Params[0]["txferid"] != "tx1" {
		t.Fatalf("unexpected params: %#v", req.Params)
	}
	if _, err := ParseRequest([]byte(`ACK tx1 fd=42 "/tmp/file.txt" ack-token=5@1001@xxh128:abc`)); err == nil {
		t.Fatal("expected inline ACK items to be rejected")
	}
}

func TestParseItemLineACKMultipleItemsWithTelemetry(t *testing.T) {
	items, err := readItemBody(framedItemBody(t,
		`fd=1 "/tmp/a.txt" ack-token=5@1001@xxh128:aaa delta-bytes=5 recv-ms=1 sync-ms=2 foo=bar`,
		`fd=2 10:/tmp/b.txt ack-token=-1`,
	), ackItemKeys, "ACK")
	if err != nil {
		t.Fatalf("readItemBody err: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items len=%d", len(items))
	}
	first := items[0]
	if first["fid"] != "1" || first["ack-token"] != "5@1001@xxh128:aaa" ||
		first["delta-bytes"] != "5" || first["recv-ms"] != "1" || first["sync-ms"] != "2" {
		t.Fatalf("unexpected first ACK item: %#v", first)
	}
	second := items[1]
	if second["fid"] != "2" || second["ack-token"] != "-1" || second["path"] != "/tmp/b.txt" {
		t.Fatalf("unexpected second ACK item: %#v", second)
	}
}

func TestParseItemLineCXSUMMultipleItemsWithOptions(t *testing.T) {
	req, err := ParseRequest([]byte(`CXSUM tx1`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Verb != VerbCXSUM {
		t.Fatalf("verb=%v", req.Verb)
	}
	if got := req.Params[0]["txferid"]; got != "tx1" {
		t.Fatalf("unexpected txferid: %q", got)
	}

	items, err := readItemBody(framedItemBody(t,
		`fd=1 "/tmp/a.txt" offset=10 size=20 algo=xxh64`,
		`fd=1 10:/tmp/a.txt size=8`,
	), cxsumItemKeys, "CXSUM")
	if err != nil {
		t.Fatalf("readItemBody err: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items len=%d", len(items))
	}
	first := items[0]
	if first["fid"] != "1" || first["path"] != "/tmp/a.txt" || first["offset"] != "10" || first["size"] != "20" || first["algo"] != "xxh64" {
		t.Fatalf("unexpected first CXSUM item: %#v", first)
	}
	second := items[1]
	if second["fid"] != "1" || second["path"] != "/tmp/a.txt" || second["size"] != "8" {
		t.Fatalf("unexpected second CXSUM item: %#v", second)
	}
}

func TestParseItemLineMalformedLenValue(t *testing.T) {
	if _, err := readItemBody(framedItemBody(t, `fd=1 10:/tmp`), sendItemKeys, "SEND"); err == nil {
		t.Fatal("expected error")
	} else if pe, ok := err.(protocolErr); !ok || pe.code != "BAD_REQUEST" {
		t.Fatalf("expected BAD_REQUEST protocolErr, got %v", err)
	}
}

func TestParseItemLineMissingBlockStart(t *testing.T) {
	if _, err := readItemBody(framedItemBody(t, `"/tmp/file.txt"`), sendItemKeys, "SEND"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseItemLineInvalidUnquotedPath(t *testing.T) {
	if _, err := readItemBody(framedItemBody(t, `fd=1 /tmp/plain`), sendItemKeys, "SEND"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseItemLineACKMissingAckToken(t *testing.T) {
	items, err := readItemBody(framedItemBody(t, `fd=1 "/tmp/a.txt"`), ackItemKeys, "ACK")
	if err != nil {
		t.Fatalf("readItemBody err: %v", err)
	}
	if got := items[0]["ack-token"]; got != "" {
		t.Fatalf("unexpected ack-token value: %q", got)
	}
}

func TestParseRequestUnknownVerb(t *testing.T) {
	_, err := ParseRequest([]byte("NOPE x"))
	if err == nil {
		t.Fatalf("expected error")
	}
	pe, ok := err.(protocolErr)
	if !ok {
		t.Fatalf("expected protocolErr got %T", err)
	}
	if pe.code != "BAD_COMMAND" {
		t.Fatalf("code=%s", pe.code)
	}
}

func TestParseRequestPROBE(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=1048576 cts0=100`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Verb != VerbPROBE {
		t.Fatalf("verb=%v", req.Verb)
	}
	if len(req.Params) != 1 {
		t.Fatalf("params len=%d", len(req.Params))
	}
	if req.Params[0]["cpu"] != "8" || req.Params[0]["probe-bytes"] != "1048576" || req.Params[0]["cts0"] != "100" {
		t.Fatalf("unexpected PROBE params: %#v", req.Params[0])
	}
}

func TestParseRequestPROBEOptionalFields(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=1048576 cts0=100 txferid=tx1 obs-link-mbps=900 gentle-cpu-pct=30 gentle-bw-pct=40`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	if req.Params[0]["txferid"] != "tx1" || req.Params[0]["obs-link-mbps"] != "900" || req.Params[0]["gentle-cpu-pct"] != "30" || req.Params[0]["gentle-bw-pct"] != "40" {
		t.Fatalf("unexpected optional PROBE params: %#v", req.Params[0])
	}
}

func TestParseRequestPROBEMissingRequired(t *testing.T) {
	_, err := ParseRequest([]byte(`PROBE cpu=8 cts0=100`))
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestHandleCommandBadVerb(t *testing.T) {
	s := &connSession{}
	err := s.handleCommand(context.Background(), Request{Verb: VerbUnknown}, nil, nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	pe, ok := err.(protocolErr)
	if !ok {
		t.Fatalf("expected protocolErr got %T", err)
	}
	if pe.code != "BAD_COMMAND" {
		t.Fatalf("code=%s", pe.code)
	}
}

// FuzzFramedItemRoundTrip pins the request grammar: whatever the client encodes
// into a framed body, the server must decode back to the same items. The
// interesting inputs are paths the old line-oriented form mangled — trailing
// whitespace, quotes, and other bytes the length prefix is supposed to protect.
func FuzzFramedItemRoundTrip(f *testing.F) {
	f.Add(uint64(1), "/tmp/a.txt", int64(0), int64(0), "")
	f.Add(uint64(42), "/remote/dir/file with spaces.txt", int64(10), int64(20), "zstd")
	f.Add(uint64(7), "/tmp/trailing ", int64(0), int64(5), "none")
	f.Add(uint64(9), `/tmp/quo"te`, int64(3), int64(0), "lz4")
	f.Add(uint64(0), "/tmp/emoji😀", int64(0), int64(0), "adapt")
	f.Add(uint64(3), "/tmp/tab\there", int64(1), int64(1), "")

	f.Fuzz(func(t *testing.T, fileID uint64, path string, offset int64, size int64, comp string) {
		if offset < 0 || size < 0 {
			t.Skip()
		}
		// The encoder rejects what the framing cannot carry; that is a valid
		// outcome and not a round-trip failure.
		if strings.TrimSpace(path) == "" || strings.ContainsAny(path, "\n\r") {
			t.Skip()
		}
		if strings.ContainsAny(comp, " \n\r") {
			t.Skip()
		}

		var body bytes.Buffer
		w := encoding.NewFramedBodyWriter(&body, encoding.EncodingZstd, encoding.DefaultBodyChunkSize, 0)
		line := "fd=" + strconv.FormatUint(fileID, 10) + " " + strconv.Itoa(len(path)) + ":" + path
		if offset != 0 {
			line += " offset=" + strconv.FormatInt(offset, 10)
		}
		if size > 0 {
			line += " size=" + strconv.FormatInt(size, 10)
		}
		if comp != "" {
			line += " comp=" + comp
		}
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			t.Fatalf("write item: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close body: %v", err)
		}
		// An item line past the cap is rejected by design, not a round trip.
		if len(line)+1 > maxItemLineBytes {
			if _, err := readItemBody(bytes.NewReader(body.Bytes()), sendItemKeys, "SEND"); err == nil {
				t.Fatal("oversized item line was accepted")
			}
			return
		}

		items, err := readItemBody(bytes.NewReader(body.Bytes()), sendItemKeys, "SEND")
		if err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if len(items) != 1 {
			t.Fatalf("got %d items, want 1 (line %q)", len(items), line)
		}
		got := items[0]
		if got["fid"] != strconv.FormatUint(fileID, 10) {
			t.Fatalf("fid round trip: got %q want %d", got["fid"], fileID)
		}
		if got["path"] != path {
			t.Fatalf("path round trip: got %q want %q", got["path"], path)
		}
		if offset != 0 && got["offset"] != strconv.FormatInt(offset, 10) {
			t.Fatalf("offset round trip: got %q want %d", got["offset"], offset)
		}
		if size > 0 && got["size"] != strconv.FormatInt(size, 10) {
			t.Fatalf("size round trip: got %q want %d", got["size"], size)
		}
		if comp != "" && got["comp"] != comp {
			t.Fatalf("comp round trip: got %q want %q", got["comp"], comp)
		}
		want, parseErr := parseSENDItem(got, loadStrategyFast)
		records, recordErr := parseRequestItemRecords(bytes.NewReader(body.Bytes()), sendItemKeys, "SEND", parseSENDRecord)
		if parseErr != nil {
			if recordErr == nil {
				t.Fatal("compact parser accepted an invalid SEND item")
			}
		} else if recordErr != nil || len(records) != 1 || records[0].item(loadStrategyFast) != want {
			t.Fatalf("compact round trip: records=%+v err=%v want=%+v", records, recordErr, want)
		}
	})
}

// The defect that motivated framing: a batch covering a whole tree used to be
// capped by the command line it was encoded into. A body far past that cap must
// now decode intact.
func TestFramedItemBodyFarPastCommandLineCap(t *testing.T) {
	const items = 60_000
	var body bytes.Buffer
	w := encoding.NewFramedBodyWriter(&body, encoding.EncodingZstd, encoding.DefaultBodyChunkSize, 0)
	var logical int
	for i := 0; i < items; i++ {
		path := fmt.Sprintf("/remote/package-%05d/nested-module-dir/index.js", i)
		line := fmt.Sprintf("fd=%d %d:%s comp=none\n", i+1, len(path), path)
		logical += len(line)
		if _, err := io.WriteString(w, line); err != nil {
			t.Fatalf("write item %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}
	if logical <= maxCommandLineBytes {
		t.Fatalf("test is not exercising the regression: %d logical bytes fits in a %d byte command line", logical, maxCommandLineBytes)
	}

	decoded, err := readItemBody(bytes.NewReader(body.Bytes()), sendItemKeys, "SEND")
	if err != nil {
		t.Fatalf("decode %d-item body (%d logical bytes): %v", items, logical, err)
	}
	if len(decoded) != items {
		t.Fatalf("got %d items, want %d", len(decoded), items)
	}
	if got := decoded[items-1]["path"]; got != fmt.Sprintf("/remote/package-%05d/nested-module-dir/index.js", items-1) {
		t.Fatalf("last item path: %q", got)
	}
	t.Logf("%d items = %d logical bytes, %d wire bytes (command-line cap is %d)",
		items, logical, body.Len(), maxCommandLineBytes)
}

func TestFramedItemBodyRejectsOversizedItemLine(t *testing.T) {
	huge := strings.Repeat("x", maxItemLineBytes)
	var body bytes.Buffer
	w := encoding.NewFramedBodyWriter(&body, encoding.EncodingZstd, encoding.DefaultBodyChunkSize, 0)
	if _, err := fmt.Fprintf(w, "fd=1 %d:/tmp/%s\n", len(huge)+5, "/tmp/"+huge); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := readItemBody(bytes.NewReader(body.Bytes()), sendItemKeys, "SEND"); err == nil {
		t.Fatal("expected an oversized item line to be rejected")
	}
}

// SEND, ACK, and CXSUM must reject oversized frame declarations.
func TestReadItemBodyRejectsHostileFrameHeader(t *testing.T) {
	for _, size := range []int64{9000000000000000000, math.MaxInt64, encoding.DefaultMaxFrameLogicalBytes() + 1} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			var body bytes.Buffer
			fmt.Fprintf(&body, "FX/1 0 offset=0 size=%d wsize=4 comp=zstd hash=xxh128:%032d ts=1\n", size, 0)
			body.WriteString("AAAA")
			fmt.Fprintf(&body, "FXT/1 0 status=ok ts=1 next=0 hash=xxh64:%016d\n", 0)

			for _, verb := range []struct {
				name    string
				allowed map[string]bool
			}{
				{"SEND", sendItemKeys}, {"ACK", ackItemKeys}, {"CXSUM", cxsumItemKeys},
			} {
				if _, err := readItemBody(bytes.NewReader(body.Bytes()), verb.allowed, verb.name); err == nil {
					t.Errorf("%s accepted a frame declaring %d logical bytes", verb.name, size)
				}
			}
		})
	}
}

// FuzzFramedBodyHeader varies frame declarations; parsing must return or
// reject them without panicking or allocating from an unbounded size.
func FuzzFramedBodyHeader(f *testing.F) {
	f.Add(int64(0), int64(10), int64(10), "zstd")
	f.Add(int64(0), int64(math.MaxInt64), int64(4), "zstd")
	f.Add(int64(0), int64(9000000000000000000), int64(4), "zstd")
	f.Add(int64(0), int64(-1), int64(4), "zstd")
	f.Add(int64(0), int64(10), int64(math.MaxInt64), "zstd")
	f.Add(int64(math.MaxInt64), int64(math.MaxInt64), int64(4), "zstd")
	f.Add(int64(0), int64(1<<40), int64(4), "none")
	f.Add(int64(0), int64(4), int64(4), "lz4")

	f.Fuzz(func(t *testing.T, offset int64, size int64, wsize int64, comp string) {
		if strings.ContainsAny(comp, " \n\r") {
			t.Skip()
		}
		var body bytes.Buffer
		fmt.Fprintf(&body, "FX/1 0 offset=%d size=%d wsize=%d comp=%s hash=xxh128:%032d ts=1\n",
			offset, size, wsize, comp, 0)
		body.WriteString("AAAA")
		fmt.Fprintf(&body, "FXT/1 0 status=ok ts=1 next=0 hash=xxh64:%016d\n", 0)

		_, _ = readItemBody(bytes.NewReader(body.Bytes()), sendItemKeys, "SEND")
	})
}

// A body that ends before its terminal frame must be rejected *by the framing
// layer*, not merely produce some error.
//
// The distinction is the whole test. Without the read-side EOF conversion the
// framing layer accepts a truncated body, and the only thing that fails is
// parsing whatever partial item line happens to be last — "invalid ACK path",
// "invalid ACK item option". That is detection by coincidence: a cut landing on
// an item-line boundary parses cleanly and the ACK applies a prefix. So this
// asserts the error names frame-level truncation, which an assertion of
// "err != nil" cannot distinguish.
func TestReadItemBodyRejectsTruncation(t *testing.T) {
	var full bytes.Buffer
	w := encoding.NewFramedBodyWriter(&full, encoding.EncodingZstd, 4096, 0)
	for i := 0; i < 400; i++ {
		if _, err := fmt.Fprintf(w, "fd=%d %d:/remote/path/to/file-%06d.bin ack-token=-1\n",
			i+1, 31, i); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	complete := full.Bytes()

	items, err := readItemBody(bytes.NewReader(complete), ackItemKeys, "ACK")
	if err != nil {
		t.Fatalf("complete body rejected: %v", err)
	}
	if len(items) != 400 {
		t.Fatalf("got %d items, want 400", len(items))
	}

	// Frame starts, so a cut can land exactly at a boundary with every
	// preceding frame complete — the case the payload-length check cannot see.
	var starts []int
	for i := 1; i+5 < len(complete); i++ {
		if complete[i] == 'F' && string(complete[i:i+5]) == "FX/1 " && complete[i-1] == '\n' {
			starts = append(starts, i)
		}
	}
	if len(starts) < 2 {
		t.Fatalf("expected a multi-frame body, found %d frame starts", len(starts))
	}

	cuts := []struct {
		name string
		at   int
	}{
		{"exact frame boundary", starts[1]},
		{"inside a header", starts[1] + 10},
		{"inside a payload", len(complete) / 4},
		{"inside the final trailer", len(complete) - 20},
		{"one byte short", len(complete) - 1},
	}
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			_, err := readItemBody(bytes.NewReader(complete[:cut.at]), ackItemKeys, "ACK")
			if err == nil {
				t.Fatalf("a body truncated at %d/%d bytes was accepted", cut.at, len(complete))
			}
			// Frame header, payload, and trailer each report their own stage;
			// all three are framing errors. An item-grammar error here means
			// the framing layer let the truncation through.
			if !strings.Contains(err.Error(), "read frame") {
				t.Fatalf("truncation surfaced as %q, not a framing error; the framing "+
					"layer accepted a short body and only item parsing happened to fail", err)
			}
		})
	}
}

// A truncated ACK must leave the store untouched, not apply the prefix that
// arrived. Uses realDeps because the point is real store state.
func TestHandleACKTruncatedAppliesNothing(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "f.bin")
	if err := os.WriteFile(path, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	deps := realDeps(t, "/")

	txferReq, err := ParseRequest([]byte(fmt.Sprintf("TXFER %q mode=fast link-mbps=100 concurrency=1 comp=none", root)))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var manifest bytes.Buffer
	var txferID string
	if err := handleTXFERWithCallback(context.Background(), txferReq, &manifest, deps, func(id string) { txferID = id }); err != nil {
		t.Fatalf("TXFER: %v", err)
	}
	entries, _ := parseSYNCResponseEntries(unframeManifestWire(t, manifest.Bytes()), nil)
	if len(entries) != 1 {
		t.Fatalf("expected one entry, got %d", len(entries))
	}
	hash := "xxh128:0000000000000000000000000000000a"
	if !deps.SetTransferFileWindowHash(txferID, entries[0].ID, entries[0].Size, hash) {
		t.Fatal("SetTransferFileWindowHash returned false")
	}

	var body bytes.Buffer
	line := fmt.Sprintf("fd=%d %q ack-token=%d@1@%s\n", entries[0].ID, path, entries[0].Size, hash)
	bw := encoding.NewFramedBodyWriter(&body, encoding.EncodingZstd, int64(len(line)), 0)
	if _, err := io.WriteString(bw, line); err != nil {
		t.Fatal(err)
	}
	terminalStart := body.Len()
	if err := bw.Close(); err != nil {
		t.Fatalf("close body: %v", err)
	}

	req, err := ParseRequest([]byte("ACK " + txferID))
	if err != nil {
		t.Fatalf("ParseRequest ACK: %v", err)
	}
	for _, cut := range []int{terminalStart, body.Len() - 8} {
		var out bytes.Buffer
		if err := handleACKWithInput(context.Background(), req, bytes.NewReader(body.Bytes()[:cut]), &out, deps); err == nil {
			t.Fatalf("ACK truncated at %d was accepted", cut)
		}
		if got, ok := deps.GetTransfer(txferID); !ok || got.DoneSize != 0 || got.Done != 0 {
			t.Fatalf("truncated ACK advanced progress: Done=%d DoneSize=%d", got.Done, got.DoneSize)
		}
		if out.Len() != 0 {
			t.Fatalf("truncated ACK produced a response: %q", out.String())
		}
	}
}

// Exercise the real maximum through the production reader without retaining
// millions of parsed items in the test harness.
func TestReadItemBodyMaxRequestBytesBoundary(t *testing.T) {
	maxBytes := int(encoding.MaxRequestBytes)
	blankLines := bytes.Repeat([]byte("\n"), 64*1024)
	for _, size := range []int{maxBytes, maxBytes + 1} {
		var wire bytes.Buffer
		w := encoding.NewFramedBodyWriter(&wire, encoding.EncodingZstd, encoding.DefaultBodyChunkSize, 0)
		for written := 0; written < size; {
			n := min(len(blankLines), size-written)
			if _, err := w.Write(blankLines[:n]); err != nil {
				t.Fatal(err)
			}
			written += n
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		body, err := readRequestItemBody(&wire, "SEND")
		if size > maxBytes {
			if err == nil {
				body.Release()
				t.Fatal("maximum plus one blank byte accepted")
			}
			continue
		}
		if err != nil {
			t.Fatalf("exact maximum rejected: %v", err)
		}
		if body.Len() != maxBytes {
			t.Fatalf("decoded %d bytes, want %d", body.Len(), maxBytes)
		}
		body.Release()
	}
}

func TestRequestValidationPrecedesResponse(t *testing.T) {
	for _, verb := range []string{"SEND", "CXSUM"} {
		t.Run(verb, func(t *testing.T) {
			req, err := ParseRequest([]byte(verb + " tx123"))
			if err != nil {
				t.Fatal(err)
			}
			var wire, out bytes.Buffer
			w := encoding.NewFramedBodyWriter(&wire, "none", 4096, 0)
			if _, err := io.WriteString(w, "fd=1 6:/first\nfd=2 7:/second offset=-1\n"); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			deps := &mockDeps{}
			if verb == "SEND" {
				err = handleSENDWithOptions(context.Background(), req, &wire, &out, deps, nil, true, 100)
			} else {
				err = handleCXSUMWithInput(context.Background(), req, &wire, &out, deps)
			}
			pe, ok := err.(protocolErr)
			if !ok || pe.code != "BAD_REQUEST" || !strings.Contains(pe.message, "offset") || out.Len() != 0 || deps.setStateCalls != 0 {
				t.Fatalf("invalid later item: err=%v, response bytes=%d, state changes=%d", err, out.Len(), deps.setStateCalls)
			}
		})
	}
}

// benchItemBody frames n item lines, returning the wire bytes. Built once per
// benchmark so the measurement covers decoding, not construction.
func benchItemBody(b *testing.B, n, pathDepth int) []byte {
	b.Helper()
	var buf bytes.Buffer
	w := encoding.NewFramedBodyWriter(&buf, encoding.EncodingZstd, encoding.DefaultBodyChunkSize, 0)
	prefix := "/remote/" + strings.Repeat("nested-directory/", pathDepth)
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("%sfile-%07d.bin", prefix, i)
		if _, err := fmt.Fprintf(w, "fd=%d %d:%s comp=none\n", i+1, len(path), path); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

// Measure the production compact-record parser, including framed decoding.
func BenchmarkRequestRecords(b *testing.B) {
	for _, tc := range []struct {
		name  string
		depth int
	}{{"short-paths", 1}, {"long-paths", 12}} {
		body := benchItemBody(b, 50000, tc.depth)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				records, err := parseRequestItemRecords(bytes.NewReader(body), sendItemKeys, "SEND", parseSENDRecord)
				if err != nil || len(records) != 50000 {
					b.Fatalf("records=%d err=%v", len(records), err)
				}
			}
		})
	}
}

func TestCompactRequestRecords(t *testing.T) {
	for _, codec := range []string{"adapt", "none", "identity", "lz4", "zstd"} {
		line := fmt.Sprintf("fd=7 4:/foo offset=9 size=11 comp=%s", codec)
		records, err := parseRequestItemRecords(framedItemBody(t, "\n"+line+"\r\n\n"+line), sendItemKeys, "SEND", parseSENDRecord)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 2 || cap(records) != 2 || records[0] != records[1] {
			t.Fatalf("records: %+v", records)
		}
		item := records[0].item(loadStrategyGentle)
		wantCodec := codec
		if codec == "identity" {
			wantCodec = "none"
		}
		if item.FileID != 7 || item.Path != "/foo" || item.Offset != 9 || item.Size != 11 || item.Comp != wantCodec || item.Mode != loadStrategyGentle {
			t.Fatalf("codec=%s item=%+v", codec, item)
		}
	}
	for _, algo := range []string{"", "none", "xxh128", "xxh64", "xxh64,none,xxh128,xxh64"} {
		for _, size := range []string{"", " size=0", " size=3"} {
			line := "fd=7 4:/foo offset=9" + size
			if algo != "" {
				line += " algo=" + algo
			}
			raw, err := parseItemLine([]byte(line), cxsumItemKeys, "CXSUM")
			if err != nil {
				t.Fatal(err)
			}
			want, err := parseCXSUMItem(raw)
			if err != nil {
				t.Fatal(err)
			}
			records, err := parseRequestItemRecords(framedItemBody(t, line), cxsumItemKeys, "CXSUM", parseCXSUMRecord)
			if err != nil || len(records) != 1 {
				t.Fatalf("records=%v err=%v", records, err)
			}
			got := records[0].item()
			if got.FileID != want.FileID || got.Path != want.Path || got.Offset != want.Offset || got.Size != want.Size || got.HasSize != want.HasSize || strings.Join(got.Algorithms, ",") != strings.Join(want.Algorithms, ",") {
				t.Fatalf("got=%+v want=%+v", got, want)
			}
		}
	}
	// Reject impossible lines in the counting pass, before reserving records.
	called := false
	_, err := parseRequestItemRecords(framedItemBody(t, strings.Repeat("x\n", 1000)), sendItemKeys, "SEND", func(raw map[string]string) (sendRequestRecord, error) { called = true; return parseSENDRecord(raw) })
	if err == nil || called {
		t.Fatalf("invalid preflight: called=%v err=%v", called, err)
	}
}
