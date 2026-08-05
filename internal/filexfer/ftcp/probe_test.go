package ftcp

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestHandlePROBERoundTrip(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=1024 cts0=100`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	payload := bytes.Repeat([]byte{0x5a}, 1024)
	in := bytes.NewReader(payload)
	var out bytes.Buffer
	if err := handlePROBEWithInput(context.Background(), req, in, &out, &mockDeps{}, 8, 25, 25, 1*1024*1024, 0); err != nil {
		t.Fatalf("handlePROBEWithInput failed: %v", err)
	}

	br := bufio.NewReader(bytes.NewReader(out.Bytes()))
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read response line: %v", err)
	}
	respReq, err := ParseRequest([]byte(strings.TrimRight(line, "\r\n")))
	if err != nil {
		t.Fatalf("parse response line: %v", err)
	}
	if respReq.Verb != VerbPROBE {
		t.Fatalf("expected PROBE response, got %v", respReq.Verb)
	}
	if got := respReq.Params[0]["probe-bytes"]; got != "1024" {
		t.Fatalf("expected probe-bytes=1024, got %q", got)
	}
	if got := respReq.Params[0]["io-depth"]; got != "8" {
		t.Fatalf("expected io-depth=8, got %q", got)
	}
	if got := respReq.Params[0]["gentle-cpu-pct"]; got != "25" {
		t.Fatalf("expected gentle-cpu-pct=25, got %q", got)
	}
	if got := respReq.Params[0]["gentle-bw-pct"]; got != "25" {
		t.Fatalf("expected gentle-bw-pct=25, got %q", got)
	}
	echo := make([]byte, 1024)
	if _, err := io.ReadFull(br, echo); err != nil {
		t.Fatalf("read probe echo bytes: %v", err)
	}
	if len(echo) != 1024 {
		t.Fatalf("unexpected echo length: %d", len(echo))
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("expected eof after payload, got %v", err)
	}
}

func TestHandlePROBEDefaultIODepth(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=4 probe-bytes=0 cts0=50`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	var out bytes.Buffer
	if err := handlePROBEWithInput(context.Background(), req, bytes.NewReader(nil), &out, &mockDeps{}, 0, 30, 25, 1*1024*1024, 0); err != nil {
		t.Fatalf("handlePROBEWithInput failed: %v", err)
	}
	line, err := bufio.NewReader(bytes.NewReader(out.Bytes())).ReadString('\n')
	if err != nil {
		t.Fatalf("read response line: %v", err)
	}
	respReq, err := ParseRequest([]byte(strings.TrimRight(line, "\r\n")))
	if err != nil {
		t.Fatalf("parse response line: %v", err)
	}
	if got := respReq.Params[0]["io-depth"]; got != "8" {
		t.Fatalf("expected default io-depth=8, got %q", got)
	}
	if got := respReq.Params[0]["gentle-cpu-pct"]; got != "30" {
		t.Fatalf("expected gentle-cpu-pct=30, got %q", got)
	}
	if got := respReq.Params[0]["gentle-bw-pct"]; got != "25" {
		t.Fatalf("expected gentle-bw-pct=25, got %q", got)
	}
}

func TestHandlePROBERejectsShortPayload(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=10 cts0=100`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	in := bytes.NewReader([]byte{1, 2, 3})
	var out bytes.Buffer
	err = handlePROBEWithInput(context.Background(), req, in, &out, &mockDeps{}, 8, 25, 25, 1*1024*1024, 0)
	if err == nil {
		t.Fatalf("expected payload validation error")
	}
}

func TestHandlePROBEReportsObservedLink(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=8 probe-bytes=0 cts0=100 txferid=tx123 obs-link-mbps=900`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	deps := &mockDeps{
		reportReturn:   TransferObservedLinkUpdate{ObservedLinkMbps: 900, EMALinkMbps: 900, OldRateBps: 0, NewRateBps: 28125000},
		reportReturnOK: true,
	}
	var out bytes.Buffer
	if err := handlePROBEWithInput(context.Background(), req, bytes.NewReader(nil), &out, deps, 8, 25, 25, 2*1024*1024, 0); err != nil {
		t.Fatalf("handlePROBEWithInput failed: %v", err)
	}
	if !deps.reportCalled {
		t.Fatalf("expected ReportTransferObservedLink call")
	}
	if deps.reportTxferID != "tx123" || deps.reportObserved != 900 {
		t.Fatalf("unexpected report args: tx=%s observed=%d", deps.reportTxferID, deps.reportObserved)
	}
	if deps.reportBWPct != 25 || deps.reportBurst != 2*1024*1024 {
		t.Fatalf("unexpected limiter args: pct=%d burst=%d", deps.reportBWPct, deps.reportBurst)
	}
	if deps.reportEMAAlpha != 0.2 {
		t.Fatalf("unexpected EMA alpha: %.2f", deps.reportEMAAlpha)
	}
}

func TestParsePROBERequestKeepAlive(t *testing.T) {
	req, err := ParseRequest([]byte(`PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=auto`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	parsed, err := parsePROBERequest(req)
	if err != nil {
		t.Fatalf("parsePROBERequest failed: %v", err)
	}
	if !parsed.KeepAlive {
		t.Fatalf("expected KeepAlive to be parsed from keep-alive=auto")
	}

	// Only "auto" is a valid request value; anything else is rejected.
	req, err = ParseRequest([]byte(`PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=1`))
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	if _, err := parsePROBERequest(req); err == nil {
		t.Fatalf("expected keep-alive=1 to be rejected")
	}
}

func TestHandlePROBEKeepAliveGrant(t *testing.T) {
	cases := []struct {
		name        string
		cmd         string
		keepAliveMS int64
		wantToken   bool
	}{
		{"requested and enabled", `PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=auto`, 60000, true},
		{"requested but disabled", `PROBE cpu=2 probe-bytes=0 cts0=5 keep-alive=auto`, 0, false},
		{"not requested", `PROBE cpu=2 probe-bytes=0 cts0=5`, 60000, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest([]byte(tc.cmd))
			if err != nil {
				t.Fatalf("ParseRequest failed: %v", err)
			}
			var out bytes.Buffer
			if err := handlePROBEWithInput(context.Background(), req, bytes.NewReader(nil), &out, &mockDeps{}, 8, 25, 25, 1*1024*1024, tc.keepAliveMS); err != nil {
				t.Fatalf("handlePROBEWithInput failed: %v", err)
			}
			line, err := bufio.NewReader(bytes.NewReader(out.Bytes())).ReadString('\n')
			if err != nil {
				t.Fatalf("read response line: %v", err)
			}
			gotToken := strings.Contains(line, "keep-alive-ms=60000")
			if gotToken != tc.wantToken {
				t.Fatalf("keep-alive-ms token presence = %v, want %v (line %q)", gotToken, tc.wantToken, line)
			}
		})
	}
}

func TestParsePROBERequestRejectsOversizedProbe(t *testing.T) {
	req := Request{
		Verb: VerbPROBE,
		Params: []map[string]string{{
			"cpu":         "4",
			"probe-bytes": strconv.FormatInt(maxProbeBytes+1, 10),
			"cts0":        "100",
		}},
	}
	_, err := parsePROBERequest(req)
	if err == nil {
		t.Fatalf("expected oversized probe error")
	}
}

func TestParsePROBERequestObservedLinkRequiresTransferID(t *testing.T) {
	req := Request{
		Verb: VerbPROBE,
		Params: []map[string]string{{
			"cpu":           "4",
			"probe-bytes":   "0",
			"cts0":          "100",
			"obs-link-mbps": "900",
		}},
	}
	if _, err := parsePROBERequest(req); err == nil {
		t.Fatalf("expected obs-link-mbps validation error")
	}
}
