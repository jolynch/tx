package report

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// KV is one event-specific field. Numbers decode as json.Number so integer
// nanosecond values keep full precision.
type KV struct {
	Key string
	Val any
}

// TraceRecord is one trace event, in either encoding.
type TraceRecord struct {
	T      int64
	Side   string
	Run    string
	TID    string
	Ev     string
	Fields []KV
}

// Get returns a field value.
func (r TraceRecord) Get(key string) (any, bool) {
	for _, f := range r.Fields {
		if f.Key == key {
			return f.Val, true
		}
	}
	return nil, false
}

// Int returns an integer field, or 0.
func (r TraceRecord) Int(key string) int64 {
	v, ok := r.Get(key)
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			f, _ := x.Float64()
			return int64(f)
		}
		return n
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

// Has reports whether key is present.
func (r TraceRecord) Has(key string) bool {
	_, ok := r.Get(key)
	return ok
}

// Str returns a string field, or "".
func (r TraceRecord) Str(key string) string {
	v, _ := r.Get(key)
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	}
	return ""
}

// Bool returns a boolean field.
func (r TraceRecord) Bool(key string) bool {
	v, _ := r.Get(key)
	b, _ := v.(bool)
	return b
}

// TraceHeader is the first line of a trace file.
type TraceHeader struct {
	Side          string `json:"side"`
	Host          string `json:"host"`
	Start         string `json:"start"`
	ClockOffsetNS int64  `json:"clock_offset_ns"`
	ClockErrNS    int64  `json:"clock_err_ns"`
}

// ParseJSONRecord decodes one JSON-lines event, keeping field order.
func ParseJSONRecord(line []byte) (TraceRecord, error) {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return TraceRecord{}, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return TraceRecord{}, fmt.Errorf("event is not an object")
	}
	var r TraceRecord
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return TraceRecord{}, err
		}
		key, _ := kt.(string)
		var val any
		if err := dec.Decode(&val); err != nil {
			return TraceRecord{}, err
		}
		switch key {
		case "t":
			n, _ := val.(json.Number)
			r.T, _ = n.Int64()
		case "side":
			r.Side, _ = val.(string)
		case "run":
			r.Run, _ = val.(string)
		case "tid":
			r.TID, _ = val.(string)
		case "ev":
			r.Ev, _ = val.(string)
		default:
			r.Fields = append(r.Fields, KV{key, val})
		}
	}
	return r, nil
}

// JSON encodes the record as one line, newline included.
func (r TraceRecord) JSON() []byte {
	var b bytes.Buffer
	b.WriteString(`{"t":`)
	b.WriteString(strconv.FormatInt(r.T, 10))
	writeJSONKV(&b, "side", r.Side)
	run := r.Run
	if run == "" {
		run = "-"
	}
	writeJSONKV(&b, "run", run)
	tid := r.TID
	if tid == "" {
		tid = "-"
	}
	writeJSONKV(&b, "tid", tid)
	writeJSONKV(&b, "ev", r.Ev)
	for _, f := range r.Fields {
		writeJSONKV(&b, f.Key, f.Val)
	}
	b.WriteString("}\n")
	return b.Bytes()
}

func writeJSONKV(b *bytes.Buffer, k string, v any) {
	kb, _ := json.Marshal(k)
	vb, err := json.Marshal(v)
	if err != nil {
		vb = []byte("null")
	}
	b.WriteByte(',')
	b.Write(kb)
	b.WriteByte(':')
	b.Write(vb)
}

// textFixed are the event fields that get their own text column.
var textFixed = []string{"file", "off", "len", "dur"}

// Text encodes the record as one space-separated line:
// t side run tid ev file off len dur k=v...
func (r TraceRecord) Text() string {
	var b strings.Builder
	b.WriteString(strconv.FormatInt(r.T, 10))
	for _, v := range []string{r.Side, r.Run, r.TID, r.Ev} {
		b.WriteByte(' ')
		if v == "-" {
			b.WriteByte('-') // the empty marker, as in "run - on the sender"
			continue
		}
		b.WriteString(textToken(v))
	}
	for _, k := range textFixed {
		b.WriteByte(' ')
		if v, ok := r.Get(k); ok {
			b.WriteString(textValue(v))
		} else {
			b.WriteByte('-')
		}
	}
	for _, f := range r.Fields {
		if isFixed(f.Key) {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(f.Key)
		b.WriteByte('=')
		b.WriteString(textValue(f.Val))
	}
	b.WriteByte('\n')
	return b.String()
}

func isFixed(k string) bool {
	for _, f := range textFixed {
		if f == k {
			return true
		}
	}
	return false
}

func textToken(s string) string {
	if s == "" {
		return "-"
	}
	return textValue(s)
}

// textValue writes numbers and booleans bare and quotes any string that
// would not survive splitting.
func textValue(v any) string {
	switch x := v.(type) {
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return "null"
	case string:
		if x == "" || x == "-" || strings.ContainsAny(x, " \t\n\r=\"\\") || looksNumeric(x) || x == "true" || x == "false" || x == "null" {
			return strconv.Quote(x)
		}
		return x
	case int, int64, uint64, float64:
		return fmt.Sprint(x)
	}
	data, _ := json.Marshal(v)
	return strconv.Quote(string(data))
}

func looksNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// ParseTextRecord decodes one text line.
func ParseTextRecord(line string) (TraceRecord, error) {
	toks, err := splitText(line)
	if err != nil {
		return TraceRecord{}, err
	}
	if len(toks) < 9 {
		return TraceRecord{}, fmt.Errorf("want at least 9 columns, got %d", len(toks))
	}
	var r TraceRecord
	if r.T, err = strconv.ParseInt(toks[0].raw, 10, 64); err != nil {
		return TraceRecord{}, fmt.Errorf("bad t %q", toks[0].raw)
	}
	str := func(t textTok) string {
		if !t.quoted && t.raw == "-" {
			return ""
		}
		return t.raw
	}
	r.Side, r.Run, r.TID, r.Ev = str(toks[1]), str(toks[2]), str(toks[3]), str(toks[4])
	for i, k := range textFixed {
		t := toks[5+i]
		if !t.quoted && t.raw == "-" {
			continue
		}
		r.Fields = append(r.Fields, KV{k, t.value()})
	}
	for _, t := range toks[9:] {
		k, v, ok := strings.Cut(t.raw, "=")
		if !ok || t.quoted {
			return TraceRecord{}, fmt.Errorf("bad field %q", t.raw)
		}
		vt := textTok{raw: v}
		if strings.HasPrefix(v, `"`) {
			uq, err := strconv.Unquote(v)
			if err != nil {
				return TraceRecord{}, fmt.Errorf("bad quoted value in %q", t.raw)
			}
			vt = textTok{raw: uq, quoted: true}
		}
		r.Fields = append(r.Fields, KV{k, vt.value()})
	}
	return r, nil
}

type textTok struct {
	raw    string
	quoted bool
}

func (t textTok) value() any {
	if t.quoted {
		return t.raw
	}
	switch t.raw {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	}
	if looksNumeric(t.raw) {
		return json.Number(t.raw)
	}
	return t.raw
}

// splitText splits on spaces, honoring Go-quoted tokens, including quoted
// values after "key=".
func splitText(line string) ([]textTok, error) {
	line = strings.TrimRight(line, "\n")
	var out []textTok
	for i := 0; i < len(line); {
		if line[i] == ' ' {
			i++
			continue
		}
		start := i
		quoteAt := -1
		if line[i] == '"' {
			quoteAt = i
		} else {
			for j := i; j < len(line) && line[j] != ' '; j++ {
				if line[j] == '=' && j+1 < len(line) && line[j+1] == '"' {
					quoteAt = j + 1
					break
				}
			}
		}
		if quoteAt < 0 {
			for i < len(line) && line[i] != ' ' {
				i++
			}
			out = append(out, textTok{raw: line[start:i]})
			continue
		}
		// Scan the quoted string to its closing quote.
		j := quoteAt + 1
		for ; j < len(line); j++ {
			if line[j] == '\\' {
				j++
				continue
			}
			if line[j] == '"' {
				break
			}
		}
		if j >= len(line) {
			return nil, fmt.Errorf("unterminated quote")
		}
		i = j + 1
		tok := line[start:i]
		if quoteAt == start {
			uq, err := strconv.Unquote(tok)
			if err != nil {
				return nil, err
			}
			out = append(out, textTok{raw: uq, quoted: true})
		} else {
			out = append(out, textTok{raw: tok})
		}
	}
	return out, nil
}

// TextHeaderLine renders the header comment of a text trace.
func TextHeaderLine(h TraceHeader) string {
	return fmt.Sprintf("# tx-bench trace v1 side=%s host=%s start=%s clock_offset_ns=%d clock_err_ns=%d\n",
		textToken(h.Side), textToken(h.Host), textToken(h.Start), h.ClockOffsetNS, h.ClockErrNS)
}

// jsonTraceHeader is the first line of a JSON-lines trace. "trace" comes
// first so the file is recognizable from its first bytes.
type jsonTraceHeader struct {
	Trace  string      `json:"trace"`
	Header TraceHeader `json:"header"`
}

// JSONHeaderLine renders the header object of a JSON-lines trace.
func JSONHeaderLine(h TraceHeader) []byte {
	data, _ := json.Marshal(jsonTraceHeader{Trace: "tx-bench v1", Header: h})
	return append(data, '\n')
}

// ReadTrace reads a trace in either encoding.
func ReadTrace(path string) (TraceHeader, []TraceRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return TraceHeader{}, nil, err
	}
	defer f.Close()
	return ParseTrace(f)
}

// ParseTrace reads a trace stream in either encoding.
func ParseTrace(r io.Reader) (TraceHeader, []TraceRecord, error) {
	var h TraceHeader
	var recs []TraceRecord
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		switch {
		case strings.TrimSpace(line) == "":
		case strings.HasPrefix(line, "# tx-bench trace"):
			for _, kv := range strings.Fields(line)[4:] {
				k, v, _ := strings.Cut(kv, "=")
				switch k {
				case "side":
					h.Side = v
				case "host":
					h.Host = v
				case "start":
					h.Start = v
				case "clock_offset_ns":
					h.ClockOffsetNS, _ = strconv.ParseInt(v, 10, 64)
				case "clock_err_ns":
					h.ClockErrNS, _ = strconv.ParseInt(v, 10, 64)
				}
			}
		case strings.HasPrefix(line, "#"):
		case strings.HasPrefix(line, `{"trace":`):
			var hdr jsonTraceHeader
			if err := json.Unmarshal([]byte(line), &hdr); err != nil {
				return h, nil, fmt.Errorf("line %d: %w", n, err)
			}
			h = hdr.Header
		case strings.HasPrefix(line, "{"):
			rec, err := ParseJSONRecord([]byte(line))
			if err != nil {
				return h, nil, fmt.Errorf("line %d: %w", n, err)
			}
			recs = append(recs, rec)
		default:
			rec, err := ParseTextRecord(line)
			if err != nil {
				return h, nil, fmt.Errorf("line %d: %w", n, err)
			}
			recs = append(recs, rec)
		}
	}
	return h, recs, sc.Err()
}

// WriteTrace writes a whole trace in the encoding path's extension selects.
func WriteTrace(path string, h TraceHeader, recs []TraceRecord) error {
	var b bytes.Buffer
	asJSON := IsJSONPath(path)
	if asJSON {
		b.Write(JSONHeaderLine(h))
	} else {
		b.WriteString(TextHeaderLine(h))
	}
	for _, r := range recs {
		if asJSON {
			b.Write(r.JSON())
		} else {
			b.WriteString(r.Text())
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b.Bytes(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// AppendTrace appends records to a trace file in its encoding.
func AppendTrace(path string, recs []TraceRecord) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	asJSON := IsJSONPath(path)
	for _, r := range recs {
		if asJSON {
			_, _ = bw.Write(r.JSON())
		} else {
			_, _ = bw.WriteString(r.Text())
		}
	}
	return bw.Flush()
}

// SiblingPath inserts "."+name before path's extension: c.jsonl ->
// c.server.jsonl.
func SiblingPath(path, name string) string {
	ext := ""
	if i := strings.LastIndexByte(path, '.'); i > strings.LastIndexByte(path, '/') {
		ext = path[i:]
		path = path[:i]
	}
	return path + "." + name + ext
}
