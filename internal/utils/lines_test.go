package utils

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

// endlessReader never yields '\n' and records how many bytes it handed out, so
// a test can assert that a read gave up rather than buffering without bound.
type endlessReader struct{ served int64 }

func (r *endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'A'
	}
	r.served += int64(len(p))
	return len(p), nil
}

// The regression this guards: ReadBytes/ReadString grow an unbounded buffer and
// can only be size-checked afterwards, so a client that never sends '\n' drives
// allocation until the process dies.
func TestReadLineLimitStopsWithoutBuffering(t *testing.T) {
	const maxBytes = 64 * 1024
	src := &endlessReader{}
	if _, err := ReadLineLimit(bufio.NewReader(src), maxBytes); err != ErrLineTooLarge {
		t.Fatalf("err = %v, want ErrLineTooLarge", err)
	}
	// Generous headroom for buffer granularity; the point is that consumption
	// is a function of the cap, not of how much the peer is willing to send.
	if src.served > maxBytes*4 {
		t.Fatalf("consumed %d bytes enforcing a %d byte cap", src.served, maxBytes)
	}
}

func TestReadLineLimitSpansBufferRefills(t *testing.T) {
	// Far larger than bufio's default 4096 buffer, so this only passes if the
	// refill loop reassembles the line across ErrBufferFull.
	want := strings.Repeat("x", 500_000) + "\n"
	got, err := ReadLineLimit(bufio.NewReader(strings.NewReader(want)), 1<<20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != want {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
}

func TestReadLineLimitUnlimited(t *testing.T) {
	want := strings.Repeat("y", 200_000) + "\n"
	got, err := ReadLineLimit(bufio.NewReader(strings.NewReader(want)), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != want {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
}

func TestReadLineLimitPropagatesEOF(t *testing.T) {
	if _, err := ReadLineLimit(bufio.NewReader(strings.NewReader("no terminator")), 1<<20); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestReadLineLimitExactlyAtCap(t *testing.T) {
	line := strings.Repeat("z", 99) + "\n" // exactly 100 bytes
	got, err := ReadLineLimit(bufio.NewReader(strings.NewReader(line)), 100)
	if err != nil {
		t.Fatalf("100 bytes under a 100 byte cap should pass, got %v", err)
	}
	if string(got) != line {
		t.Fatalf("got %q", got)
	}
	if _, err := ReadLineLimit(bufio.NewReader(strings.NewReader(line)), 99); err != ErrLineTooLarge {
		t.Fatalf("err = %v, want ErrLineTooLarge", err)
	}
}

func TestTrimLineTerminator(t *testing.T) {
	cases := map[string]string{
		"abc\r\n": "abc",
		"abc\n":   "abc",
		"abc":     "abc",
		"\n":      "",
		"":        "",
		// Whitespace that is not the terminator must survive: path tokens are
		// length-prefixed and the prefix counted these bytes.
		"trailing \n":   "trailing ",
		"trailing \r\n": "trailing ",
		"\ttabbed\t\n":  "\ttabbed\t",
	}
	for in, want := range cases {
		if got := string(TrimLineTerminator([]byte(in))); got != want {
			t.Errorf("TrimLineTerminator(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestContainsLineBreak(t *testing.T) {
	for _, s := range []string{"a\nb", "a\rb", "\n", "\r", "trailing\n"} {
		if !ContainsLineBreak(s) {
			t.Errorf("ContainsLineBreak(%q) = false", s)
		}
	}
	for _, s := range []string{"", "abc", "with space ", "\ttab", "emoji😀"} {
		if ContainsLineBreak(s) {
			t.Errorf("ContainsLineBreak(%q) = true", s)
		}
	}
}
