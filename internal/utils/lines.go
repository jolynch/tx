package utils

import (
	"bufio"
	"errors"
)

// ErrLineTooLarge is returned by ReadLineLimit when a line exceeds the caller's
// budget. Callers map it to their own protocol error.
var ErrLineTooLarge = errors.New("line too large")

// ReadLineLimit reads through the next '\n' and returns the line including that
// terminator, failing with ErrLineTooLarge as soon as the accumulated bytes
// exceed maxBytes.
//
// The point is the "as soon as": bufio.Reader.ReadBytes/ReadString grow an
// unbounded buffer and can only be size-checked after the allocation they were
// meant to prevent. ReadSlice instead reads within the reader's fixed buffer and
// reports bufio.ErrBufferFull when it fills without finding the delimiter, which
// is what lets this loop enforce the budget as it goes.
//
// Like bufio.Reader.ReadBytes, data read before an error is returned alongside
// it, so a caller can salvage a final unterminated line. The ErrLineTooLarge
// case is the exception: there is deliberately nothing to salvage.
//
// maxBytes <= 0 means unlimited.
func ReadLineLimit(br *bufio.Reader, maxBytes int) ([]byte, error) {
	// ReadSlice's result aliases the reader's internal buffer and is invalidated
	// by the next read, so every return path copies first.
	chunk, err := br.ReadSlice('\n')
	if maxBytes > 0 && len(chunk) > maxBytes {
		return nil, ErrLineTooLarge
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		out := make([]byte, len(chunk))
		copy(out, chunk)
		return out, err
	}

	// The line is longer than the reader's buffer. Keep pulling buffer-sized
	// chunks, stopping the moment the total would exceed the budget.
	out := make([]byte, 0, len(chunk)*2)
	out = append(out, chunk...)
	for {
		chunk, err = br.ReadSlice('\n')
		if maxBytes > 0 && len(out)+len(chunk) > maxBytes {
			return nil, ErrLineTooLarge
		}
		out = append(out, chunk...)
		if !errors.Is(err, bufio.ErrBufferFull) {
			return out, err
		}
	}
}

// TrimLineTerminator strips a trailing "\r\n" or "\n" from a line read by
// ReadLineLimit. It does not touch any other whitespace: path tokens on the wire
// are length-prefixed, so trimming spaces would eat bytes the prefix counted.
func TrimLineTerminator(line []byte) []byte {
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
	}
	return line
}

// ContainsLineBreak reports whether s contains '\n' or '\r'. Values carrying
// either byte cannot be encoded into FTCP's line-oriented framing: the receiver
// splits on '\n' before consulting the length prefix that would otherwise make
// the token self-delimiting, so the remainder is re-parsed as a new line.
func ContainsLineBreak(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' || s[i] == '\r' {
			return true
		}
	}
	return false
}
