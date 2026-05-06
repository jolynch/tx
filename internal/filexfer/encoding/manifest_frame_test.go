package encoding

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestChunkedManifestWriterEmitsMultipleFramesAtSizeThreshold(t *testing.T) {
	var out bytes.Buffer
	cw := NewChunkedManifestWriter(&out, "none", 16, 0)
	payload := bytes.Repeat([]byte("abcdefgh"), 8) // 64 bytes -> 4 chunks of 16
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 4 {
		t.Fatalf("expected at least 4 frames, got %d", len(frames))
	}
	var reassembled bytes.Buffer
	for _, fr := range frames {
		reassembled.Write(fr.Payload)
	}
	if !bytes.Equal(reassembled.Bytes(), payload) {
		t.Fatalf("reassembled bytes differ from input")
	}
	if frames[len(frames)-1].TerminalNext != 0 {
		t.Fatalf("last frame should have next=0, got %d", frames[len(frames)-1].TerminalNext)
	}
}

func TestChunkedManifestWriterTimeBasedFlush(t *testing.T) {
	var out bytes.Buffer
	cw := NewChunkedManifestWriter(&out, "none", 1024*1024, 50*time.Millisecond)
	if _, err := cw.Write([]byte("hello ")); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := cw.Write([]byte("world")); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 2 {
		t.Fatalf("expected >=2 frames due to time-based flush, got %d", len(frames))
	}
}

func TestChunkedManifestWriterEmptyEmitsTerminalFrame(t *testing.T) {
	var out bytes.Buffer
	cw := NewChunkedManifestWriter(&out, "none", 16, 0)
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 terminal frame, got %d", len(frames))
	}
	if frames[0].Meta.Size != 0 || frames[0].Meta.WireSize != 0 {
		t.Fatalf("expected size=0 wsize=0, got %+v", frames[0].Meta)
	}
	if frames[0].TerminalNext != 0 {
		t.Fatalf("expected next=0, got %d", frames[0].TerminalNext)
	}
}

func TestChunkedManifestWriterCompZstd(t *testing.T) {
	var out bytes.Buffer
	cw := NewChunkedManifestWriter(&out, EncodingZstd, 32, 0)
	payload := []byte(strings.Repeat("FM/1 line: payload data here\n", 4))
	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 2 {
		t.Fatalf("expected >=2 zstd frames, got %d", len(frames))
	}
	var reassembled bytes.Buffer
	for _, fr := range frames {
		if fr.Meta.Comp != EncodingZstd {
			t.Fatalf("frame comp=%q, want zstd", fr.Meta.Comp)
		}
		if fr.Meta.WireSize == 0 && fr.Meta.Size > 0 {
			t.Fatalf("non-empty chunk has wsize=0")
		}
		if fr.Meta.Size > 0 {
			decoded, err := DecompressZstd(fr.Payload)
			if err != nil {
				t.Fatalf("DecompressZstd: %v", err)
			}
			reassembled.Write(decoded)
		}
	}
	if !bytes.Equal(reassembled.Bytes(), payload) {
		t.Fatalf("decoded bytes differ from input")
	}
}

func TestChunkedManifestWriterTerminalCarriesFileHashIntermediatesDoNot(t *testing.T) {
	var out bytes.Buffer
	cw := NewChunkedManifestWriter(&out, "none", 4, 0)
	if _, err := cw.Write([]byte("abcdefghij")); err != nil { // 10 bytes -> 3 chunks
		t.Fatalf("Write: %v", err)
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frames := parseAllFrames(t, out.Bytes())
	if len(frames) < 2 {
		t.Fatalf("expected multiple frames, got %d", len(frames))
	}
	for i, fr := range frames {
		hasFile := fr.Trailer.FileHashToken != ""
		if i == len(frames)-1 && !hasFile {
			t.Fatalf("terminal frame missing file-hash")
		}
		if i != len(frames)-1 && hasFile {
			t.Fatalf("non-terminal frame %d has unexpected file-hash %q", i, fr.Trailer.FileHashToken)
		}
	}
}

type testFrame struct {
	Meta         FileFrameMeta
	Payload      []byte
	Trailer      FrameTrailer
	TerminalNext int64
}

func parseAllFrames(t *testing.T, wire []byte) []testFrame {
	t.Helper()
	var frames []testFrame
	for len(wire) > 0 {
		nl := bytes.IndexByte(wire, '\n')
		if nl < 0 {
			t.Fatalf("missing header newline; remaining=%q", wire)
		}
		meta, err := ParseFXHeader(string(wire[:nl]))
		if err != nil {
			t.Fatalf("ParseFXHeader: %v", err)
		}
		wire = wire[nl+1:]
		if int64(len(wire)) < meta.WireSize {
			t.Fatalf("wire short: have=%d need=%d", len(wire), meta.WireSize)
		}
		payload := append([]byte(nil), wire[:meta.WireSize]...)
		wire = wire[meta.WireSize:]
		nl = bytes.IndexByte(wire, '\n')
		if nl < 0 {
			t.Fatalf("missing trailer newline; remaining=%q", wire)
		}
		trailer, err := ParseFXTrailer(string(wire[:nl]))
		if err != nil {
			t.Fatalf("ParseFXTrailer: %v", err)
		}
		wire = wire[nl+1:]
		nextVal := int64(-1)
		if trailer.Next != nil {
			nextVal = *trailer.Next
		}
		frames = append(frames, testFrame{Meta: meta, Payload: payload, Trailer: trailer, TerminalNext: nextVal})
	}
	return frames
}
