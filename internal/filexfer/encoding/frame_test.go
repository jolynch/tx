package encoding

import (
	"bytes"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestWriteFrameReturnsStats(t *testing.T) {
	w := httptest.NewRecorder()
	payload := bytes.Repeat([]byte("x"), 1<<20)
	stats, err := WriteFrame(w, WriteArgs{
		FileID:     1,
		Offset:     0,
		Size:       int64(len(payload)),
		WSize:      int64(len(payload)),
		Comp:       "none",
		HeaderHash: "xxh128:00",
		HeaderTS:   time.Now().UnixMilli(),
		Payload:    payload,
		TrailerTS:  time.Now().UnixMilli(),
		HashTokens: []string{"xxh128:00"},
		Next:       0,
	})
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}
	if stats.WriteLatency <= 0 {
		t.Fatalf("expected positive write latency, got %v", stats.WriteLatency)
	}
	if stats.WireThroughputBps <= 0 {
		t.Fatalf("expected positive wire throughput, got %f", stats.WireThroughputBps)
	}
}

func TestParseFXTrailerAllowsMissingTrailerHash(t *testing.T) {
	trailer, err := ParseFXTrailer("FXT/1 2 status=ok ts=1001 next=0 file-hash=xxh128:0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("ParseFXTrailer failed: %v", err)
	}
	if trailer.FileID != 2 {
		t.Fatalf("unexpected file id: %d", trailer.FileID)
	}
	if trailer.HashToken != "" {
		t.Fatalf("expected empty trailer hash token, got %q", trailer.HashToken)
	}
}

// frameInfo supplies OS metadata without requiring privileged chown calls.
type frameInfo struct {
	os.FileInfo
	stat any
}

func (i frameInfo) Sys() any { return i.stat }

func TestCollectFileFrameMetadataStat(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "frame")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte("metadata")); err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	userNameCache.Store("1234567", "test user")
	groupNameCache.Store("7654321", "test group")
	defer userNameCache.Delete("1234567")
	defer groupNameCache.Delete("7654321")
	for _, tc := range []struct {
		name                  string
		stat                  any
		uid, gid, user, group string
	}{
		{"numeric", &syscall.Stat_t{Uid: 1234567, Gid: 7654321}, "1234567", "7654321", "test user", "test group"},
		{"missing", nil, "unknown", "unknown", "unknown", "unknown"},
		{"foreign", "not a stat", "unknown", "unknown", "unknown", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := CollectFileFrameMetadata(file.Name(), frameInfo{FileInfo: info, stat: tc.stat})
			want := FileFrameMetadata{Size: info.Size(), MtimeNS: info.ModTime().UnixNano(), Mode: FormatManifestMode(info.Mode()), UID: tc.uid, GID: tc.gid, User: tc.user, Group: tc.group}
			if got != want {
				t.Fatalf("metadata = %+v, want %+v", got, want)
			}
			if tc.name == "numeric" {
				tokens := got.trailerTokens()
				want := []string{"meta:uid=1234567", "meta:gid=7654321", "meta:user=test_user", "meta:group=test_group"}
				for i, token := range want {
					if tokens[i+3] != token {
						t.Errorf("token %d = %q, want %q", i+3, tokens[i+3], token)
					}
				}
			}
		})
	}
}
