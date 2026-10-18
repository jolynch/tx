package ftcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jolynch/tx/internal/events"
	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/zeebo/xxh3"
)

func TestHandleCXSUMMultipleRanges(t *testing.T) {
	tmp := t.TempDir()
	filePath := filepath.Join(tmp, "cxsum.txt")
	if err := os.WriteFile(filePath, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	deps := &mockDeps{filePath: filePath}
	req, err := ParseRequest([]byte(`CXSUM tx1`))
	if err != nil {
		t.Fatalf("ParseRequest err: %v", err)
	}
	body := framedItemBody(t,
		`fd=1 "/tmp/a.txt" offset=0 size=5 algo=xxh128`,
		`fd=1 "/tmp/a.txt" offset=6 size=5 algo=xxh64`,
	)

	var out bytes.Buffer
	if err := handleCXSUMWithInput(context.Background(), req, body, &out, deps); err != nil {
		t.Fatalf("handleCXSUMWithInput err: %v", err)
	}
	if deps.refBatches != 1 || deps.openFileCalls != 2 {
		t.Fatalf("lookups=%d opens=%d; want 1, 2", deps.refBatches, deps.openFileCalls)
	}

	raw := out.String()
	if got := strings.Count(raw, "FX/1 "); got != 2 {
		t.Fatalf("expected 2 frames, got %d output=%q", got, raw)
	}
	if !strings.Contains(raw, "offset=0 size=5 wsize=0") {
		t.Fatalf("missing first checksum range: %q", raw)
	}
	if !strings.Contains(raw, "offset=6 size=5 wsize=0") {
		t.Fatalf("missing second checksum range: %q", raw)
	}
	if !strings.Contains(raw, "file-hash="+encoding.FormatXXH128HashToken(xxh3.Hash128([]byte("hello")))) {
		t.Fatalf("missing xxh128 checksum token: %q", raw)
	}
	if !strings.Contains(raw, "file-hash="+encoding.FormatXXH64HashToken(xxh3.Hash([]byte("world")))) {
		t.Fatalf("missing xxh64 checksum token: %q", raw)
	}
	if got := strings.Count(raw, "next=0"); got != 2 {
		t.Fatalf("expected terminal trailers for each range, got %d output=%q", got, raw)
	}
}

func TestHandleCXSUMRangeBounds(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(filePath, []byte("hello"), 0600); err != nil {
		t.Fatal(err)
	}
	req, err := ParseRequest([]byte("CXSUM tx1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, algo := range []string{"none", "xxh128", "xxh64"} {
		for _, tc := range []struct {
			name         string
			offset, size int64
			invalid      bool
		}{
			{"within file", 1, 4, false},
			{"empty at eof", 5, 0, false},
			{"past eof", 5, 1, true},
			{"offset past eof", 6, 0, true},
			{"overflow", 1, 1<<63 - 1, true},
		} {
			t.Run(algo+"/"+tc.name, func(t *testing.T) {
				var out bytes.Buffer
				body := framedItemBody(t, fmt.Sprintf(`fd=1 "/file" offset=%d size=%d algo=%s`, tc.offset, tc.size, algo))
				err := handleCXSUMWithInput(context.Background(), req, body, &out, &mockDeps{filePath: filePath})
				if tc.invalid {
					var pe protocolErr
					if !errors.As(err, &pe) || pe.code != "BAD_REQUEST" || out.Len() != 0 {
						t.Fatalf("err=%v output=%q", err, out.String())
					}
				} else if err != nil || strings.Count(out.String(), "FX/1 ") != 1 {
					t.Fatalf("err=%v output=%q", err, out.String())
				}
			})
		}
	}
}

// failingReaderAt fails every read, so a hash that reads nothing passes.
type failingReaderAt struct{}

func (failingReaderAt) ReadAt([]byte, int64) (int, error) { return 0, os.ErrClosed }

func TestHashChecksumRangeComputesOnlyRequested(t *testing.T) {
	data := []byte("hello world")
	want128 := encoding.FormatXXH128HashToken(xxh3.Hash128(data[6:]))
	want64 := encoding.FormatXXH64HashToken(xxh3.Hash(data[6:]))
	// One set of hashers serves every case, as one request's items share it.
	var hashers checksumHashers
	for _, tc := range []struct {
		algorithms []string
		want       []string
	}{
		{[]string{"xxh128"}, []string{want128}},
		{[]string{"xxh64"}, []string{want64}},
		{[]string{"xxh128", "xxh64"}, []string{want128, want64}},
	} {
		got, err := hashChecksumRange(bytes.NewReader(data), 6, 5, tc.algorithms, make([]byte, 2), &hashers)
		if err != nil {
			t.Fatalf("%v: %v", tc.algorithms, err)
		}
		if len(tc.algorithms) == 1 && tc.algorithms[0] == "xxh128" && hashers.h64 != nil {
			t.Fatal("allocated the unrequested xxh64 hasher")
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Fatalf("%v: got %v want %v", tc.algorithms, got, tc.want)
		}
	}
	got, err := hashChecksumRange(failingReaderAt{}, 0, 5, nil, nil, &hashers)
	if err != nil || len(got) != 0 {
		t.Fatalf("no algorithms: got %v, %v; want no tokens and no read", got, err)
	}
}

// FuzzChecksumFrameMatchesWriteFrame checks that CXSUM's pooled frame
// encoder writes exactly what encoding.WriteFrame writes for the same frame,
// so clients see no change in format or frame hash.
func FuzzChecksumFrameMatchesWriteFrame(f *testing.F) {
	f.Add(uint64(1), int64(0), int64(5), int64(1700000000000), uint8(1), "root", "0644")
	f.Add(uint64(1<<63), int64(6), int64(0), int64(0), uint8(3), "a user", "0755")
	f.Add(uint64(7), int64(1<<40), int64(4<<20), int64(-1), uint8(0), "", "")
	f.Fuzz(func(t *testing.T, fileID uint64, offset, size, ts int64, algos uint8, user, mode string) {
		hashes := []string{}
		if algos&1 != 0 {
			hashes = append(hashes, encoding.FormatXXH128HashToken(xxh3.Hash128([]byte(user))))
		}
		if algos&2 != 0 {
			hashes = append(hashes, encoding.FormatXXH64HashToken(xxh3.Hash([]byte(mode))))
		}
		meta := encoding.FileFrameMetadata{Size: size, MtimeNS: ts, Mode: mode, UID: "1000", GID: "1000", User: user, Group: user}
		headerHash := "none:0"
		if len(hashes) > 0 {
			headerHash = hashes[0]
		}
		var want bytes.Buffer
		if _, err := encoding.WriteFrame(&want, encoding.WriteArgs{
			FileID: fileID, Offset: offset, Size: size, Comp: "none", HeaderHash: headerHash,
			HeaderTS: ts, TrailerTS: ts, FileHashes: hashes, Metadata: &meta,
		}); err != nil {
			t.Fatal(err)
		}
		got := appendChecksumFrame([]byte("prefix"), fileID, offset, size, hashes, ts, &meta)
		if !bytes.Equal(got[len("prefix"):], want.Bytes()) {
			t.Fatalf("frame mismatch:\n got %q\nwant %q", got[len("prefix"):], want.Bytes())
		}
	})
}

func TestHandleCXSUMItemsKeepOrder(t *testing.T) {
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i * 7)
	}
	filePath := filepath.Join(t.TempDir(), "cxsum.bin")
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"range", "lookup", "open"} {
		for _, failAt := range []int{-1, 0, 5, 12, 40, 73, 99} {
			deps := &mockDeps{filePath: filePath, missingIDs: map[uint64]bool{}, openErrors: map[uint64]error{}}
			var items []string
			for i := range 100 {
				offset := i
				if i == failAt {
					switch kind {
					case "range":
						offset = len(data) + 1
					case "lookup":
						deps.missingIDs[uint64(i+1)] = true
					case "open":
						deps.openErrors[uint64(i+1)] = &FileLookupError{Code: http.StatusNotFound, Msg: "file disappeared"}
					}
				}
				items = append(items, fmt.Sprintf(`fd=%d "/tmp/a" offset=%d size=%d algo=xxh128`, i+1, offset, 1+i%50))
			}
			req, err := ParseRequest([]byte(`CXSUM tx1`))
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err = handleCXSUMWithInput(context.Background(), req, framedItemBody(t, items...), &out, deps)
			want, opens := 100, 100
			if failAt >= 0 {
				want, opens = failAt, failAt+1
				code := "BAD_REQUEST"
				if kind != "range" {
					code = "NOT_FOUND"
				}
				if kind == "lookup" {
					opens--
				}
				var pe protocolErr
				if !errors.As(err, &pe) || pe.code != code {
					t.Fatalf("%s failAt=%d: err=%v, want %s", kind, failAt, err, code)
				}
			} else if err != nil {
				t.Fatalf("handleCXSUMWithInput: %v", err)
			}
			if deps.refBatches != 1 || deps.openFileCalls != opens {
				t.Fatalf("%s failAt=%d: lookups=%d opens=%d, want 1, %d", kind, failAt, deps.refBatches, deps.openFileCalls, opens)
			}
			for _, fd := range deps.openedFiles {
				if _, err := fd.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("%s failAt=%d: file left open: %v", kind, failAt, err)
				}
			}
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if out.Len() == 0 {
				lines = nil
			}
			if len(lines) != 2*want {
				t.Fatalf("%s failAt=%d: got %d lines, want %d frames", kind, failAt, len(lines), want)
			}
			for i := range want {
				size := 1 + i%50
				wantHeader := fmt.Sprintf("FX/1 %d offset=%d size=%d ", i+1, i, size)
				wantHash := "file-hash=" + encoding.FormatXXH128HashToken(xxh3.Hash128(data[i:i+size]))
				if !strings.HasPrefix(lines[2*i], wantHeader) || !strings.Contains(lines[2*i+1], wantHash) {
					t.Fatalf("%s failAt=%d: frame %d out of order or wrong:\n%s\n%s", kind, failAt, i, lines[2*i], lines[2*i+1])
				}
			}
		}
	}
}

func TestHandleCXSUMCancelledBeforeLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, err := ParseRequest([]byte("CXSUM tx1"))
	if err != nil {
		t.Fatal(err)
	}
	deps := &mockDeps{}
	var out bytes.Buffer
	err = handleCXSUMWithInput(ctx, req, framedItemBody(t, `fd=1 "/file" size=0`), &out, deps)
	if !errors.Is(err, context.Canceled) || deps.refBatches != 0 || deps.openFileCalls != 0 || out.Len() != 0 {
		t.Fatalf("err=%v lookups=%d opens=%d output=%q", err, deps.refBatches, deps.openFileCalls, out.String())
	}
}

// Disk fault sites in SEND and CXSUM.

// TestOpenFaultsMapLikeStoreErrors checks an injected open failure reports
// as the store reports a real one: a missing file as NOT_FOUND, anything
// else as an internal error.
func TestOpenFaultsMapLikeStoreErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(path, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ spec, code string }{
		{"open.enoent=1", mapHTTPErrorCode(http.StatusNotFound)},
		{"open.eio=1", mapHTTPErrorCode(http.StatusInternalServerError)},
	} {
		inj := faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyDisk: tc.spec}})
		ctx := faults.NewContext(context.Background(), inj)
		req, err := ParseRequest([]byte("CXSUM tx1"))
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		err = handleCXSUMWithInput(ctx, req, framedItemBody(t, `fd=1 "/a" offset=0 size=5 algo=xxh128`), &out, &mockDeps{filePath: path})
		var perr protocolErr
		if !errors.As(err, &perr) || perr.code != tc.code {
			t.Fatalf("%s: CXSUM error %v, want code %s", tc.spec, err, tc.code)
		}
		o := unbufferedSendOutput(&out)
		o.faults = inj
		deps := &mockDeps{filePath: path}
		_, err = streamSendItem(context.Background(), o, deps, "tx1", sendItem{FileID: 1, Comp: "none", Path: path, Mode: loadStrategyFast},
			mustFileRef(t, deps, 1, path), defaultZeroCopyMinFrameBytes)
		if !errors.As(err, &perr) || perr.code != tc.code {
			t.Fatalf("%s: SEND error %v, want code %s", tc.spec, err, tc.code)
		}
	}
}

// TestDiskFaultsNameTheirFile checks SEND and CXSUM pass each item's file ID
// and window offset to their disk fault sites, which key faults per file and
// offset; a fault event reports both.
func TestDiskFaultsNameTheirFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(path, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	type at struct {
		kind   string
		file   uint64
		offset int64
	}
	injector := func(spec string) (*faults.Injector, *[]at) {
		sink := events.NewSink()
		var got []at
		sink.Subscribe(func(e events.Event) {
			if e.Name == "fault" {
				k, _ := e.Get("kind")
				off, _ := e.Get("offset")
				got = append(got, at{k.(string), uint64(e.Int("file")), off.(int64)})
			}
		})
		return faults.New(faults.Config{Seed: 1, Specs: map[faults.Family]string{faults.FamilyDisk: spec}, Events: sink}), &got
	}
	for _, spec := range []string{"open.eio=1", "read.eio=1"} {
		kind := "disk." + strings.Split(spec, "=")[0]
		inj, got := injector(spec)
		var out bytes.Buffer
		o := unbufferedSendOutput(&out)
		o.faults = inj
		deps := &mockDeps{filePath: path}
		_, _ = streamSendItem(context.Background(), o, deps, "tx1", sendItem{FileID: 7, Offset: 3, Comp: "none", Path: path, Mode: loadStrategyFast},
			mustFileRef(t, deps, 7, path), defaultZeroCopyMinFrameBytes)
		if want := (at{kind, 7, 3}); len(*got) != 1 || (*got)[0] != want {
			t.Fatalf("SEND %s: fault events %v, want [%v]", spec, *got, want)
		}

		inj, got = injector(spec)
		req, err := ParseRequest([]byte("CXSUM tx1"))
		if err != nil {
			t.Fatal(err)
		}
		ctx := faults.NewContext(context.Background(), inj)
		_ = handleCXSUMWithInput(ctx, req, framedItemBody(t, `fd=9 "/a" offset=2 size=5 algo=xxh128`), &out, &mockDeps{filePath: path})
		if want := (at{kind, 9, 2}); len(*got) != 1 || (*got)[0] != want {
			t.Fatalf("CXSUM %s: fault events %v, want [%v]", spec, *got, want)
		}
	}
}
