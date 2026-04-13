package filexfer

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type progressStatusFixture struct {
	mu sync.Mutex
	s  ProgressStatus
}

func (f *progressStatusFixture) set(s ProgressStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.s = s
}

func (f *progressStatusFixture) get() ProgressStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.s
}

func waitForFileContent(t *testing.T, path string, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil && string(raw) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read progress file %s: %v", path, err)
	}
	t.Fatalf("progress file mismatch:\n got=%q\nwant=%q", string(raw), want)
}

func TestStartProgressFileWriterTruncatesThenAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	if err := os.WriteFile(path, []byte("stale\n"), 0o644); err != nil {
		t.Fatalf("seed progress file: %v", err)
	}

	status := &progressStatusFixture{}
	s1 := ProgressStatus{Source: "s", DoneBytes: 10, TotalBytes: 100}
	status.set(s1)
	stop := StartProgressFileWriter(context.Background(), []ProgressTarget{{Path: path, Format: ProgressFormatJSON}}, 10*time.Millisecond, status.get)
	t.Cleanup(func() { stop(false) })

	json1 := FormatProgressStatusLine(s1.Source, s1.TxID, s1.DoneFiles, s1.TotalFiles, s1.DoneBytes, s1.TotalBytes)
	waitForFileContent(t, path, json1+"\n")

	s2 := ProgressStatus{Source: "s", DoneBytes: 20, TotalBytes: 100}
	status.set(s2)
	json2 := FormatProgressStatusLine(s2.Source, s2.TxID, s2.DoneFiles, s2.TotalFiles, s2.DoneBytes, s2.TotalBytes)
	waitForFileContent(t, path, json1+"\n"+json2+"\n")
}

func TestStartProgressFileWriterDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	s := ProgressStatus{Source: "s", DoneBytes: 42, TotalBytes: 100}
	status.set(s)
	stop := StartProgressFileWriter(context.Background(), []ProgressTarget{{Path: path, Format: ProgressFormatInt}}, 10*time.Millisecond, status.get)
	t.Cleanup(func() { stop(false) })

	waitForFileContent(t, path, "42\n")
	time.Sleep(40 * time.Millisecond)
	waitForFileContent(t, path, "42\n")

	status.set(ProgressStatus{Source: "s", DoneBytes: 43, TotalBytes: 100})
	waitForFileContent(t, path, "42\n43\n")
}

func TestStartProgressFileWriterStopSuccessWritesFinal100(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	status.set(ProgressStatus{Source: "s", DoneBytes: 64, TotalBytes: 100})

	stop := StartProgressFileWriter(context.Background(), []ProgressTarget{{Path: path, Format: ProgressFormatInt}}, time.Hour, status.get)
	stop(true)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read progress file: %v", err)
	}
	if got, want := string(raw), "100\n"; got != want {
		t.Fatalf("final progress mismatch: got %q want %q", got, want)
	}
}

func TestStartProgressFileWriterStopFailureWritesCurrentPct(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	status.set(ProgressStatus{Source: "s", DoneBytes: 64, TotalBytes: 100})

	stop := StartProgressFileWriter(context.Background(), []ProgressTarget{{Path: path, Format: ProgressFormatInt}}, time.Hour, status.get)
	stop(false)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read progress file: %v", err)
	}
	if got, want := string(raw), "64\n"; got != want {
		t.Fatalf("final progress mismatch: got %q want %q", got, want)
	}
}

func TestFormatProgressStatusLine(t *testing.T) {
	got := FormatProgressStatusLine("server", "1ef1a42b", 9121, 10127, 18_350_000_000, 18_790_000_000)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("expected valid json, got %q err=%v", got, err)
	}
	if parsed["source"] != "server" {
		t.Fatalf("expected server source, got %#v", parsed["source"])
	}
	if parsed["txid"] != "1ef1a42b" {
		t.Fatalf("expected txid, got %#v", parsed["txid"])
	}
	files, ok := parsed["files"].(map[string]any)
	if !ok {
		t.Fatalf("expected files object, got %#v", parsed["files"])
	}
	if files["percent"] != "90.1" {
		t.Fatalf("expected files percent string, got %#v", files["percent"])
	}
	bytesObj, ok := parsed["bytes"].(map[string]any)
	if !ok {
		t.Fatalf("expected bytes object, got %#v", parsed["bytes"])
	}
	if bytesObj["percent"] != "97.7" {
		t.Fatalf("expected bytes percent string, got %#v", bytesObj["percent"])
	}
	if _, ok := parsed["bytes_human"].(map[string]any); !ok {
		t.Fatalf("expected bytes_human object, got %#v", parsed["bytes_human"])
	}
}

func TestStartProgressFileWriterEmptyTargets(t *testing.T) {
	stop := StartProgressFileWriter(context.Background(), nil, 10*time.Millisecond, func() ProgressStatus {
		return ProgressStatus{Source: "nope", DoneBytes: 50, TotalBytes: 100}
	})
	stop(true)
}

func TestStartProgressFileWriterIntFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	status.set(ProgressStatus{Source: "s", DoneBytes: 42, TotalBytes: 100})
	stop := StartProgressFileWriter(context.Background(), []ProgressTarget{{Path: path, Format: ProgressFormatInt}}, 10*time.Millisecond, status.get)
	t.Cleanup(func() { stop(false) })

	waitForFileContent(t, path, "42\n")
	status.set(ProgressStatus{Source: "s", DoneBytes: 80, TotalBytes: 100})
	waitForFileContent(t, path, "42\n80\n")
}

func TestStartProgressFileWriterMultipleTargets(t *testing.T) {
	dir := t.TempDir()
	pathJSON := filepath.Join(dir, "json.txt")
	pathInt := filepath.Join(dir, "int.txt")
	status := &progressStatusFixture{}
	s := ProgressStatus{Source: "s", DoneBytes: 10, TotalBytes: 100}
	status.set(s)
	targets := []ProgressTarget{
		{Path: pathJSON, Format: ProgressFormatJSON},
		{Path: pathInt, Format: ProgressFormatInt},
	}
	stop := StartProgressFileWriter(context.Background(), targets, 10*time.Millisecond, status.get)
	t.Cleanup(func() { stop(false) })

	jsonLine := FormatProgressStatusLine(s.Source, s.TxID, s.DoneFiles, s.TotalFiles, s.DoneBytes, s.TotalBytes)
	waitForFileContent(t, pathJSON, jsonLine+"\n")
	waitForFileContent(t, pathInt, "10\n")
}

func TestStartProgressFileWriterSamePathMultipleFormats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.txt")
	status := &progressStatusFixture{}
	s := ProgressStatus{Source: "s", DoneBytes: 10, TotalBytes: 100}
	status.set(s)
	targets := []ProgressTarget{
		{Path: path, Format: ProgressFormatJSON},
		{Path: path, Format: ProgressFormatInt},
	}
	stop := StartProgressFileWriter(context.Background(), targets, 10*time.Millisecond, status.get)
	t.Cleanup(func() { stop(false) })

	jsonLine := FormatProgressStatusLine(s.Source, s.TxID, s.DoneFiles, s.TotalFiles, s.DoneBytes, s.TotalBytes)
	waitForFileContent(t, path, jsonLine+"\n10\n")
}

func TestStartProgressFileWriterStdoutTarget(t *testing.T) {
	var buf bytes.Buffer
	status := &progressStatusFixture{}
	status.set(ProgressStatus{Source: "s", DoneBytes: 55, TotalBytes: 100})
	targets := []ProgressTarget{
		{Path: "-", Format: ProgressFormatInt, Stdout: &buf},
	}
	stop := StartProgressFileWriter(context.Background(), targets, 10*time.Millisecond, status.get)
	stop(false)
	got := buf.String()
	if !strings.Contains(got, "55\n") {
		t.Fatalf("stdout target: got %q, want it to contain %q", got, "55\n")
	}
}
