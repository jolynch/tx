package filexfer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"golang.org/x/sys/unix"
)

type ProgressFormat string

const (
	ProgressFormatJSON ProgressFormat = "json"
	ProgressFormatInt  ProgressFormat = "int"
)

type ProgressTarget struct {
	Path   string
	Format ProgressFormat
	Stdout io.Writer // when Path=="-" and non-nil, write here instead of os.Stdout
}

const (
	progressStatusBytesWidth = 10
)

// ProgressStatus holds the raw counters that callers provide each tick.
// The progress writer formats these per-target (JSON for json targets,
// integer percentage for int targets).
type ProgressStatus struct {
	Source     string
	TxID       string
	DoneFiles  uint64
	TotalFiles uint64
	DoneBytes  int64
	TotalBytes int64
}

func (s ProgressStatus) pct() int {
	if s.TotalBytes <= 0 {
		return 0
	}
	pct := int(s.DoneBytes * 100 / s.TotalBytes)
	return normalizeProgressPct(pct)
}

func formatProgressOutput(format ProgressFormat, s ProgressStatus) string {
	pct := s.pct()
	switch format {
	case ProgressFormatInt:
		return fmt.Sprintf("%d\n", pct)
	default:
		return fmt.Sprintf("%s\n", FormatProgressStatusLine(s.Source, s.TxID, s.DoneFiles, s.TotalFiles, s.DoneBytes, s.TotalBytes))
	}
}

// StartProgressFileWriter starts a background goroutine that periodically
// writes progress records to one or more targets.
// File targets are opened non-blocking so FIFOs without a reader don't hang.
// If a file/pipe doesn't exist or can't be opened, it silently retries
// on the next tick. Targets with Path "-" write to Stdout (or os.Stdout).
//
// The returned stop function must be called when the operation completes.
// The first successful write to each file target truncates any existing
// content; later writes append. If success is true the final write forces
// the percentage to 100; otherwise the current percentage from statusFn is
// written. Returns a no-op stop if targets is empty.
func StartProgressFileWriter(ctx context.Context, targets []ProgressTarget, interval time.Duration, statusFn func() ProgressStatus) (stop func(success bool)) {
	if len(targets) == 0 {
		return func(bool) {}
	}

	needsJSON := false
	for _, t := range targets {
		if t.Format != ProgressFormatInt {
			needsJSON = true
			break
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)

	wroteFile := make(map[string]bool, len(targets))

	writeOnce := func(s ProgressStatus) {
		pendingPaths := make([]string, 0, len(targets))
		pendingOutput := make(map[string]string, len(targets))

		for _, target := range targets {
			output := formatProgressOutput(target.Format, s)
			if target.Path == "-" {
				w := target.Stdout
				if w == nil {
					w = os.Stdout
				}
				fmt.Fprint(w, output)
				continue
			}

			if _, ok := pendingOutput[target.Path]; !ok {
				pendingPaths = append(pendingPaths, target.Path)
			}
			pendingOutput[target.Path] += output
		}

		for _, path := range pendingPaths {
			flags := unix.O_WRONLY | unix.O_CREAT | unix.O_NONBLOCK
			if !wroteFile[path] {
				flags |= unix.O_TRUNC
			} else {
				flags |= unix.O_APPEND
			}
			fd, err := unix.Open(path, flags, 0644)
			if err != nil {
				continue
			}
			f := os.NewFile(uintptr(fd), path)
			fmt.Fprint(f, pendingOutput[path])
			f.Close()
			wroteFile[path] = true
		}
	}

	var lastPct int = -1
	var lastJSON string

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s := statusFn()
				pct := s.pct()
				jsonStr := ""
				if needsJSON {
					jsonStr = FormatProgressStatusLine(s.Source, s.TxID, s.DoneFiles, s.TotalFiles, s.DoneBytes, s.TotalBytes)
				}
				if pct != lastPct || jsonStr != lastJSON {
					writeOnce(s)
					lastPct = pct
					lastJSON = jsonStr
				}
			}
		}
	}()

	return func(success bool) {
		cancel()
		wg.Wait()
		s := statusFn()
		if success {
			s.DoneBytes = s.TotalBytes
			s.DoneFiles = s.TotalFiles
		}
		writeOnce(s)
	}
}

type progressStatusCounts struct {
	Done    uint64 `json:"done"`
	Total   uint64 `json:"total"`
	Percent string `json:"percent"`
}

type progressStatusBytes struct {
	Done    int64  `json:"done"`
	Total   int64  `json:"total"`
	Percent string `json:"percent"`
}

type progressStatusHumanBytes struct {
	Done  string `json:"done"`
	Total string `json:"total"`
}

type progressStatusJSON struct {
	Source     string                   `json:"source,omitempty"`
	TxID       string                   `json:"txid,omitempty"`
	Files      progressStatusCounts     `json:"files"`
	Bytes      progressStatusBytes      `json:"bytes"`
	BytesHuman progressStatusHumanBytes `json:"bytes_human"`
}

func FormatProgressStatusLine(source string, txid string, doneFiles uint64, totalFiles uint64, doneBytes int64, totalBytes int64) string {
	if totalFiles > 0 && doneFiles > totalFiles {
		doneFiles = totalFiles
	}
	if doneBytes < 0 {
		doneBytes = 0
	}
	if totalBytes < 0 {
		totalBytes = 0
	}
	if totalBytes > 0 && doneBytes > totalBytes {
		doneBytes = totalBytes
	}

	var filesPct float64
	if totalFiles > 0 {
		filesPct = float64(doneFiles) * 100 / float64(totalFiles)
	}
	var bytesPct float64
	if totalBytes > 0 {
		bytesPct = float64(doneBytes) * 100 / float64(totalBytes)
	}

	payload := progressStatusJSON{
		Source: source,
		TxID:   txid,
		Files: progressStatusCounts{
			Done:    doneFiles,
			Total:   totalFiles,
			Percent: formatProgressPercent(filesPct),
		},
		Bytes: progressStatusBytes{
			Done:    doneBytes,
			Total:   totalBytes,
			Percent: formatProgressPercent(bytesPct),
		},
		BytesHuman: progressStatusHumanBytes{
			Done:  encoding.HumanBytesFixedWidth(doneBytes, progressStatusBytesWidth),
			Total: encoding.HumanBytesFixedWidth(totalBytes, progressStatusBytesWidth),
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return `{"source":"unknown","error":"marshal"}`
	}
	return string(raw)
}

func formatProgressPercent(pct float64) string {
	return fmt.Sprintf("%4.1f", pct)
}

func normalizeProgressPct(pct int) int {
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}
