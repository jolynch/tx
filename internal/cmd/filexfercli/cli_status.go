package filexfercli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jolynch/tx"
	"github.com/jolynch/tx/internal/cliflags"
	"github.com/jolynch/tx/internal/filexfer/encoding"
)

func runStatusCLI(serverURL string, args []string, stdout io.Writer, stderr io.Writer) int {
	cf := cliflags.New("status")
	cf.SetOutput(stderr)
	cf.FlagSet().Usage = func() {
		fmt.Fprintln(stderr, "usage: tx recv [addr] status [--tid <id>] [LOCAL_DST]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Query and monitor transfer progress.")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Modes:")
		fmt.Fprintln(stderr, "  status LOCAL_DST       Discover transfer from .tx/ state and poll until complete")
		fmt.Fprintln(stderr, "  status --tid <id>      Poll a transfer by ID (server-side progress only)")
		fmt.Fprintln(stderr, "  status                 List all active transfers on the server")
		fmt.Fprintln(stderr)
		cf.PrintDefaults(stderr)
	}
	var txferID string
	var encryptMode string
	var keysDir string
	var authTokens []string
	cf.StringVar(&txferID, "", "tid", "", "Transfer ID")
	cf.StringVar(&encryptMode, "", "encrypt", "", "Encryption algorithm: none|auto|aes|chacha20 (default: none)")
	cf.StringVar(&keysDir, "k", "keys", "", "Persistent age keys directory (default: ephemeral)")
	cf.StringSliceVar(&authTokens, "t", "auth-token", "Client auth token presented in encrypted AUTH blob; repeatable")
	if err := cf.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if cf.NArg() > 1 {
		fmt.Fprintln(stderr, "status accepts at most one positional argument: LOCAL_DST")
		return 2
	}
	agePublicKey, ageIdentity, resolvedEncMode, err := resolveEncryptionOptionsWithKeys(encryptMode, keysDir)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --encrypt: %v\n", err)
		return 2
	}
	if err := validateAuthTokens(authTokens, resolvedEncMode); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	client := tx.NewClient(serverURL, tx.WithClientAgePublicKey(agePublicKey), tx.WithClientAgeIdentity(ageIdentity), tx.WithEncryptMode(resolvedEncMode), tx.WithClientAuthTokens(authTokens...))
	defer client.Close()

	// Mode 1: LOCAL_DST given — discover transfer from .tx/ state.
	if cf.NArg() == 1 {
		localDst := cf.Arg(0)
		ps, err := newTxState(localDst)
		if err != nil {
			fmt.Fprintf(stderr, "invalid target directory: %v\n", err)
			return 2
		}
		if _, err := os.Stat(ps.ServerManifestPath); os.IsNotExist(err) {
			fmt.Fprintf(stderr, "No active transfer for %s\n", localDst)
			return 0
		}
		manifest, err := tx.LoadManifest(ps.ServerManifestPath)
		if err != nil {
			fmt.Fprintf(stderr, "load manifest failed: %v\n", err)
			return 1
		}
		txferID = manifest.TransferID
		return pollTransferStatus(client, txferID, manifest, ps.ProgressPath, stdout, stderr)
	}

	// Mode 2: --tid given — poll by transfer ID (no local progress).
	if txferID != "" {
		return pollTransferStatus(client, txferID, nil, "", stdout, stderr)
	}

	// Mode 3: no args — list all active transfers.
	listResp, err := client.ListStatuses(context.Background(), tx.ListStatusesRequest{})
	if err != nil {
		fmt.Fprintf(stderr, "status failed: %v\n", err)
		return 1
	}
	if len(listResp.Statuses) == 0 {
		fmt.Fprintln(stdout, "No active transfers")
		return 0
	}
	for _, s := range listResp.Statuses {
		fmt.Fprintf(
			stdout,
			"[%s] source=[%s] files=[%d/%d](%.1f%%) [%s/%s](%.1f%%)\n",
			s.TransferID,
			s.Directory,
			s.Done, s.NumFiles, s.PercentFiles,
			encoding.HumanBytesFixedWidth(s.DoneSize, fixedWidthProgressBytesWidth),
			encoding.HumanBytesFixedWidth(s.TotalSize, fixedWidthProgressBytesWidth), s.PercentBytes,
		)
	}
	return 0
}

func computeLocalProgress(manifest *tx.Manifest, progressPath string) (doneFiles int, totalFiles int, doneBytes int64, totalBytes int64) {
	if manifest == nil || progressPath == "" {
		return
	}
	totalFiles = len(manifest.Entries)
	for _, e := range manifest.Entries {
		totalBytes += e.Size
	}
	progressState, err := loadProgressState(progressPath)
	if err != nil {
		return
	}
	for _, e := range manifest.Entries {
		if p, ok := progressState[e.ID]; ok {
			ack := p.AckBytes
			if ack > e.Size {
				ack = e.Size
			}
			doneBytes += ack
			if ack >= e.Size {
				doneFiles++
			}
		}
	}
	return
}

func pollTransferStatus(client *tx.Client, txferID string, manifest *tx.Manifest, progressPath string, stdout io.Writer, stderr io.Writer) int {
	hasLocal := manifest != nil && progressPath != ""
	var totalBytes int64
	var totalFiles int
	if manifest != nil {
		totalFiles = len(manifest.Entries)
		for _, e := range manifest.Entries {
			totalBytes += e.Size
		}
	}

	ticker := time.NewTicker(defaultVerboseProgressInterval)
	defer ticker.Stop()
	var prevDoneSize int64
	prevTime := time.Now()
	startTime := prevTime

	// Track high-water marks for local progress so it never regresses
	// (the progress file may be cleaned up when the copy finishes).
	var peakLocalDoneFiles int
	var peakLocalDoneBytes int64

	for {
		statusResp, statusErr := client.GetStatus(context.Background(), tx.GetStatusRequest{
			TransferID: txferID,
		})
		if statusErr != nil {
			if strings.Contains(statusErr.Error(), "NOT_FOUND") {
				fmt.Fprintf(stderr, "Transfer %s expired on server\n", txferID)
				return 0
			}
			fmt.Fprintf(stderr, "status failed: %v\n", statusErr)
			return 1
		}
		s := statusResp.Status
		now := time.Now()
		dt := now.Sub(prevTime).Seconds()
		var rateBps float64
		if dt > 0 {
			rateBps = float64(s.DoneSize-prevDoneSize) / dt
		}
		prevDoneSize = s.DoneSize
		prevTime = now

		etaPart := ""
		if rateBps > 0 && s.TotalSize > s.DoneSize {
			remaining := float64(s.TotalSize - s.DoneSize)
			etaSec := remaining / rateBps
			etaPart = fmt.Sprintf(" eta=%s", fixedWidthETA(time.Duration(etaSec*float64(time.Second))))
		}
		fmt.Fprintf(
			stdout,
			"server: files=[%d/%d](%.1f%%) [%s/%s](%.1f%%) rate=%s%s\n",
			s.Done, s.NumFiles, s.PercentFiles,
			encoding.HumanBytesFixedWidth(s.DoneSize, fixedWidthProgressBytesWidth),
			encoding.HumanBytesFixedWidth(s.TotalSize, fixedWidthProgressBytesWidth),
			s.PercentBytes,
			encoding.HumanRateFixedWidth(rateBps, fixedWidthProgressRateWidth), etaPart,
		)
		if hasLocal {
			localDoneFiles, localTotalFiles, localDoneBytes, localTotalBytes := computeLocalProgress(manifest, progressPath)
			if localDoneFiles > peakLocalDoneFiles {
				peakLocalDoneFiles = localDoneFiles
			}
			if localDoneBytes > peakLocalDoneBytes {
				peakLocalDoneBytes = localDoneBytes
			}
			// If the server is done, the client must be done too — the progress
			// file may have already been cleaned up, so clamp peaks to totals.
			if s.PercentBytes >= 100.0 {
				peakLocalDoneFiles = localTotalFiles
				peakLocalDoneBytes = localTotalBytes
			}
			var localPctFiles, localPctBytes float64
			if localTotalFiles > 0 {
				localPctFiles = float64(peakLocalDoneFiles) * 100.0 / float64(localTotalFiles)
			}
			if localTotalBytes > 0 {
				localPctBytes = float64(peakLocalDoneBytes) * 100.0 / float64(localTotalBytes)
			}
			fmt.Fprintf(
				stdout,
				"client: files=[%d/%d](%.1f%%) [%s/%s](%.1f%%)\n",
				peakLocalDoneFiles, localTotalFiles, localPctFiles,
				encoding.HumanBytesFixedWidth(peakLocalDoneBytes, fixedWidthProgressBytesWidth),
				encoding.HumanBytesFixedWidth(localTotalBytes, fixedWidthProgressBytesWidth),
				localPctBytes,
			)
		}

		// Check for completion.
		serverDone := s.PercentBytes >= 100.0
		localDone := true
		if hasLocal {
			localDone = peakLocalDoneFiles >= totalFiles && peakLocalDoneBytes >= totalBytes
		}
		if serverDone && localDone {
			elapsed := time.Since(startTime)
			overallSpeed := 0.0
			if elapsed.Seconds() > 0 {
				overallSpeed = float64(s.TotalSize) / elapsed.Seconds()
			}
			fmt.Fprintf(
				stdout,
				"\ntransfer complete: tid=%s files=%d size=%s elapsed=%s speed=%s\n",
				txferID,
				s.NumFiles,
				encoding.HumanBytes(s.TotalSize),
				elapsed.Round(time.Millisecond),
				encoding.HumanRate(overallSpeed),
			)
			return 0
		}

		<-ticker.C
	}
}
