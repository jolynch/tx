package main

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/zeebo/xxh3"

	"github.com/jolynch/tx/internal/filexfer/store"
)

func BenchmarkTransferStoreConcurrentHotPaths(b *testing.B) {
	s := store.NewStore()
	b.Cleanup(s.Close)

	const numTransfers = 32
	const filesPerTransfer = 8
	transferIDs := make([]string, 0, numTransfers)
	for i := 0; i < numTransfers; i++ {
		transfer, err := s.NewTransfer("/tmp/bench-"+strconv.Itoa(i), filesPerTransfer, int64(filesPerTransfer*1024))
		if err != nil {
			b.Fatalf("NewTransfer failed: %v", err)
		}
		updates := make([]store.TransferFileStateUpdate, 0, filesPerTransfer)
		for fileID := 0; fileID < filesPerTransfer; fileID++ {
			updates = append(updates, store.TransferFileStateUpdate{
				FileID:   uint64(fileID),
				PathHash: xxh3.Hash128([]byte(transfer.ID + "-" + strconv.Itoa(fileID))),
				FileSize: 1024,
			})
		}
		s.RegisterTransferFileStates(transfer.ID, updates, store.TransferStateRunning)
		transferIDs = append(transferIDs, transfer.ID)
	}

	var counter atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			txferID := transferIDs[int(n)%len(transferIDs)]
			fileID := uint64((n / uint64(len(transferIDs))) % filesPerTransfer)
			endBytes := int64((n%8)+1) * 64
			token := "xxh128:" + strconv.FormatUint(n, 16)

			if !s.SetTransferFileState(txferID, fileID, store.TransferStateRunning) {
				b.Fatalf("SetTransferFileState returned false")
			}
			if !s.SetTransferFileWindowHash(txferID, fileID, endBytes, token) {
				b.Fatalf("SetTransferFileWindowHash returned false")
			}
			if !s.AcknowledgeTransferFiles([]store.AckEntry{{TxferID: txferID, FileID: fileID, AckBytes: endBytes}}) {
				b.Fatalf("AcknowledgeTransferFiles returned false")
			}
		}
	})
}

// BenchmarkTransferStoreAckBatch applies two ACK requests' worth of entries
// to a transfer of 4096 1 KiB files, as one AcknowledgeTransferFiles call
// each: a partial ACK, then a completing one, so every file goes through
// progress and then completion. Creating and deleting the transfer is
// untimed.
func BenchmarkTransferStoreAckBatch(b *testing.B) {
	const files = 4096
	const fileSize = 1024
	s := store.NewStore()
	b.Cleanup(s.Close)
	updates := make([]store.TransferFileStateUpdate, files)
	for i := range updates {
		updates[i] = store.TransferFileStateUpdate{
			FileID:   uint64(i),
			PathHash: xxh3.Hash128([]byte("/tmp/bench-ack/f" + strconv.Itoa(i))),
			FileSize: fileSize,
		}
	}
	partial := make([]store.AckEntry, files)
	complete := make([]store.AckEntry, files)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		transfer, err := s.NewTransfer("/tmp/bench-ack", files, files*fileSize)
		if err != nil {
			b.Fatalf("NewTransfer failed: %v", err)
		}
		s.RegisterTransferFileStates(transfer.ID, updates, store.TransferStateRunning)
		for fileID := range files {
			partial[fileID] = store.AckEntry{TxferID: transfer.ID, FileID: uint64(fileID), AckBytes: fileSize / 2}
			complete[fileID] = store.AckEntry{TxferID: transfer.ID, FileID: uint64(fileID), AckBytes: fileSize}
		}
		b.StartTimer()
		if !s.AcknowledgeTransferFiles(partial) || !s.AcknowledgeTransferFiles(complete) {
			b.Fatal("AcknowledgeTransferFiles returned false")
		}
		b.StopTimer()
		if summary, ok := s.GetTransferSummary(transfer.ID); !ok || summary.Done != files {
			b.Fatalf("transfer counted %d of %d files done", summary.Done, files)
		}
		s.DeleteTransfer(transfer.ID)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/(2*files), "ns/ack")
}
