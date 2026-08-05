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
