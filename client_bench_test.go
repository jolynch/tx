package tx

import (
	"fmt"
	"strings"
	"testing"
)

// benchTargets builds n SEND targets. pathDepth controls how long each path is,
// which is what actually drives request body size — the rest of an item line is
// a fixed handful of bytes.
func benchTargets(n, pathDepth int) []FetchFileTarget {
	targets := make([]FetchFileTarget, n)
	prefix := "/remote/" + strings.Repeat("nested-directory/", pathDepth)
	for i := range targets {
		targets[i] = FetchFileTarget{
			FileID:   uint64(i + 1),
			FullPath: prefix + fmt.Sprintf("file-%07d.bin", i),
			Comp:     "none",
		}
	}
	return targets
}

// Measure bounded request construction for short and long paths.
func BenchmarkSplitEncodedRequests(b *testing.B) {
	limits := defaultClientRequestLimits()
	for _, tc := range []struct {
		name  string
		depth int
	}{
		{"short-paths", 1},
		{"long-paths", 12},
	} {
		targets := benchTargets(100000, tc.depth)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err := visitEncodedRequests(targets, limits, fetchTargetItemBytes, func(requestChunk) error { return nil })
				if err != nil {
					b.Fatalf("split: %v", err)
				}
			}
		})
	}
}
