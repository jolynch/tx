// Package metrics holds counter primitives used by the tx client. The types
// here own the atomic state; callers interact through methods so the
// underlying representation can change without churning every call site.
package metrics

import "sync/atomic"

// ClientMetrics aggregates atomic counters for a single tx.Client. The zero
// value is ready to use and safe for concurrent access.
type ClientMetrics struct {
	syncConnFallbacks atomic.Int64
}

// ClientMetricsSnapshot is a point-in-time copy of the counters in ClientMetrics.
type ClientMetricsSnapshot struct {
	// SyncConnectionCount is the cumulative number of synchronous fallback
	// dials performed after the warmed TCP pool was exhausted.
	SyncConnectionCount int64
}

// IncSyncConnectionFallback records one synchronous fallback dial.
func (m *ClientMetrics) IncSyncConnectionFallback() {
	if m == nil {
		return
	}
	m.syncConnFallbacks.Add(1)
}

// Snapshot returns a copy of the current counter values.
func (m *ClientMetrics) Snapshot() ClientMetricsSnapshot {
	if m == nil {
		return ClientMetricsSnapshot{}
	}
	return ClientMetricsSnapshot{
		SyncConnectionCount: m.syncConnFallbacks.Load(),
	}
}
