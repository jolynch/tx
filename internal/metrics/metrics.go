// Package metrics holds counter primitives used by the tx client. The types
// here own the atomic state; callers interact through methods so the
// underlying representation can change without churning every call site.
package metrics

import "sync/atomic"

// ClientMetrics aggregates atomic counters for a single tx.Client. The zero
// value is ready to use and safe for concurrent access.
type ClientMetrics struct {
	syncConnFallbacks  atomic.Int64
	connReuses         atomic.Int64
	heartbeats         atomic.Int64
	heartbeatFailures  atomic.Int64
	lastHeartbeatRTTMS atomic.Int64
}

// ClientMetricsSnapshot is a point-in-time copy of the counters in ClientMetrics.
type ClientMetricsSnapshot struct {
	// SyncConnectionCount is the cumulative number of synchronous fallback
	// dials performed after the warmed TCP pool was exhausted.
	SyncConnectionCount int64
	// ConnectionReuseCount is the cumulative number of kept-alive
	// connections returned to the pool for reuse after a clean response.
	ConnectionReuseCount int64
	// HeartbeatCount is the cumulative number of successful keep-alive
	// heartbeat probes on idle pooled connections.
	HeartbeatCount int64
	// HeartbeatFailureCount is the cumulative number of heartbeat probes
	// that failed and caused a pooled connection to be evicted.
	HeartbeatFailureCount int64
	// LastHeartbeatRTTMillis is the round-trip time of the most recent
	// successful heartbeat probe, in milliseconds.
	LastHeartbeatRTTMillis int64
}

// IncSyncConnectionFallback records one synchronous fallback dial.
func (m *ClientMetrics) IncSyncConnectionFallback() {
	if m == nil {
		return
	}
	m.syncConnFallbacks.Add(1)
}

// IncConnectionReuse records one kept-alive connection recycled into the pool.
func (m *ClientMetrics) IncConnectionReuse() {
	if m == nil {
		return
	}
	m.connReuses.Add(1)
}

// ObserveHeartbeat records one successful keep-alive heartbeat and its
// observed round-trip time.
func (m *ClientMetrics) ObserveHeartbeat(rttMillis int64) {
	if m == nil {
		return
	}
	m.heartbeats.Add(1)
	m.lastHeartbeatRTTMS.Store(rttMillis)
}

// IncHeartbeatFailure records one failed keep-alive heartbeat.
func (m *ClientMetrics) IncHeartbeatFailure() {
	if m == nil {
		return
	}
	m.heartbeatFailures.Add(1)
}

// Snapshot returns a copy of the current counter values.
func (m *ClientMetrics) Snapshot() ClientMetricsSnapshot {
	if m == nil {
		return ClientMetricsSnapshot{}
	}
	return ClientMetricsSnapshot{
		SyncConnectionCount:    m.syncConnFallbacks.Load(),
		ConnectionReuseCount:   m.connReuses.Load(),
		HeartbeatCount:         m.heartbeats.Load(),
		HeartbeatFailureCount:  m.heartbeatFailures.Load(),
		LastHeartbeatRTTMillis: m.lastHeartbeatRTTMS.Load(),
	}
}
