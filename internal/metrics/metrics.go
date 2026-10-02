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
	dials              atomic.Int64
	ackRetries         atomic.Int64
	requestErrors      atomic.Int64
	logicalBytes       atomic.Int64
	wireBytes          atomic.Int64
	windows            [numCodecs]atomic.Int64
}

// Codecs counted per window. Anything else counts as "other".
var codecNames = [...]string{"none", "lz4", "zstd", "other"}

const numCodecs = len(codecNames)

func codecIndex(codec string) int {
	for i, name := range codecNames[:numCodecs-1] {
		if name == codec {
			return i
		}
	}
	return numCodecs - 1
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
	// DialCount is the cumulative number of TCP dials, pooled or not.
	DialCount int64
	// AckRetryCount is the cumulative number of ACK attempts beyond the
	// first for each acknowledgment.
	AckRetryCount int64
	// RequestErrorCount is the cumulative number of failed dials plus ERR
	// responses from the server.
	RequestErrorCount int64
	// LogicalBytes and WireBytes total the decoded and on-the-wire sizes of
	// every received file window.
	LogicalBytes int64
	WireBytes    int64
	// WindowsByCodec counts received file windows per codec; codecs never
	// seen are absent.
	WindowsByCodec map[string]int64
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

// IncDial records one TCP dial attempt.
func (m *ClientMetrics) IncDial() {
	if m == nil {
		return
	}
	m.dials.Add(1)
}

// IncAckRetry records one ACK attempt beyond the first.
func (m *ClientMetrics) IncAckRetry() {
	if m == nil {
		return
	}
	m.ackRetries.Add(1)
}

// IncRequestError records one failed dial or ERR response.
func (m *ClientMetrics) IncRequestError() {
	if m == nil {
		return
	}
	m.requestErrors.Add(1)
}

// ObserveWindow records one received file window.
func (m *ClientMetrics) ObserveWindow(codec string, logical, wire int64) {
	if m == nil {
		return
	}
	m.windows[codecIndex(codec)].Add(1)
	m.logicalBytes.Add(logical)
	m.wireBytes.Add(wire)
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
		DialCount:              m.dials.Load(),
		AckRetryCount:          m.ackRetries.Load(),
		RequestErrorCount:      m.requestErrors.Load(),
		LogicalBytes:           m.logicalBytes.Load(),
		WireBytes:              m.wireBytes.Load(),
		WindowsByCodec:         m.windowsByCodec(),
	}
}

func (m *ClientMetrics) windowsByCodec() map[string]int64 {
	var out map[string]int64
	for i := range m.windows {
		if n := m.windows[i].Load(); n > 0 {
			if out == nil {
				out = make(map[string]int64, numCodecs)
			}
			out[codecNames[i]] = n
		}
	}
	return out
}
