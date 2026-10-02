package report

import "math"

// EstimateClockOffset aligns the sender's clock to the client's from
// timestamps already on the wire. Each client window event carries the
// sender's FX/1 header timestamp (server_ts_ms) and its arrival time (t - dur),
// so min(arrival - server_ts) is the clock offset plus the
// minimum one-way delay; subtracting half the minimum probe RTT leaves the
// offset, good to within rtt/2 plus the 1ms resolution of server_ts.
//
// The result is client time minus sender time: add it to a sender
// timestamp to place it on the client's clock. Without window events it
// returns zeros.
func EstimateClockOffset(client []TraceRecord) (offsetNS, errNS int64) {
	minDelta := int64(math.MaxInt64)
	minRTT := int64(math.MaxInt64)
	for _, r := range client {
		switch r.Ev {
		case "window":
			// Window events are stamped when the window completes; its
			// header arrived dur earlier.
			if ts := r.Int("server_ts_ms"); ts > 0 {
				minDelta = min(minDelta, r.T-r.Int("dur")-ts*1e6)
			}
		case "probe", "heartbeat":
			if rtt := r.Int("rtt"); rtt > 0 {
				minRTT = min(minRTT, rtt)
			}
		}
	}
	if minDelta == math.MaxInt64 {
		return 0, 0
	}
	if minRTT == math.MaxInt64 {
		minRTT = 0
	}
	return minDelta - minRTT/2, minRTT/2 + 1e6
}
