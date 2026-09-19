//go:build !race

package bufpool

// See the encoding package for why allocation measurements are skipped under
// the race detector.
const raceEnabled = false
