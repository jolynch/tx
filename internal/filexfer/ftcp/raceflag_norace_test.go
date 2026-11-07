//go:build !race

package ftcp

// raceEnabled reports whether the race detector is compiled in. It drops
// sync.Pool items at random, so allocation assertions on pooled paths skip
// under it.
const raceEnabled = false
