//go:build linux && !race

package memxfer

// raceEnabled reports whether the race detector is compiled in. It slows
// the codecs several times over, so a property that depends on compression
// outrunning a link checks only that the copy completes under it.
const raceEnabled = false
