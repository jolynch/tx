//go:build race

package idset

// randomIDs is how many random IDs TestSetMemoryBound inserts. Race
// instrumentation slows the sparse form's memmove by about 25x, so it uses
// fewer.
const randomIDs = 100_000
