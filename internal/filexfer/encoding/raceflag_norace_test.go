//go:build !race

package encoding

// raceEnabled reports whether the race detector is compiled in. Allocation
// measurements are meaningless under it: the detector's shadow state is itself
// heap-allocated and scales with the allocations being measured, so a cost
// assertion tuned without it fails with it for reasons unrelated to the code
// under test.
const raceEnabled = false
