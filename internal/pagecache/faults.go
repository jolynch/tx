package pagecache

import (
	"sync/atomic"

	"github.com/jolynch/tx/internal/faults"
)

// injector gates this package's syscalls. They are reached from many entry
// points that carry no request to hold an injector, so it is process-wide.
var injector atomic.Pointer[faults.Injector]

// SetFaults routes the package's page-cache syscalls through inj until the
// returned function clears it. The last caller wins, so only one
// fault-injected server or client should set it at a time. Clearing leaves
// alone an injector set by a later caller, and never brings back an earlier
// one, whose owner may already be done with it.
func SetFaults(inj *faults.Injector) (clear func()) {
	injector.Store(inj)
	return func() { injector.CompareAndSwap(inj, nil) }
}
