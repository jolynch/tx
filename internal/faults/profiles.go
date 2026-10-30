package faults

// profiles are named sets of family specs, chosen with TX_FAULTS_PROFILE.
// Family variables override a profile kind by kind.
var profiles = map[string]map[Family]string{
	// bad-day turns on most of the catalog at rates low enough that an
	// in-memory transfer still finishes with correct bytes within a few
	// resume rounds (TestBadDayCompletes in internal/memxfer checks this).
	// Per-file kinds stay near 0.1-0.3%: one fault fails a whole SEND batch,
	// so a batch of 40 small files must usually get through. Each duration
	// fits a shape with alpha between about 0.2 and 0.7: most samples near the
	// low end with a real tail, rather than nearly all at the minimum.
	// It leaves out UndetectedKinds, which would let a run without --verify
	// succeed with wrong data, and open.enoent, after which the file is correctly
	// reported missing for good.
	"bad-day": {
		FamilyNet: "delay=0.01:β(10us,300us,2ms,10ms), pause=0.001:β(5ms,30ms,200ms,300ms), " +
			"reset=0.001, partial=0.002, eof=0.0005, flip=0.0005, drop=0.0005, dup=0.0005, reorder=0.0005, " +
			"dial.fail=0.05, dial.slow=0.05:β(100us,5ms,25ms,50ms), accept.fail=0.05",
		FamilyMsg: "lose-response=0.02, dup=0.05, delay=0.05:β(100us,5ms,25ms,50ms), busy=0.05",
		FamilyDisk: "open.eio=0.003, read.eio=0.002, read.latency=0.05:gp3, " +
			"write.eio=0.001, write.enospc=0.001, write.short=0.001, write.torn=0.001, write.latency=0.02:gp3",
		FamilySys: "*.fail=0.002, *.latency=0.01:β(1us,100us,500us,2ms), *.short=0.05",
	},
}
