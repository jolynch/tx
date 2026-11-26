package ftcp

import (
	"sync"
	"time"

	"github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/policy"
)

// windowChain reports whether an adaptive window wants its file's
// compression chain, and whether it creates a missing one: it wants the chain
// when another window precedes or follows it within the manifest size, and
// creates it only when one follows. A whole-file or tail window (size 0 or
// reaching the manifest end) has no successor, so a single-window file never
// touches the store. A file that shrank since its manifest may get a chain
// no later window uses; it stays until the transfer is reaped, one entry per
// file.
func windowChain(item sendItem, ref FileRef) (want, create bool) {
	switch {
	case item.Comp != "adapt":
		return false, false
	case item.Size > 0 && ref.FileSize >= 0 && item.Offset+item.Size < ref.FileSize:
		return true, true
	}
	return item.Offset > 0, false
}

// sendChains holds the chains a SEND request resolved, in a slice reused
// across requests. reqs lists the windows that want a chain, in record
// order, and records holds each one's record index.
type sendChains struct {
	reqs    []ChainRequest
	records []int
	next    int
}

var sendChainsPool = sync.Pool{New: func() any { return new(sendChains) }}

// requestChains resolves the chains of a request's adaptive windows in one
// store call, or returns nil when no window wants one. The caller releases
// the result after its last window.
func requestChains(deps Deps, txferID, mode string, records []sendRequestRecord, refs []FileRef, refErrs []error) *sendChains {
	var c *sendChains
	for i, record := range records {
		if refErrs[i] != nil {
			continue
		}
		want, create := windowChain(record.item(mode), refs[i])
		if !want {
			continue
		}
		if c == nil {
			c = sendChainsPool.Get().(*sendChains)
		}
		c.reqs = append(c.reqs, ChainRequest{FileID: record.FileID, Create: create})
		c.records = append(c.records, i)
	}
	if c != nil {
		deps.FileCompressionChains(txferID, c.reqs)
	}
	return c
}

// chain returns record i's chain, or nil. Calls must not go back to an
// earlier record. A nil receiver has no chains.
func (c *sendChains) chain(i int) *policy.FileChain {
	if c == nil {
		return nil
	}
	for c.next < len(c.records) && c.records[c.next] < i {
		c.next++
	}
	if c.next < len(c.records) && c.records[c.next] == i {
		return c.reqs[c.next].Chain
	}
	return nil
}

// release forgets the chains, so the pool keeps none alive, and returns the
// slices to the pool. A nil receiver does nothing.
func (c *sendChains) release() {
	if c == nil {
		return
	}
	clear(c.reqs)
	c.reqs, c.records, c.next = c.reqs[:0], c.records[:0], 0
	sendChainsPool.Put(c)
}

// chainedFrame is one frame a window offers its file's chain: the codec it
// was sent in, whether it was measured for recovery (the window's view of the
// policy had a check or its validation running when the frame started), and
// what it measured.
type chainedFrame struct {
	Mode       policy.CompressionMode
	Recovering bool
	Metrics    policy.CompressionMetrics
	Sample     *encoding.CodecSamples
}

// decideChained decides a frame on its file's shared chain, then copies the
// chain's state into local. It decides nothing and reports false when another
// window holds the chain; when the chain's codec is no longer the frame's,
// because a sibling's decision moved it while the frame was in flight; and
// when the chain runs a recovery check or its validation but the frame was
// not measured for one, since a check judges only frames flushed and timed
// on their own. local then holds the chain's state, when it could be read,
// so the window's next frame uses the chain's codec and measurement. The
// hold covers no I/O and no allocation, and nothing waits for it.
func decideChained(chain *policy.FileChain, local *policy.FileCompression, rate *sendRate, f chainedFrame) (policy.CompressionDecision, bool) {
	st := chain.TryHold()
	if st == nil {
		return policy.CompressionDecision{}, false
	}
	var d policy.CompressionDecision
	fed := st.Mode == f.Mode && (f.Recovering || !st.Policy.RecoveryActive())
	if fed {
		rate.seedLink(&st.Policy)
		d = st.Policy.DecideSampled(f.Mode, f.Metrics, f.Sample)
		st.Mode = d.Next
	}
	*local = *st
	chain.Release()
	return d, fed
}

// settleCheckFrame ends a decided frame's claim and returns the recovery
// loss to charge. A claimed frame the check did not observe, because the
// check ended or the chain was held, is charged its time over the check's
// baseline and goes back to the chain, so a check missing frames can still
// finish. view is the window's policy after the decision.
func settleCheckFrame(chain *policy.FileChain, claim *policy.CheckClaim, fed bool, view *policy.CompressionPolicy, loss time.Duration, m policy.CompressionMetrics) time.Duration {
	if claim.Held() && !fed {
		loss = view.CheckFrameLoss(m)
		chain.ReturnCheckFrame(*claim)
	}
	*claim = policy.CheckClaim{}
	return loss
}

func initialCompressionMode(comp string) policy.CompressionMode {
	switch comp {
	case encoding.EncodingLz4:
		return policy.CompressionModeLz4
	case encoding.EncodingZstd:
		return policy.CompressionModeZstdLevel1
	default:
		return policy.CompressionModeNone
	}
}
