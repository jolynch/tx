package store

import (
	"fmt"
	"strings"

	intencoding "github.com/jolynch/tx/internal/filexfer/encoding"
	"github.com/jolynch/tx/internal/filexfer/policy"
)

// ChainRequest asks for the adaptive compression chain of one SEND window's
// file. Create also makes a missing chain, for a file that is in range and
// below Done, which excludes Done and Missing files. FileCompressionChains
// sets Chain, or leaves it nil when the file has none.
type ChainRequest struct {
	FileID uint64
	Create bool
	Chain  *policy.FileChain
}

// chainSlabSize is how many chains a transfer allocates at once. Chains are
// never reused, since a window may still hold one its file dropped; the slab
// only spreads one allocation over that many files.
const chainSlabSize = 16

// FileCompressionChains resolves a SEND request's chains in place, under one
// acquisition of the transfer's locks: compMu, and a shared hold of mu only
// when a request may create one. It creates before it looks up, so a window
// that only takes its file's chain gets one that a window anywhere in the
// same request creates. Windows then share their chains without the store's
// locks. An unknown transfer leaves every Chain nil.
func (s *Store) FileCompressionChains(txferID string, reqs []ChainRequest) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return
	}
	create := false
	for i := range reqs {
		create = create || reqs[i].Create
	}
	if create {
		managed.mu.RLock()
		defer managed.mu.RUnlock()
	}
	managed.compMu.Lock()
	defer managed.compMu.Unlock()
	for _, create := range [2]bool{true, false} {
		for i := range reqs {
			if r := &reqs[i]; r.Create == create {
				r.Chain = managed.chainLocked(r.FileID, create)
			}
		}
	}
}

// chainLocked returns a file's chain, creating it when create is set and the
// file may have one. Callers hold compMu, and mu when create is set.
func (m *managedTransfer) chainLocked(fileID uint64, create bool) *policy.FileChain {
	c := m.compression[fileID]
	if c != nil || !create || m.deleted ||
		fileID >= uint64(len(m.transfer.State)) || m.transfer.State[fileID] >= TransferStateDone {
		return c
	}
	if m.compression == nil {
		m.compression = make(map[uint64]*policy.FileChain)
	}
	if len(m.chainSlab) == 0 {
		m.chainSlab = make([]policy.FileChain, chainSlabSize)
	}
	c = &m.chainSlab[0]
	m.chainSlab = m.chainSlab[1:]
	m.compression[fileID] = c
	return c
}

// saveRatesLocked adds a request's delivery rates to the transfer's
// summary. Callers hold mu, shared or exclusive; compMu nests inside it and
// covers no I/O.
func (m *managedTransfer) saveRatesLocked(rates RateSummary, rated []uint64) {
	if rates.Windows == 0 {
		return
	}
	m.compMu.Lock()
	m.rates.merge(rates)
	for _, id := range rated {
		if m.ratedFiles.Add(id) {
			m.rates.Files++
		}
	}
	m.compMu.Unlock()
}

// dropFileCompression forgets a file's chain. Windows still holding it
// finish on it; a later window starts a new one. Callers hold mu.
func (m *managedTransfer) dropFileCompression(fileID uint64) {
	m.compMu.Lock()
	delete(m.compression, fileID)
	m.compMu.Unlock()
}

// dropAllCompression forgets every file's chain. Callers hold mu.
func (m *managedTransfer) dropAllCompression() {
	m.compMu.Lock()
	m.compression, m.chainSlab = nil, nil
	m.compMu.Unlock()
}

// WindowRate is what one adaptive window showed about its connection's
// delivery rate at its last decided frame: the kernel's reading and the rate
// the server's own writes showed, each 0 when it had none. FirstWindow marks
// the window at offset 0, whose file the transfer then counts once however
// often that window is sent; Sampled marks a window that used a background
// codec sample. Classes counts its decided frames by the bottleneck class
// they reported, indexed by policy.Bottleneck.
type WindowRate struct {
	FileID               uint64
	FirstWindow, Sampled bool
	KernelBps, UserBps   uint64
	Classes              [4]uint32
}

// ClassCounts counts adaptive frames by the bottleneck class their
// connection reported.
type ClassCounts struct {
	Unknown  int64 `json:"unknown,omitempty"`
	Sender   int64 `json:"sender,omitempty"`
	Network  int64 `json:"network,omitempty"`
	Receiver int64 `json:"receiver,omitempty"`
}

func (c *ClassCounts) add(frames [4]uint32) {
	c.Unknown += int64(frames[policy.BottleneckUnknown])
	c.Sender += int64(frames[policy.BottleneckSender])
	c.Network += int64(frames[policy.BottleneckNetwork])
	c.Receiver += int64(frames[policy.BottleneckReceiver])
}

func (c *ClassCounts) merge(o ClassCounts) {
	c.Unknown += o.Unknown
	c.Sender += o.Sender
	c.Network += o.Network
	c.Receiver += o.Receiver
}

// since returns the frames counted after o, an earlier snapshot of c.
func (c ClassCounts) since(o ClassCounts) ClassCounts {
	return ClassCounts{c.Unknown - o.Unknown, c.Sender - o.Sender, c.Network - o.Network, c.Receiver - o.Receiver}
}

// Total is the frames counted.
func (c ClassCounts) Total() int64 { return c.Unknown + c.Sender + c.Network + c.Receiver }

// formatClasses renders c as "[net 92% rcv  6% snd  2%]": each known
// class's share of all the frames in three characters ("99+" for all,
// "<1%" for a trace), largest first, nonzero only. Frames of no known class
// count toward the total but not in the bracket, so its shares can sum to
// less than 100%. It returns "" when no frame has a known class.
func formatClasses(c ClassCounts) string {
	total := c.Total()
	if total <= c.Unknown {
		return ""
	}
	type share struct {
		label string
		n     int64
	}
	shares := [3]share{{"net", c.Network}, {"rcv", c.Receiver}, {"snd", c.Sender}}
	// Insertion sort, stable, so ties keep the order above.
	for i := 1; i < len(shares); i++ {
		for j := i; j > 0 && shares[j].n > shares[j-1].n; j-- {
			shares[j], shares[j-1] = shares[j-1], shares[j]
		}
	}
	var b strings.Builder
	b.WriteByte('[')
	for _, sh := range shares {
		if sh.n == 0 {
			continue
		}
		if b.Len() > 1 {
			b.WriteByte(' ')
		}
		switch pct := (sh.n*100 + total/2) / total; {
		case pct >= 100:
			fmt.Fprintf(&b, "%s 99+", sh.label)
		case pct == 0:
			fmt.Fprintf(&b, "%s <1%%", sh.label)
		default:
			fmt.Fprintf(&b, "%s %2d%%", sh.label, pct)
		}
	}
	b.WriteByte(']')
	return b.String()
}

// RateStats summarizes one estimate's nonzero readings, in bytes per second.
type RateStats struct {
	N   int64   `json:"n"`
	Min uint64  `json:"min_bps,omitempty"`
	Max uint64  `json:"max_bps,omitempty"`
	Sum float64 `json:"sum_bps,omitempty"`
}

func (s *RateStats) add(v uint64) {
	if v == 0 {
		return
	}
	if s.N == 0 || v < s.Min {
		s.Min = v
	}
	s.Max = max(s.Max, v)
	s.Sum += float64(v)
	s.N++
}

func (s *RateStats) merge(o RateStats) {
	if o.N == 0 {
		return
	}
	if s.N == 0 || o.Min < s.Min {
		s.Min = o.Min
	}
	s.Max = max(s.Max, o.Max)
	s.Sum += o.Sum
	s.N += o.N
}

// Mean is the readings' mean, or 0 without one.
func (s RateStats) Mean() float64 {
	if s.N == 0 {
		return 0
	}
	return s.Sum / float64(s.N)
}

// RateSummary totals the WindowRates of a transfer's adaptive windows: how
// many files and windows reached the compression policy, how many windows
// used a background sample, what the kernel and the server's own writes said
// the delivery rate was, and their ratio where a window had both.
type RateSummary struct {
	Files    int64     `json:"files"`
	Windows  int64     `json:"windows"`
	Sampled  int64     `json:"sampled"`
	Kernel   RateStats `json:"kernel"`
	User     RateStats `json:"userspace"`
	RatioSum float64   `json:"userspace_over_kernel_sum,omitempty"`
	RatioN   int64     `json:"userspace_over_kernel_n,omitempty"`
	// Bottlenecks counts the windows' decided frames by bottleneck class.
	Bottlenecks ClassCounts `json:"bottleneck_frames,omitzero"`
}

// Add counts one window. Files is the store's: it counts distinct files
// across requests (WindowRecords.RatedFiles), so a resent first window does
// not count its file again.
func (r *RateSummary) Add(w WindowRate) {
	r.Windows++
	if w.Sampled {
		r.Sampled++
	}
	r.Kernel.add(w.KernelBps)
	r.User.add(w.UserBps)
	if w.KernelBps > 0 && w.UserBps > 0 {
		r.RatioSum += float64(w.UserBps) / float64(w.KernelBps)
		r.RatioN++
	}
	r.Bottlenecks.add(w.Classes)
}

func (r *RateSummary) merge(o RateSummary) {
	r.Files += o.Files
	r.Windows += o.Windows
	r.Sampled += o.Sampled
	r.Kernel.merge(o.Kernel)
	r.User.merge(o.User)
	r.RatioSum += o.RatioSum
	r.RatioN += o.RatioN
	r.Bottlenecks.merge(o.Bottlenecks)
}

// formatRates is the line a completed transfer logs after txfer-complete
// when any of its windows reached the compression policy.
func formatRates(id string, numFiles int, r RateSummary) string {
	stats := func(s RateStats) string {
		if s.N == 0 {
			return "[n=0]"
		}
		return fmt.Sprintf("[n=%d min=%s mean=%s max=%s]", s.N,
			intencoding.HumanRate(float64(s.Min)), intencoding.HumanRate(s.Mean()), intencoding.HumanRate(float64(s.Max)))
	}
	ratio := "n/a"
	if r.RatioN > 0 {
		ratio = fmt.Sprintf("%.2fx", r.RatioSum/float64(r.RatioN))
	}
	line := fmt.Sprintf("txfer-rate: tid=%s files=%d/%d windows=%d sampled=%d kernel=%s userspace=%s userspace/kernel=%s",
		id, r.Files, numFiles, r.Windows, r.Sampled, stats(r.Kernel), stats(r.User), ratio)
	if classes := formatClasses(r.Bottlenecks); classes != "" {
		line += fmt.Sprintf(" bottleneck=%s frames=%d", classes, r.Bottlenecks.Total())
	}
	return line
}

// TransferRates returns the transfer's rate summary so far.
func (s *Store) TransferRates(txferID string) (RateSummary, bool) {
	managed, ok := s.getManagedTransfer(txferID)
	if !ok {
		return RateSummary{}, false
	}
	managed.compMu.Lock()
	defer managed.compMu.Unlock()
	return managed.rates, true
}
