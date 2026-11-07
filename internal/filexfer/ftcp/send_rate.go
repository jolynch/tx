package ftcp

import (
	"context"
	"io"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/jolynch/tx/internal/faults"
	"github.com/jolynch/tx/internal/filexfer/policy"
	"github.com/jolynch/tx/internal/sysio"
)

// A frame write that lands in socket buffers returns long before its bytes
// reach the receiver, so its measured time understates what the path costs.
// An adaptive SEND hands the compression policy, for each frame, the larger
// of the measured write and the frame's wire bytes over the connection's
// delivery rate. A SEND with no adaptive frame reads nothing.
//
// The rate comes from the kernel when it can: Linux reports a TCP socket's
// delivery rate in TCP_INFO. A connection in slow start reports a rate far
// below its path, and charging every frame at that rate would make a fast
// link look slow enough to compress. So a SEND reads again as it writes and
// keeps the highest rate it has seen, as BBR estimates bandwidth. A rate
// that is too high costs nothing: a frame that waits on the path measures
// the wait itself.
//
// Without a kernel rate, as on a connection that is not a TCP socket, the
// SEND uses the rate the connection's own writes showed: their wire bytes
// over their measured time, decayed by frame. Writes that land in buffers
// make it high, which costs nothing; writes that wait on the path make it
// the path's rate.

// sendRateFirstGap is how many wire bytes of adaptive frames a SEND writes
// after its first read before it reads again. The gap doubles after each
// read up to sendRateMaxGap, so reads follow slow start closely and then
// settle at once per sendSniffEvery frames, never once per frame.
const (
	sendRateFirstGap = defaultFileFrameLogicalSize
	sendRateMaxGap   = sendSniffEvery * defaultFileFrameLogicalSize
)

// sendRateKey carries a connection's *sendRateSource in the context of its
// commands.
type sendRateKey struct{}

// sendRateSource is a connection's rate state: its TCP socket, for the
// kernel's delivery rate, and the rate its own writes showed. It builds its
// reader on the first read and keeps it, so later reads allocate nothing. A
// connection serves one command at a time, so its SENDs never share it
// concurrently.
type sendRateSource struct {
	conn   *net.TCPConn     // nil when the connection is not a TCP socket
	inj    *faults.Injector // gates the reads; nil injects nothing
	reader *sysio.TCPRateReader
	// wire and secs are decayed sums of adaptive frames' wire bytes and
	// measured write seconds, each scaled by 7/8 per frame.
	wire, secs float64
	// link is the latest uncompressed baseline a file on this connection
	// measured, which seeds the connection's next fresh file.
	link    policy.LinkPrior
	hasLink bool
	// wallPerByte averages the seconds per logical byte that adaptive
	// frames really took, prepare and write, with no rate floor. Sample
	// admission asks whether a job finishes before its window ends, which
	// is wall time, not the cost the policy is charged.
	wallPerByte float64
	// budget is the server's recovery budget, which all its connections
	// share; nil for a SEND outside a server, which then uses own.
	budget *policy.RecoveryBudget
	own    policy.RecoveryBudget
}

// withSendRateSource returns ctx carrying src for conn, looking through a
// fault-injection wrapper for its TCP socket. Its reads use the injector ctx
// carries.
func withSendRateSource(ctx context.Context, src *sendRateSource, conn net.Conn) context.Context {
	src.conn, src.inj = rawTCPConn(conn), faults.FromContext(ctx)
	return context.WithValue(ctx, sendRateKey{}, src)
}

// sendRateSourceFor returns the source a SEND's context carries or, for a
// SEND outside a connection's session, a new one for this SEND alone.
func sendRateSourceFor(ctx context.Context, out io.Writer) *sendRateSource {
	if src, ok := ctx.Value(sendRateKey{}).(*sendRateSource); ok {
		return src
	}
	src := &sendRateSource{inj: faults.FromContext(ctx)}
	if c, ok := out.(net.Conn); ok {
		src.conn = rawTCPConn(c)
	}
	return src
}

// readDeliveryRate reads one TCP_INFO. Tests replace it to count reads or
// script their results.
var readDeliveryRate = (*sysio.TCPRateReader).DeliveryRate

// kernel reports whether the connection can have a kernel delivery rate.
func (s *sendRateSource) kernel() bool { return s.conn != nil || s.reader != nil }

func (s *sendRateSource) deliveryRate() (bytesPerSec uint64, appLimited bool) {
	if s.reader == nil {
		s.reader = sysio.NewTCPRateReader(s.inj, s.conn)
	}
	return readDeliveryRate(s.reader)
}

// observe adds an adaptive frame's wire bytes and measured write time to the
// rate its writes show.
func (s *sendRateSource) observe(wire int64, measured time.Duration) {
	s.wire = s.wire*7/8 + float64(wire)
	s.secs = s.secs*7/8 + measured.Seconds()
}

// userRate is the rate the connection's writes showed, in bytes per second,
// or 0 before a write took measurable time.
func (s *sendRateSource) userRate() uint64 {
	if s.secs <= 0 {
		return 0
	}
	return uint64(s.wire / s.secs)
}

// sendRate is one SEND's write estimate. Its items, buffered or gentle,
// share one.
type sendRate struct {
	src *sendRateSource
	// bytesPerSec is the highest kernel delivery rate a read found, or 0 for
	// none yet. It covers one SEND, so a later SEND sees a path that slowed.
	bytesPerSec uint64
	// sinceRead counts adaptive wire bytes since the last read, and gap is
	// how many there must be before the next; 0 before the first read.
	sinceRead, gap int64
}

// writeTime returns the write time the policy sees for an adaptive frame of
// wire bytes whose writes took measured: measured, or wire over the rate
// when that is longer. The rate is the kernel's when the SEND has one, and
// otherwise the one earlier writes showed. An application-limited kernel
// rate is not used: it measures how fast the sender produced bytes, not how
// fast the path takes them, and charging a compressed frame at that pace
// would favor compression because compression was slow.
func (r *sendRate) writeTime(wire int64, measured time.Duration) time.Duration {
	if r.src == nil {
		return measured
	}
	if r.src.kernel() {
		r.sinceRead += wire
		if r.gap == 0 || r.sinceRead >= r.gap {
			r.gap = min(max(sendRateFirstGap, 2*r.gap), sendRateMaxGap)
			r.sinceRead = 0
			if rate, appLimited := r.src.deliveryRate(); !appLimited {
				r.bytesPerSec = max(r.bytesPerSec, rate)
			}
		}
	}
	rate := r.bytesPerSec
	if rate == 0 {
		rate = r.src.userRate()
	}
	if wire > 0 {
		r.src.observe(wire, measured)
	}
	if rate == 0 || wire <= 0 {
		return measured
	}
	return max(measured, time.Duration(float64(wire)*float64(time.Second)/float64(rate)))
}

// readings returns the SEND's kernel delivery rate and the rate the
// connection's writes showed, each 0 when it has none.
func (r *sendRate) readings() (kernel, user uint64) {
	if r.src == nil {
		return r.bytesPerSec, 0
	}
	return r.bytesPerSec, r.src.userRate()
}

// sendFrameWall averages, across the process, the seconds per logical byte
// adaptive frames really took, as float64 bits. It stands in for a
// connection that has sent no frame yet: without keep-alive, every SEND
// arrives on a new connection. Concurrent updates may drop one another,
// which only slows the average.
var sendFrameWall atomic.Uint64

// observeFrame adds a decided frame's measured prepare and write time to
// the connection's average and the process's.
func (r *sendRate) observeFrame(logical int64, wall time.Duration) {
	if logical <= 0 || wall <= 0 {
		return
	}
	perByte := wall.Seconds() / float64(logical)
	average := func(avg float64) float64 {
		if avg == 0 {
			return perByte
		}
		return avg + (perByte-avg)/4
	}
	sendFrameWall.Store(math.Float64bits(average(math.Float64frombits(sendFrameWall.Load()))))
	if r != nil && r.src != nil {
		r.src.wallPerByte = average(r.src.wallPerByte)
	}
}

// wallFrame returns metrics for a full frame at the connection's measured
// wall time, or the process's, or zero metrics before either has one.
func (r *sendRate) wallFrame() policy.CompressionMetrics {
	perByte := math.Float64frombits(sendFrameWall.Load())
	if r != nil && r.src != nil && r.src.wallPerByte > 0 {
		perByte = r.src.wallPerByte
	}
	if perByte <= 0 {
		return policy.CompressionMetrics{}
	}
	return policy.CompressionMetrics{LogicalSize: defaultFileFrameLogicalSize,
		PrepareLatency: time.Duration(perByte * float64(defaultFileFrameLogicalSize) * float64(time.Second))}
}

// seedLink starts p from the connection's link estimate when p is fresh and
// the connection has one.
func (r *sendRate) seedLink(p *policy.CompressionPolicy) {
	if r != nil && r.src != nil && r.src.hasLink {
		p.SeedLink(r.src.link)
	}
}

// saveLink keeps p's link estimate, when it has one, for the connection's
// next fresh file.
func (r *sendRate) saveLink(p *policy.CompressionPolicy) {
	if r == nil || r.src == nil {
		return
	}
	if l, ok := p.Link(); ok {
		r.src.link, r.src.hasLink = l, true
	}
}

func (s *sendRateSource) recovery() *policy.RecoveryBudget {
	if s.budget != nil {
		return s.budget
	}
	return &s.own
}

func (r *sendRate) recoveryReady() bool {
	return r == nil || r.src == nil || r.src.recovery().Ready()
}

// Each lost second requires 100 seconds of ordinary transfer time: a 1%
// steady-state budget. Detected changes may check earlier, without a refund.
func (r *sendRate) noteRecovery(loss, elapsed time.Duration) {
	if r == nil || r.src == nil {
		return
	}
	r.src.recovery().Observe(loss, elapsed)
}
