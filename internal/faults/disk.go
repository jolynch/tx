package faults

import (
	"context"
	"io"
	"io/fs"
	"syscall"
)

// Site names where a file is opened, read, or written.
type Site uint8

const (
	SiteSendOpen Site = iota + 1
	SiteSendRead
	SiteCXSUMOpen
	SiteCXSUMRead
	SiteVerifyRead
	SiteOutputWrite
	siteCount
)

// diskKey identifies one attempt at one byte offset of one file at a site.
// Disk faults key on it, so they reproduce from the seed whatever the
// goroutines do: concurrent windows of a file start at different offsets,
// and an offset is retried only after its earlier attempt ended. A retry
// still gets a fresh decision.
func (i *Injector) diskKey(site Site, file uint64, offset int64) uint64 {
	base := mix(uint64(site), file, uint64(offset))
	return mix(base, i.attempt(base))
}

// OpenErr returns the error to report instead of opening file at site for
// the window that starts at offset, or nil to open it. file is the caller's
// stable ID for the file, such as its manifest ID.
func (i *Injector) OpenErr(site Site, file uint64, offset int64) error {
	if i == nil {
		return nil
	}
	i.counts.opens[site].Add(1)
	if !i.famOn[FamilyDisk] {
		return nil
	}
	key := i.diskKey(site, file, offset)
	switch {
	case i.firesAt(DiskOpenENOENT, key, 0, file, offset):
		return &fs.PathError{Op: "open", Path: "(fault)", Err: syscall.ENOENT}
	case i.firesAt(DiskOpenEIO, key, 0, file, offset):
		return &fs.PathError{Op: "open", Path: "(fault)", Err: syscall.EIO}
	}
	return nil
}

// ZeroCopyReads reports whether a SEND may splice a file's frames past its
// reader. With read.flip on it may not, since a spliced payload never passes
// through the reader a flip corrupts. A nil injector returns true.
func (i *Injector) ZeroCopyReads() bool {
	return i == nil || i.rates[DiskReadFlip] == 0
}

// WrapReaderAt wraps file's reader at site. A nil injector returns r.
func (i *Injector) WrapReaderAt(ctx context.Context, site Site, file uint64, r io.ReaderAt) io.ReaderAt {
	if i == nil || r == nil {
		return r
	}
	i.counts.reads[site].Add(1)
	return &readerAt{r: r, inj: i, ctx: ctx, site: site, file: file}
}

type readerAt struct {
	r    io.ReaderAt
	inj  *Injector
	ctx  context.Context
	site Site
	file uint64
}

func (r *readerAt) ReadAt(p []byte, off int64) (int, error) {
	i := r.inj
	if !i.famOn[FamilyDisk] {
		return r.r.ReadAt(p, off)
	}
	key := i.diskKey(r.site, r.file, off)
	if i.firesAt(DiskReadLatency, key, 0, r.file, off) {
		if err := sleep(r.ctx, i.sample(DiskReadLatency, key, 0), noDeadline, nil); err != nil {
			return 0, err
		}
	}
	if i.firesAt(DiskReadEIO, key, 0, r.file, off) {
		return 0, &fs.PathError{Op: "read", Path: "(fault)", Err: syscall.EIO}
	}
	n, err := r.r.ReadAt(p, off)
	if n > 0 && i.firesAt(DiskReadFlip, key, 0, r.file, off) {
		if r.site == SiteSendRead {
			i.recordHit(r.file)
		}
		rng := i.rng(DiskReadFlip, key, 0)
		p[rng.IntN(n)] ^= 1 << rng.IntN(8)
	}
	return n, err
}

// writeBlock is the grid write faults are decided on. Deciding per block of
// the file, rather than per Write call, keeps the decisions the same however
// the writes are cut, which in plaintext follows the network's reads.
const writeBlock = 64 << 10

// WrapWriter wraps file's writer at site, for a window that starts at
// offset. A nil injector returns w.
func (i *Injector) WrapWriter(site Site, file uint64, offset int64, w io.WriteCloser) io.WriteCloser {
	if i == nil || w == nil {
		return w
	}
	i.counts.writes[site].Add(1)
	if !i.famOn[FamilyDisk] {
		return &writer{w: w, inj: i, pos: offset}
	}
	first := (offset + writeBlock - 1) / writeBlock * writeBlock
	_, seeks := w.(io.Seeker)
	return &writer{w: w, inj: i, key: i.diskKey(site, file, offset), file: file, site: site, pos: offset, next: first, failAt: -1, seeks: seeks}
}

// writer decides each write fault once per block of the file it writes, at
// the block's start, and gives the fault an absolute position: a failure
// point, a flipped byte, or a lost block. A Write applies whatever falls
// inside the bytes it covers.
type writer struct {
	w    io.WriteCloser
	inj  *Injector
	key  uint64
	file uint64
	site Site
	pos  int64 // where the next byte lands
	next int64 // the next block start to decide
	// seeks reports whether w can skip a lost block, leaving the file's
	// earlier bytes there. A writer that cannot gets no lost blocks, since
	// dropping the bytes would shift every later one.
	seeks bool

	failAt   int64 // where a write fails, or -1
	failKind Kind
	flips    []int64 // byte positions to flip
	lost     [][2]int64
}

// decide makes the decisions for every block that starts before end. One
// failure is pending at a time: a block whose failure is decided while
// another waits gets none. Callers stop at the first failure, so they see
// the same outcome however their writes are cut.
func (w *writer) decide(end int64) {
	i := w.inj
	for ; w.next < end; w.next += writeBlock {
		b := w.next
		op := uint64(b / writeBlock)
		at := func(k Kind) bool { return i.firesAt(k, w.key, op, w.file, b) }
		inside := func(k Kind) int64 { return b + 1 + i.rng(k, w.key, op).Int64N(writeBlock-1) }
		if at(DiskWriteLatency) {
			_ = sleep(nil, i.sample(DiskWriteLatency, w.key, op), noDeadline, nil)
		}
		if w.failAt < 0 {
			switch {
			case at(DiskWriteEIO):
				w.failAt, w.failKind = b, DiskWriteEIO
			case at(DiskWriteENOSPC):
				w.failAt, w.failKind = b, DiskWriteENOSPC
			case at(DiskWriteTorn):
				w.failAt, w.failKind = inside(DiskWriteTorn), DiskWriteTorn
			case at(DiskWriteShort):
				w.failAt, w.failKind = inside(DiskWriteShort), DiskWriteShort
			}
		}
		if w.seeks && at(DiskWriteLost) {
			w.lost = append(w.lost, [2]int64{b, b + writeBlock})
			w.recordHit()
		}
		if at(DiskWriteFlip) {
			w.recordHit()
			w.flips = append(w.flips, b+i.rng(DiskWriteFlip, w.key, op).Int64N(writeBlock))
		}
	}
}

// recordHit notes that an undetected fault fired on w's file, if w writes
// the downloaded copy.
func (w *writer) recordHit() {
	if w.site == SiteOutputWrite {
		w.inj.recordHit(w.file)
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if !w.inj.famOn[FamilyDisk] || len(p) == 0 {
		n, err := w.w.Write(p)
		w.pos += int64(n)
		return n, err
	}
	start := w.pos
	end := start + int64(len(p))
	w.decide(end)
	for w.pos < end {
		if w.failAt >= 0 && w.pos >= w.failAt {
			n := int(w.pos - start)
			kind := w.failKind
			w.failAt = -1
			switch kind {
			case DiskWriteShort:
				// A short write with no error breaks io.Writer's contract, as
				// a buggy filesystem or driver might; callers must notice.
				return n, nil
			case DiskWriteENOSPC:
				return n, &fs.PathError{Op: "write", Path: "(fault)", Err: syscall.ENOSPC}
			default:
				return n, &fs.PathError{Op: "write", Path: "(fault)", Err: syscall.EIO}
			}
		}
		segEnd := end
		if w.failAt >= 0 && w.failAt < segEnd {
			segEnd = w.failAt
		}
		if from, to, ok := w.lostRange(); ok {
			if w.pos >= from {
				// Lost bytes are reported written but never reach the file,
				// which keeps its earlier contents there.
				skip := min(to, segEnd) - w.pos
				if _, err := w.w.(io.Seeker).Seek(skip, io.SeekCurrent); err != nil {
					return int(w.pos - start), err
				}
				w.pos += skip
				continue
			}
			segEnd = min(segEnd, from)
		}
		seg := p[w.pos-start : segEnd-start]
		if flipped := w.flipIn(w.pos, segEnd, seg); flipped != nil {
			seg = flipped
		}
		n, err := w.w.Write(seg)
		w.pos += int64(n)
		if err != nil {
			return int(w.pos - start), err
		}
		if n < len(seg) {
			return int(w.pos - start), io.ErrShortWrite
		}
	}
	return len(p), nil
}

// lostRange returns the first lost range that ends after the current
// position, dropping those already passed.
func (w *writer) lostRange() (from, to int64, ok bool) {
	for len(w.lost) > 0 && w.lost[0][1] <= w.pos {
		w.lost = w.lost[1:]
	}
	if len(w.lost) == 0 {
		return 0, 0, false
	}
	return w.lost[0][0], w.lost[0][1], true
}

// flipIn returns a copy of seg, the bytes for [from, to), with every pending
// flip in that range applied, or nil when none falls there.
func (w *writer) flipIn(from, to int64, seg []byte) []byte {
	var out []byte
	kept := w.flips[:0]
	for _, at := range w.flips {
		if at < from || at >= to {
			if at >= to {
				kept = append(kept, at)
			}
			continue
		}
		if out == nil {
			out = append([]byte(nil), seg...)
		}
		out[at-from] ^= 1 << (uint64(at) % 8)
	}
	w.flips = kept
	return out
}

func (w *writer) Close() error { return w.w.Close() }
