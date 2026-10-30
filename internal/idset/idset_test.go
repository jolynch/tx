package idset

import (
	"encoding/binary"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
	"testing"
)

// pick draws an ID from a mix of small dense values and arbitrary 64-bit ones.
func pick(sel byte, v uint64) uint64 {
	switch sel % 4 {
	case 0:
		return v % 64
	case 1:
		return v % 4096
	case 2:
		return v
	default:
		return v | 1<<63
	}
}

func check(t *testing.T, s *Set, m map[uint64]bool) {
	t.Helper()
	if s.Len() != len(m) {
		t.Fatalf("Len() = %d, want %d", s.Len(), len(m))
	}
	got := slices.Collect(s.All())
	want := slices.Sorted(maps.Keys(m))
	if !slices.Equal(got, want) {
		t.Fatalf("All() = %v, want %v", got, want)
	}
}

// fuzzOp encodes one FuzzSet operation: an opcode, an ID selector, and an
// 8-byte value. Run ops take the run length from the value's top 16 bits and
// the base ID from the low 48.
func fuzzOp(op, sel byte, v uint64) []byte {
	b := []byte{op, sel, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(b[2:], v)
	return b
}

func runVal(base uint64, k uint64) uint64 { return k<<48 | base }

// FuzzSet drives a sequence of Add, Has, Union, Clone, run-insert, self-union,
// and read-only checks against two sets and compares every result with a
// map model. Run inserts push a set past the sparse buffer and across the
// sparse/dense threshold; unions take either side in either form.
func FuzzSet(f *testing.F) {
	f.Add([]byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 5})
	f.Add([]byte{0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 0, 0, 0, 0, 1})
	// Sparse target, small dense source: the union flips the target's form.
	f.Add(slices.Concat(fuzzOp(6, 2, runVal(1280, 10)), fuzzOp(7, 0, runVal(0, 64)), fuzzOp(3, 0, 0), fuzzOp(5, 0, 0)))
	// Pending sparse IDs with spare capacity, then reads that must not merge them.
	f.Add(slices.Concat(fuzzOp(6, 2, runVal(1<<40, 66)), fuzzOp(6, 2, runVal(1<<41, 3)), fuzzOp(9, 0, 0), fuzzOp(5, 0, 0)))
	f.Add(slices.Concat(fuzzOp(6, 2, runVal(1, 3000)), fuzzOp(8, 0, 0), fuzzOp(10, 0, 0), fuzzOp(5, 0, 0)))
	f.Fuzz(func(t *testing.T, data []byte) {
		var a, b Set
		ma, mb := map[uint64]bool{}, map[uint64]bool{}
		run := func(s *Set, m map[uint64]bool, base, k uint64) {
			for i := range k {
				id := base + i
				if got, want := s.Add(id), !m[id]; got != want {
					t.Fatalf("Add(%d) = %v, want %v", id, got, want)
				}
				m[id] = true
			}
		}
		for len(data) >= 10 {
			op := data[0] % 12
			v := binary.BigEndian.Uint64(data[2:10])
			id := pick(data[1], v)
			base := pick(data[1], v<<16>>16)
			k := v>>48%4000 + 1
			data = data[10:]
			switch op {
			case 0:
				if got, want := a.Add(id), !ma[id]; got != want {
					t.Fatalf("Add(%d) = %v, want %v", id, got, want)
				}
				ma[id] = true
			case 1:
				if got := a.Has(id); got != ma[id] {
					t.Fatalf("Has(%d) = %v, want %v", id, got, ma[id])
				}
			case 2:
				if got, want := b.Add(id), !mb[id]; got != want {
					t.Fatalf("b.Add(%d) = %v, want %v", id, got, want)
				}
				mb[id] = true
			case 3:
				a.Union(&b)
				maps.Copy(ma, mb)
			case 4:
				c := a.Clone()
				mc := maps.Clone(ma)
				check(t, &c, mc)
				// The clone and the original must not share storage.
				c.Add(id)
				mc[id] = true
				check(t, &a, ma)
				a.Add(id ^ 1)
				ma[id^1] = true
				check(t, &c, mc)
			case 5:
				check(t, &a, ma)
				check(t, &b, mb)
			case 6:
				run(&a, ma, base, k)
			case 7:
				run(&b, mb, base, k)
			case 8:
				a.Union(&a)
			case 9:
				// Reading a set, directly or as Union's argument, must not
				// change it.
				before := snap(&a)
				_ = slices.Collect(a.All())
				var x Set
				x.Union(&b)
				x.Union(&a)
				unchanged(t, "reading a", &a, before)
			case 10:
				b.Union(&a)
				maps.Copy(mb, ma)
			case 11:
				if got := b.Has(id); got != mb[id] {
					t.Fatalf("b.Has(%d) = %v, want %v", id, got, mb[id])
				}
			}
		}
		check(t, &a, ma)
		check(t, &b, mb)
		for k := range ma {
			if !a.Has(k) {
				t.Fatalf("Has(%d) = false for a member", k)
			}
		}
	})
}

// TestUnionAcrossFormChange unions a small dense set into a sparse set. The
// merge flips the target to the bitset form part way, and must neither lose
// nor double-count members.
func TestUnionAcrossFormChange(t *testing.T) {
	var s, b Set
	want := map[uint64]bool{}
	for id := uint64(1280); id < 1290; id++ {
		s.Add(id)
		want[id] = true
	}
	for id := uint64(0); id < 64; id++ {
		b.Add(id)
		want[id] = true
	}
	s.Union(&b)
	check(t, &s, want)
}

// snapshot is a deep copy of a set's internal state.
type snapshot struct {
	n                int
	sparse           bool
	words, ids, pend []uint64
	idsCap, pendCap  int
}

func snap(s *Set) snapshot {
	return snapshot{n: s.n, sparse: s.sparse, words: slices.Clone(s.words), ids: slices.Clone(s.ids),
		pend: slices.Clone(s.pend), idsCap: cap(s.ids), pendCap: cap(s.pend)}
}

// unchanged fails t if s's internal state differs from before.
func unchanged(t *testing.T, what string, s *Set, before snapshot) {
	t.Helper()
	after := snap(s)
	if after.n != before.n || after.sparse != before.sparse || after.idsCap != before.idsCap || after.pendCap != before.pendCap ||
		!slices.Equal(after.words, before.words) || !slices.Equal(after.ids, before.ids) || !slices.Equal(after.pend, before.pend) {
		t.Fatalf("%s changed the set it only reads: before %+v, after %+v", what, before, after)
	}
}

// TestReadsLeaveSetUnchanged checks All and the argument of Union never
// change a set, even one holding unmerged sparse IDs with spare slice
// capacity, where an in-place merge would write into shared backing arrays.
func TestReadsLeaveSetUnchanged(t *testing.T) {
	var s Set
	want := map[uint64]bool{}
	for i := range uint64(66) {
		id := 1<<40 + i*1000
		s.Add(id)
		want[id] = true
	}
	for i := range uint64(3) {
		id := 1<<41 + i
		s.Add(id)
		want[id] = true
	}
	if !s.sparse || len(s.pend) == 0 {
		t.Fatalf("setup: want a sparse set with pending IDs, got sparse=%v pend=%d", s.sparse, len(s.pend))
	}
	before := snap(&s)
	_ = slices.Collect(s.All())
	unchanged(t, "All", &s, before)
	var dense, sparse Set
	for id := uint64(0); id < 64; id++ {
		dense.Add(id)
	}
	sparse.Add(1 << 50)
	dense.Union(&s)
	unchanged(t, "dense.Union(&s)", &s, before)
	sparse.Union(&s)
	unchanged(t, "sparse.Union(&s)", &s, before)
	check(t, &s, want)
}

func heapAlloc() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestSetMemoryBound(t *testing.T) {
	grew := func(build func() *Set) (int64, *Set) {
		before := heapAlloc()
		s := build()
		after := heapAlloc()
		return int64(after) - int64(before), s
	}
	t.Run("dense", func(t *testing.T) {
		const n = 10_000_000
		g, s := grew(func() *Set {
			s := new(Set)
			for i := uint64(1); i <= n; i++ {
				s.Add(i)
			}
			return s
		})
		runtime.KeepAlive(s)
		if g > 2<<20 {
			t.Fatalf("%d dense IDs grew the heap by %d bytes, want under 2 MiB", n, g)
		}
		if s.Len() != n {
			t.Fatalf("Len() = %d, want %d", s.Len(), n)
		}
	})
	t.Run("random", func(t *testing.T) {
		const n = randomIDs
		rng := rand.New(rand.NewPCG(1, 2))
		ids := make([]uint64, n)
		for i := range ids {
			ids[i] = rng.Uint64()
		}
		g, s := grew(func() *Set {
			s := new(Set)
			for _, id := range ids {
				s.Add(id)
			}
			return s
		})
		runtime.KeepAlive(s)
		runtime.KeepAlive(ids)
		if limit := int64(n)*16 + 1<<20; g > limit {
			t.Fatalf("%d random IDs grew the heap by %d bytes, want at most %d (16 bytes per ID plus 1 MiB)", n, g, limit)
		}
	})
	t.Run("huge single", func(t *testing.T) {
		g, s := grew(func() *Set {
			s := new(Set)
			s.Add(1 << 62)
			return s
		})
		runtime.KeepAlive(s)
		if g > 1<<20 {
			t.Fatalf("Add(1<<62) grew the heap by %d bytes, want under 1 MiB", g)
		}
		if !s.Has(1<<62) || s.Len() != 1 {
			t.Fatal("set lost the huge ID")
		}
	})
}

func TestSetDenseAddHasAllocateNothing(t *testing.T) {
	var s Set
	const n = 1 << 16
	for i := uint64(1); i <= n; i++ {
		s.Add(i)
	}
	// Re-adding and probing IDs inside the sized set must not allocate.
	var i uint64
	if a := testing.AllocsPerRun(1000, func() {
		i = i%n + 1
		if s.Add(i) {
			t.Fatal("Add of a member reported new")
		}
	}); a != 0 {
		t.Fatalf("Add on a sized set allocated %v times per call", a)
	}
	if a := testing.AllocsPerRun(1000, func() {
		i = i%n + 1
		if !s.Has(i) {
			t.Fatal("Has of a member = false")
		}
	}); a != 0 {
		t.Fatalf("Has allocated %v times per call", a)
	}
}
