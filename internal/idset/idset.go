// Package idset holds sets of file IDs compactly. Manifest IDs are dense in
// practice, so the set is a bitset, but IDs come from the server and one huge
// ID must not force an allocation proportional to its value.
package idset

import (
	"iter"
	"math/bits"
	"slices"
)

const (
	// slackWords is how many bitset words beyond the set's size the bitset
	// may span before the set falls back to a sorted slice.
	slackWords = 16
	// minPending is the smallest number of IDs the sparse form buffers
	// before it merges them into the sorted slice.
	minPending = 64
)

// Set is a set of file IDs. The zero value is empty. It is not safe for
// concurrent use with a writer. Reads (Has, Len, All, Clone, and Union's
// argument) never modify the set.
//
// Do not copy a Set after use: a copy shares its backing arrays, and a write
// to either corrupts the other. Pass *Set, or call Clone. The methods accept a
// nil *Set as an empty set for reading.
//
// Memory contract: a set of the dense IDs 1..N uses about N/8 bytes plus a
// small constant. Any set of N distinct IDs uses at most about 16 bytes per ID
// plus a constant, however large the IDs are.
//
// The set has two forms. The bitset form holds one bit per possible ID up to
// the largest member. It is used while that span is at most Len()+16 words,
// which is at most about 10 bytes per member. Otherwise the set holds a
// sorted []uint64 (8 bytes per member, up to twice that with slice growth)
// plus a short sorted buffer of recent additions that spares the large slice
// an insertion per Add; the buffer merges in once it passes about the square
// root of the slice's length. A sparse set returns to the bitset form when it
// merges and its span has fallen under Len()/2 words. The gap between the two
// limits keeps a set near the boundary from converting back and forth.
type Set struct {
	_      noCopy
	n      int
	sparse bool
	words  []uint64 // bitset form: bit id%64 of word id/64
	ids    []uint64 // sparse form: sorted members, merged
	pend   []uint64 // sparse form: sorted members not yet merged into ids
}

// noCopy makes go vet's copylocks check flag copies of a Set.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Len returns the number of IDs in the set.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return s.n
}

// Has reports whether id is in the set.
func (s *Set) Has(id uint64) bool {
	if s == nil {
		return false
	}
	if !s.sparse {
		w := id >> 6
		return w < uint64(len(s.words)) && s.words[w]&(1<<(id&63)) != 0
	}
	if _, found := slices.BinarySearch(s.ids, id); found {
		return true
	}
	_, found := slices.BinarySearch(s.pend, id)
	return found
}

// Add inserts id and reports whether it was new. In the bitset form it
// allocates only to grow the bitset.
func (s *Set) Add(id uint64) bool {
	if !s.sparse {
		w := id >> 6
		if w >= uint64(len(s.words)) {
			if w+1 > uint64(s.n+1+slackWords) {
				s.toSparse()
				return s.addSparse(id)
			}
			s.growWords(int(w) + 1)
		}
		bit := uint64(1) << (id & 63)
		if s.words[w]&bit != 0 {
			return false
		}
		s.words[w] |= bit
		s.n++
		return true
	}
	return s.addSparse(id)
}

func (s *Set) addSparse(id uint64) bool {
	if s.Has(id) {
		return false
	}
	i, _ := slices.BinarySearch(s.pend, id)
	s.pend = slices.Insert(s.pend, i, id)
	s.n++
	if len(s.pend) > s.pendingLimit() {
		s.flush()
	}
	return true
}

// pendingLimit is how many IDs the pending buffer holds before it merges:
// about the square root of the merged slice's length.
func (s *Set) pendingLimit() int {
	return max(minPending, 1<<(bits.Len(uint(len(s.ids)))/2))
}

// growWords extends the bitset to n words, with a quarter more capacity.
func (s *Set) growWords(n int) {
	if n <= cap(s.words) {
		s.words = s.words[:n]
		return
	}
	grown := make([]uint64, n, n+n/4)
	copy(grown, s.words)
	s.words = grown
}

// toSparse converts the bitset form to the sparse form.
func (s *Set) toSparse() {
	ids := make([]uint64, 0, s.n)
	for id := range s.All() {
		ids = append(ids, id)
	}
	s.words, s.ids, s.pend, s.sparse = nil, ids, nil, true
}

// mergePending merges the pending buffer into ids in place, from the back.
// Pending members are never in ids.
func (s *Set) mergePending() {
	if len(s.pend) == 0 {
		return
	}
	old := len(s.ids)
	s.ids = slices.Grow(s.ids, len(s.pend))[:old+len(s.pend)]
	i, j := old-1, len(s.pend)-1
	for k := len(s.ids) - 1; j >= 0; k-- {
		if i >= 0 && s.ids[i] > s.pend[j] {
			s.ids[k] = s.ids[i]
			i--
		} else {
			s.ids[k] = s.pend[j]
			j--
		}
	}
	s.pend = s.pend[:0]
}

// flush merges the pending buffer, then returns to the bitset form if the
// span now allows it.
func (s *Set) flush() {
	s.mergePending()
	s.maybeDense()
}

// maybeDense switches a merged sparse set to the bitset form when its span is
// under Len()/2 words.
func (s *Set) maybeDense() {
	if len(s.ids) == 0 || s.ids[len(s.ids)-1]>>6 >= uint64(s.n/2) {
		return
	}
	words := make([]uint64, s.ids[len(s.ids)-1]>>6+1)
	for _, id := range s.ids {
		words[id>>6] |= 1 << (id & 63)
	}
	s.words, s.ids, s.pend, s.sparse = words, nil, nil, false
}

// All iterates the IDs in ascending order without modifying the set. The set
// must not be written while the iteration runs.
func (s *Set) All() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		if s == nil {
			return
		}
		if s.sparse {
			// Merge ids and pend, which are disjoint and sorted.
			ids, pend := s.ids, s.pend
			for len(ids) > 0 || len(pend) > 0 {
				var id uint64
				if len(pend) == 0 || (len(ids) > 0 && ids[0] < pend[0]) {
					id, ids = ids[0], ids[1:]
				} else {
					id, pend = pend[0], pend[1:]
				}
				if !yield(id) {
					return
				}
			}
			return
		}
		for wi, w := range s.words {
			for ; w != 0; w &= w - 1 {
				if !yield(uint64(wi)<<6 | uint64(bits.TrailingZeros64(w))) {
					return
				}
			}
		}
	}
}

// Clone returns an independent copy.
func (s *Set) Clone() Set {
	if s == nil {
		return Set{}
	}
	return Set{
		n:      s.n,
		sparse: s.sparse,
		words:  slices.Clone(s.words),
		ids:    slices.Clone(s.ids),
		pend:   slices.Clone(s.pend),
	}
}

// Union adds every member of o to s. It does not modify o.
func (s *Set) Union(o *Set) {
	if o == s || o.Len() == 0 {
		return
	}
	// Add decides the form member by member, so the set never grows past its
	// per-ID bound. A long list into a sparse set takes one linear merge
	// instead. That remainder is collected as a slice, so a dense o merged
	// into a sparse s costs 8 bytes per member of o until the merge ends.
	var rest []uint64 // o's members still to merge, ascending
	if o.sparse && len(o.pend) == 0 {
		rest = o.ids // already a slice; no copy
		i := 0
		for ; i < len(rest); i++ {
			if s.sparse && len(rest)-i > s.pendingLimit() {
				break
			}
			s.Add(rest[i])
		}
		if i == len(rest) {
			return
		}
		rest = rest[i:]
	} else {
		left := o.n
		for id := range o.All() {
			if rest == nil && s.sparse && left > s.pendingLimit() {
				rest = make([]uint64, 0, left)
			}
			if rest != nil {
				rest = append(rest, id)
			} else {
				s.Add(id)
			}
			left--
		}
		if rest == nil {
			return
		}
	}
	s.mergePending()
	merged := make([]uint64, 0, len(s.ids)+len(rest))
	x, y := 0, 0
	for x < len(s.ids) || y < len(rest) {
		switch {
		case y == len(rest) || (x < len(s.ids) && s.ids[x] < rest[y]):
			merged = append(merged, s.ids[x])
			x++
		case x == len(s.ids) || rest[y] < s.ids[x]:
			merged = append(merged, rest[y])
			y++
		default:
			merged = append(merged, s.ids[x])
			x++
			y++
		}
	}
	s.ids, s.n = merged, len(merged)
	s.maybeDense()
}
