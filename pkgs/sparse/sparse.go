// Package sparse implements sparse byte Extents: written runs of a file that
// otherwise reads back as zeroes, with copy-on-write sharing of the backing bytes.
package sparse

import (
	"io"
	"sort"
)

// Extent is one contiguous run of written bytes.
type Extent struct {
	Data []byte `json:"data"`
	Off  int64  `json:"off"`
}

func (e Extent) end() int64 { return e.Off + int64(len(e.Data)) }

// Extents is a sorted, non-overlapping, non-adjacent list of written runs.
// Backing byte slices are never mutated after creation, so clones share them.
type Extents []Extent

// Range is an inclusive byte range.
type Range struct{ Start, End int64 }

// Clone copies the run list; the backing bytes are shared.
func (x Extents) Clone() Extents {
	return append(Extents(nil), x...)
}

// Write stores data at off, replacing anything it overlaps.
func (x Extents) Write(off int64, data []byte) Extents {
	if len(data) == 0 {
		return x
	}

	end := off + int64(len(data))
	out := x.cut(off, end)
	out = append(out, Extent{Off: off, Data: append([]byte(nil), data...)})

	return out.normalize()
}

// Clear removes [off, off+n) so it reads back as zeroes.
func (x Extents) Clear(off, n int64) Extents {
	return x.cut(off, off+n)
}

// Truncate drops everything at or beyond size.
func (x Extents) Truncate(size int64) Extents {
	out := make(Extents, 0, len(x))

	for _, e := range x {
		switch {
		case e.Off >= size:
		case e.end() > size:
			out = append(out, Extent{Off: e.Off, Data: e.Data[:size-e.Off]})
		default:
			out = append(out, e)
		}
	}

	return out
}

func (x Extents) cut(from, to int64) Extents {
	out := make(Extents, 0, len(x)+1)

	for _, e := range x {
		if e.end() <= from || e.Off >= to {
			out = append(out, e)

			continue
		}

		if e.Off < from {
			out = append(out, Extent{Off: e.Off, Data: e.Data[:from-e.Off]})
		}

		if e.end() > to {
			out = append(out, Extent{Off: to, Data: e.Data[to-e.Off:]})
		}
	}

	return out
}

func (x Extents) normalize() Extents {
	sort.Slice(x, func(i, j int) bool { return x[i].Off < x[j].Off })

	out := make(Extents, 0, len(x))

	for _, e := range x {
		if n := len(out); n > 0 && out[n-1].end() == e.Off {
			merged := make([]byte, 0, len(out[n-1].Data)+len(e.Data))
			merged = append(merged, out[n-1].Data...)
			merged = append(merged, e.Data...)
			out[n-1] = Extent{Off: out[n-1].Off, Data: merged}

			continue
		}

		out = append(out, e)
	}

	return out
}

// Read returns n bytes starting at off, zero-filled where nothing was written.
func (x Extents) Read(off, n int64) []byte {
	buf := make([]byte, n)

	for _, e := range x {
		if e.end() <= off || e.Off >= off+n {
			continue
		}

		lo := max(e.Off, off)
		hi := min(e.end(), off+n)
		copy(buf[lo-off:hi-off], e.Data[lo-e.Off:hi-e.Off])
	}

	return buf
}

// Ranges lists written runs clipped to [from, to] (inclusive).
func (x Extents) Ranges(from, to int64) []Range {
	var out []Range

	for _, e := range x {
		lo := max(e.Off, from)
		hi := min(e.end()-1, to)

		if lo <= hi {
			out = append(out, Range{Start: lo, End: hi})
		}
	}

	return out
}

// Presence marks which of the n bytes from off lie inside a written run.
func (x Extents) Presence(off, n int64) []bool {
	mask := make([]bool, n)

	for _, r := range x.Ranges(off, off+n-1) {
		for i := r.Start; i <= r.End; i++ {
			mask[i-off] = true
		}
	}

	return mask
}

// Diff returns ranges of x that differ from prev (changed) and ranges
// written in prev but absent from x (cleared), clipped to [0, size).
func (x Extents) Diff(prev Extents, size int64) ([]Range, []Range) {
	var changed, cleared []Range

	for _, r := range x.Ranges(0, size-1) {
		before := prev.Read(r.Start, r.End-r.Start+1)
		now := x.Read(r.Start, r.End-r.Start+1)
		changed = appendDiffRuns(changed, r.Start, before, now, prev.Presence(r.Start, r.End-r.Start+1))
	}

	for _, r := range prev.Ranges(0, size-1) {
		cleared = appendClearedRuns(cleared, r, x)
	}

	return changed, cleared
}

func appendDiffRuns(out []Range, base int64, before, now []byte, inPrev []bool) []Range {
	start := int64(-1)

	for i := range now {
		differs := before[i] != now[i] || !inPrev[i]
		switch {
		case differs && start < 0:
			start = int64(i)
		case !differs && start >= 0:
			out = append(out, Range{Start: base + start, End: base + int64(i) - 1})
			start = -1
		}
	}

	if start >= 0 {
		out = append(out, Range{Start: base + start, End: base + int64(len(now)) - 1})
	}

	return out
}

func appendClearedRuns(out []Range, r Range, cur Extents) []Range {
	start := int64(-1)
	inCur := cur.Presence(r.Start, r.End-r.Start+1)

	for i := r.Start; i <= r.End; i++ {
		gone := !inCur[i-r.Start]
		switch {
		case gone && start < 0:
			start = i
		case !gone && start >= 0:
			out = append(out, Range{Start: start, End: i - 1})
			start = -1
		}
	}

	if start >= 0 {
		out = append(out, Range{Start: start, End: r.End})
	}

	return out
}

// Reader streams [off, end] of a sparse file, zero-filling gaps.
// Reader streams a byte range of Extents, zero-filling gaps.
type Reader struct {
	x   Extents
	off int64
	end int64
}

// NewReader streams bytes [off, end] (inclusive) of x.
func NewReader(x Extents, off, end int64) *Reader { return &Reader{x: x, off: off, end: end} }

func (r *Reader) Read(p []byte) (int, error) {
	if r.off > r.end {
		return 0, io.EOF
	}

	n := min(int64(len(p)), r.end-r.off+1)
	i := sort.Search(len(r.x), func(i int) bool { return r.x[i].end() > r.off })

	if i == len(r.x) || r.x[i].Off > r.off {
		if i < len(r.x) {
			n = min(n, r.x[i].Off-r.off)
		}

		clear(p[:n])
		r.off += n

		return int(n), nil
	}

	e := r.x[i]
	n = min(n, e.end()-r.off)
	copy(p[:n], e.Data[r.off-e.Off:])
	r.off += n

	return int(n), nil
}
