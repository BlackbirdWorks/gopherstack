package gcpbigtable

import (
	"slices"
	"sort"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
)

const (
	microsPerMilli = 1000
	maxKeyLen      = 4096
	maxQualLen     = 16384
)

func (b *InMemoryBackend) nowMicros() int64 { return b.now().UnixMicro() }

func (t *tableRec) gran() int64 {
	if t.pb.GetGranularity() == adminpb.Table_MICROS {
		return 1
	}

	return microsPerMilli
}

func (t *tableRec) sortedKeys() []string {
	if t.dirty {
		t.keys = t.keys[:0]
		for k := range t.rows {
			t.keys = append(t.keys, k)
		}

		sort.Strings(t.keys)
		t.dirty = false
	}

	return t.keys
}

func (t *tableRec) row(key string) *rowRec {
	r, ok := t.rows[key]
	if !ok {
		r = &rowRec{fams: make(map[string]family)}
		t.rows[key] = r
		t.dirty = true
	}

	return r
}

func (t *tableRec) delRow(key string) {
	if _, ok := t.rows[key]; ok {
		delete(t.rows, key)

		t.dirty = true
	}
}

func (r *rowRec) set(fam, qual string, ts int64, val []byte) {
	f := r.fams[fam]
	if f == nil {
		f = family{}
		r.fams[fam] = f
	}

	cells := f[qual]
	i := sort.Search(len(cells), func(i int) bool { return cells[i].ts <= ts })

	if i < len(cells) && cells[i].ts == ts {
		cells[i].val = val

		return
	}

	cells = slices.Insert(cells, i, cell{ts: ts, val: val})
	f[qual] = cells
}

// deleteRange removes cells with start <= ts < end from a column (end 0 means unbounded).
func (r *rowRec) deleteRange(fam, qual string, start, end int64) {
	f := r.fams[fam]
	if f == nil {
		return
	}

	kept := f[qual][:0:0]

	for _, c := range f[qual] {
		if c.ts >= start && (end == 0 || c.ts < end) {
			continue
		}

		kept = append(kept, c)
	}

	if len(kept) == 0 {
		delete(f, qual)
	} else {
		f[qual] = kept
	}

	if len(f) == 0 {
		delete(r.fams, fam)
	}
}

func (r *rowRec) empty() bool { return len(r.fams) == 0 }

func (t *tableRec) dropFamily(fam string) {
	for k, r := range t.rows {
		delete(r.fams, fam)

		if r.empty() {
			t.delRow(k)
		}
	}
}

// compact physically removes cells that the families' GC rules consider garbage.
func (t *tableRec) compact(nowMicros int64) {
	for k, r := range t.rows {
		t.compactRow(k, r, nowMicros)
	}
}

func (t *tableRec) compactRow(key string, r *rowRec, nowMicros int64) {
	for fam, f := range r.fams {
		rule := t.pb.GetColumnFamilies()[fam].GetGcRule()
		if rule == nil {
			continue
		}

		for q, cells := range f {
			if kept := live(rule, cells, nowMicros); len(kept) == 0 {
				delete(f, q)
			} else if len(kept) != len(cells) {
				f[q] = slices.Clone(kept)
			}
		}

		if len(f) == 0 {
			delete(r.fams, fam)
		}
	}

	if r.empty() {
		t.delRow(key)
	}
}

// liveCells returns the row's live cells in Bigtable order: family, qualifier, then newest first.
func (t *tableRec) liveCells(r *rowRec, nowMicros int64) []fcell {
	var out []fcell

	for _, fam := range sortedKeys(r.fams) {
		rule := t.pb.GetColumnFamilies()[fam].GetGcRule()
		f := r.fams[fam]

		for _, q := range sortedKeys(f) {
			for _, c := range live(rule, f[q], nowMicros) {
				out = append(out, fcell{fam: fam, qual: q, ts: c.ts, val: c.val})
			}
		}
	}

	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func (r *rowRec) size() int64 {
	var n int64

	for fam, f := range r.fams {
		for q, cells := range f {
			for _, c := range cells {
				n += int64(len(fam) + len(q) + len(c.val))
			}
		}
	}

	return n
}

func cloneTable(t *tableRec) *tableRec {
	out := &tableRec{
		pb:     clone(t.pb),
		rows:   make(map[string]*rowRec, len(t.rows)),
		splits: slices.Clone(t.splits),
		dirty:  true,
	}

	for k, r := range t.rows {
		nr := &rowRec{fams: make(map[string]family, len(r.fams))}

		for fam, f := range r.fams {
			nf := make(family, len(f))
			for q, cells := range f {
				nf[q] = slices.Clone(cells)
			}

			nr.fams[fam] = nf
		}

		out.rows[k] = nr
	}

	return out
}
