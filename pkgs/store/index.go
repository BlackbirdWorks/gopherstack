package store

// Index is an opt-in secondary index over a [Table]'s values, grouping them by
// a caller-supplied key function. It exists for real filter hot paths — e.g.
// "all SQS messages in queue X" — where a linear scan of [Table.All] would
// otherwise be required on every call.
//
// The zero value is not usable; always create via [Table.AddIndex].
//
// Like [Table], Index performs no locking of its own; it is maintained
// in-line by [Table.Put], [Table.Delete], and [Table.Restore] under whatever
// external lock the owning backend already holds.
type Index[V any] struct {
	groups map[string][]*V
	// keys records, per value pointer, the group key it was last filed under.
	// remove uses this instead of recomputing idx.keyFn(v) so that a caller
	// which mutates a Table.Get'd pointer in place before calling Table.Put
	// (where the "old" and "new" value passed to Put are the very same,
	// already-mutated object) still removes the entry from the bucket it
	// actually lives in, not the bucket its now-mutated state would hash to.
	// See remove for the full rationale.
	keys  map[*V]string
	keyFn func(*V) string
	name  string
}

// AddIndex creates and registers a secondary [Index] on t, keyed by keyFn.
// name identifies the index for debugging/error messages (e.g. "queue"); it
// plays no role in lookup. The index is populated immediately from every
// value already in the table, then kept consistent by [Table.Put],
// [Table.Delete], and [Table.Restore] for the rest of the table's lifetime.
//
// A [Table] with no indexes pays no cost for this feature: [Table.Put] and
// [Table.Delete] range over t.indexes, which is a nil slice until AddIndex is
// first called, so the loop is zero iterations and zero allocations.
//
// keyFn is only consulted when a value is added to the index (via
// [Table.Put], [Table.Restore], or AddIndex's own initial population); if a
// value is mutated in place through a pointer obtained from [Table.Get] such
// that keyFn's result would change, the index becomes stale for that value
// until the caller re-files it with [Table.Put]. Put (even called with that
// same, already-mutated pointer) correctly relocates the entry to its new
// key and clears the old one — see [Index.remove] — so the caveat is
// "re-insert to refresh," not "re-insert or the old bucket leaks forever."
// This is the same staleness-until-reinsert caveat every secondary index
// carries (relational or otherwise) and is not specific to this package.
func (t *Table[V]) AddIndex(name string, keyFn func(*V) string) *Index[V] {
	idx := &Index[V]{
		groups: make(map[string][]*V),
		keys:   make(map[*V]string),
		keyFn:  keyFn,
		name:   name,
	}

	for _, v := range t.data {
		idx.add(v)
	}

	t.indexes = append(t.indexes, idx)

	return idx
}

// Name returns the identifier passed to [Table.AddIndex] when this index was
// created.
func (idx *Index[V]) Name() string {
	return idx.name
}

// Get returns the values currently grouped under key. Iteration order within
// the group is insertion order, not any table-defined order. The returned
// slice is owned by the index — the caller must not mutate it, and must not
// retain it across a subsequent [Table.Put]/[Table.Delete]/[Table.Restore]
// call, which may reuse or invalidate it. Copy the slice first if you need to
// hold onto it past the current lock scope.
func (idx *Index[V]) Get(key string) []*V {
	return idx.groups[key]
}

// Len returns the number of distinct group keys currently populated.
func (idx *Index[V]) Len() int {
	return len(idx.groups)
}

// add inserts v into its group, deriving the group key from the value's
// current state, and records that key so a later remove(v) finds the right
// bucket even if v's fields change in the meantime.
func (idx *Index[V]) add(v *V) {
	key := idx.keyFn(v)
	idx.groups[key] = append(idx.groups[key], v)
	idx.keys[v] = key
}

// remove drops v from its group by pointer identity — always well-defined
// since *V is comparable regardless of whether V itself is. When the group
// becomes empty its map entry is deleted outright rather than left as a
// zero-length slice, so a table with high key churn does not accumulate
// empty index buckets.
//
// The group key comes from idx.keys — the key recorded when v was added —
// rather than a fresh idx.keyFn(v) call. This matters for [Table.Put]: when
// a caller mutates a value obtained from [Table.Get] in place and then calls
// Put, the "old" value Put passes to remove and the "new" value it passes to
// add are the same, already-mutated pointer. Recomputing the key from v's
// current state here would therefore compute the *new* key, look in the new
// (usually still-empty-for-v) bucket, find nothing, and leave the entry
// stranded in the old bucket forever — a silent, permanent index corruption.
// Using the recorded key finds the bucket v actually lives in regardless of
// what the caller has done to v's fields since it was added.
func (idx *Index[V]) remove(v *V) {
	key, ok := idx.keys[v]
	if !ok {
		return
	}

	delete(idx.keys, v)

	group := idx.groups[key]
	for i, item := range group {
		if item == v {
			group[i] = group[len(group)-1]
			group = group[:len(group)-1]

			break
		}
	}

	if len(group) == 0 {
		delete(idx.groups, key)
	} else {
		idx.groups[key] = group
	}
}

// clear empties every group and the key-tracking map without reallocating
// the backing maps, so it is ready for immediate reuse by
// [Table.reset]/[Table.Restore]. Clearing keys too matters for correctness,
// not just memory: leaving stale pointer entries around risks a future
// allocation reusing the same address and inheriting a bogus recorded key.
func (idx *Index[V]) clear() {
	clear(idx.groups)
	clear(idx.keys)
}
