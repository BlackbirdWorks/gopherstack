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
	indexCore
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
	idx := new(Index[V])
	t.addIndex(&idx.indexCore, name, eraseKeyFn(keyFn))

	return idx
}

// Get returns the values currently grouped under key. Iteration order within
// the group is insertion order, not any table-defined order. The returned
// slice is owned by the index — the caller must not mutate it, and must not
// retain it across a subsequent [Table.Put]/[Table.Delete]/[Table.Restore]
// call, which may reuse or invalidate it. Copy the slice first if you need to
// hold onto it past the current lock scope.
func (idx *Index[V]) Get(key string) []*V {
	g := idx.groups[key]
	if g == nil {
		return nil
	}

	return typedSlice[V](g)
}
