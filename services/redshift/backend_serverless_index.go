package redshift

import "sort"

// sortedStringIndex maintains a set of keys in ascending sorted order.
//
// Redshift Serverless list operations return resources sorted by name/ID. The
// backing stores are Go maps (unordered), so previously every List call did an
// O(n log n) sort.Slice on every read. This index keeps the keys sorted as
// resources are created and deleted (O(n) insert/delete via binary search +
// shift), letting the hot read path iterate keys in order with zero per-read
// sorting.
type sortedStringIndex struct {
	keys []string
}

// insert adds k to the index, preserving sorted order. Duplicate keys are ignored.
func (s *sortedStringIndex) insert(k string) {
	i := sort.SearchStrings(s.keys, k)
	if i < len(s.keys) && s.keys[i] == k {
		return
	}

	s.keys = append(s.keys, "")
	copy(s.keys[i+1:], s.keys[i:])
	s.keys[i] = k
}

// remove deletes k from the index if present, preserving sorted order.
func (s *sortedStringIndex) remove(k string) {
	i := sort.SearchStrings(s.keys, k)
	if i < len(s.keys) && s.keys[i] == k {
		s.keys = append(s.keys[:i], s.keys[i+1:]...)
	}
}

// ordered returns the keys in ascending order. The returned slice is the
// internal backing store and MUST NOT be mutated by callers; callers read it
// under the backend lock.
func (s *sortedStringIndex) ordered() []string {
	return s.keys
}

// reset clears all keys from the index.
func (s *sortedStringIndex) reset() {
	s.keys = nil
}

// rebuildFromKeys replaces the index contents with a sorted copy of keys. Used
// after a bulk state load (Restore) where the maps are swapped wholesale.
func (s *sortedStringIndex) rebuildFromKeys(keys []string) {
	cp := make([]string, len(keys))
	copy(cp, keys)
	sort.Strings(cp)
	s.keys = cp
}

// rebuildServerlessIndexes reconstructs all serverless sorted indexes from the
// current backing maps. Callers MUST hold b.mu for writing (it is invoked from
// Restore, which swaps every map under the lock).
func (b *InMemoryBackend) rebuildServerlessIndexes() {
	b.slNamespaceIdx.rebuildFromKeys(mapKeys(b.slNamespaces))
	b.slWorkgroupIdx.rebuildFromKeys(mapKeys(b.slWorkgroups))
	b.slSnapshotIdx.rebuildFromKeys(mapKeys(b.slSnapshots))
	b.slUsageLimitIdx.rebuildFromKeys(mapKeys(b.slUsageLimits))
	b.slScheduledActionIdx.rebuildFromKeys(mapKeys(b.slScheduledActions))
}

// resetServerlessIndexes clears every serverless sorted index.
func (b *InMemoryBackend) resetServerlessIndexes() {
	b.slNamespaceIdx.reset()
	b.slWorkgroupIdx.reset()
	b.slSnapshotIdx.reset()
	b.slUsageLimitIdx.reset()
	b.slScheduledActionIdx.reset()
}

// mapKeys returns the keys of m in unspecified order.
func mapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
