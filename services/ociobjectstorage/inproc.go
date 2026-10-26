package ociobjectstorage

import "sort"

// AllBuckets lists every bucket of the namespace across compartments, sorted by name.
func (b *InMemoryBackend) AllBuckets() []*BucketWire {
	b.mu.RLock("AllBuckets")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.buckets))
	for n := range b.buckets {
		names = append(names, n)
	}

	sort.Strings(names)

	out := make([]*BucketWire, 0, len(names))
	for _, n := range names {
		out = append(out, b.bucketWire(b.namespace, b.buckets[n], []string{"approximateCount", "approximateSize"}))
	}

	return out
}
