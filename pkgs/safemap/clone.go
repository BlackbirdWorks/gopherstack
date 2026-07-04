package safemap

// ClonePtrMap returns a deep copy of a map whose values are pointers: the
// returned map has the same keys, but each value points to a freshly allocated
// copy of the pointed-to value (nil pointers are preserved as nil).
//
// This differs from the standard library's [maps.Clone], which copies the map
// structure but shares the underlying pointers — insufficient for a stable
// point-in-time snapshot, where the copy must not observe later mutations of
// the original values. Returns nil when m is nil.
func ClonePtrMap[K comparable, V any](m map[K]*V) map[K]*V {
	if m == nil {
		return nil
	}

	out := make(map[K]*V, len(m))
	for k, v := range m {
		if v == nil {
			out[k] = nil

			continue
		}

		cp := *v
		out[k] = &cp
	}

	return out
}

// CloneRegionalPtrMap returns a deep copy of the region-nested pointer maps used
// pervasively by service backends (outer key = region, inner key = resource
// name, value = *T). Both map levels are reallocated and each value is
// value-copied via [ClonePtrMap]. Returns nil when m is nil.
func CloneRegionalPtrMap[R comparable, K comparable, V any](m map[R]map[K]*V) map[R]map[K]*V {
	if m == nil {
		return nil
	}

	out := make(map[R]map[K]*V, len(m))
	for region, inner := range m {
		out[region] = ClonePtrMap(inner)
	}

	return out
}
