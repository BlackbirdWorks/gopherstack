// Package collections provides small, type-safe slice and map helpers that the
// Go standard library's slices and maps packages do not cover. Prefer the
// stdlib directly where it suffices (slices.Sorted, slices.Contains,
// maps.Keys/Values, etc.); this package only adds the transformation and
// filtering helpers that would otherwise be hand-rolled as range loops in
// hundreds of handler list-builders.
package collections

import (
	"cmp"
	"slices"
)

// Map returns a new slice holding f(v) for each element of s, preserving order.
// It is the type-safe replacement for the `out := make([]B, 0, len(s)); for ...
// { out = append(out, f(v)) }` pattern used pervasively to convert internal
// records into wire/response shapes.
func Map[A, B any](s []A, f func(A) B) []B {
	if s == nil {
		return nil
	}

	out := make([]B, len(s))
	for i, v := range s {
		out[i] = f(v)
	}

	return out
}

// Filter returns a new slice containing only the elements of s for which keep
// returns true, preserving order. Returns nil when s is nil.
func Filter[T any](s []T, keep func(T) bool) []T {
	if s == nil {
		return nil
	}

	out := make([]T, 0, len(s))
	for _, v := range s {
		if keep(v) {
			out = append(out, v)
		}
	}

	return out
}

// MapValues returns a slice of f(v) for each value in m. Iteration order is
// unspecified (map order); use SortedKeys plus a lookup when order matters.
func MapValues[K comparable, V, B any](m map[K]V, f func(V) B) []B {
	if m == nil {
		return nil
	}

	out := make([]B, 0, len(m))
	for _, v := range m {
		out = append(out, f(v))
	}

	return out
}

// Values returns the values of m as a slice in unspecified order. It is a
// convenience wrapper over slices.Collect(maps.Values(m)).
func Values[K comparable, V any](m map[K]V) []V {
	if m == nil {
		return nil
	}

	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}

	return out
}

// SortedKeys returns the keys of m sorted in ascending order. It replaces the
// common `for k := range m { keys = append(keys, k) }; sort.Strings(keys)`
// idiom with a single type-safe call.
func SortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	if m == nil {
		return nil
	}

	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	return keys
}
