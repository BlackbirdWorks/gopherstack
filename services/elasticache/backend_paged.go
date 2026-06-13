package elasticache

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// describePaged handles the common lookup-or-paginate pattern for Describe* operations.
// If id is non-empty, a single item is returned (or notFoundErr if missing).
// Otherwise all items are collected, optionally filtered, sorted by key(), and paginated.
// A nil filter includes every item.
func describePaged[T any](
	store map[string]*T,
	id string,
	notFoundErr error,
	filter func(T) bool,
	key func(T) string,
	marker string,
	maxRecords int,
) (page.Page[T], error) {
	if id != "" {
		item, exists := store[id]
		if !exists {
			return page.Page[T]{}, notFoundErr
		}

		return page.Page[T]{Data: []T{*item}}, nil
	}

	out := make([]T, 0, len(store))
	for _, item := range store {
		if filter == nil || filter(*item) {
			out = append(out, *item)
		}
	}

	sort.Slice(out, func(i, j int) bool { return key(out[i]) < key(out[j]) })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}
