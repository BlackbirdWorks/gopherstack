package elasticache

import (
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// describePaged handles the common lookup-or-paginate pattern for Describe* operations.
// If id is non-empty, a single item is returned (or notFoundErr if missing).
// Otherwise all items are collected, sorted by key(), and paginated.
func describePaged[T any](
	store map[string]*T,
	id string,
	notFoundErr error,
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
		out = append(out, *item)
	}

	sort.Slice(out, func(i, j int) bool { return key(out[i]) < key(out[j]) })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}
