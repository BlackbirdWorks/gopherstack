package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// tableGet returns the value stored under key in t, or nil if absent. It lets
// a single-value store.Table lookup be substituted inline (including chained
// field access, e.g. tableGet(t, key).Field) for the raw map[string]*V index
// expression it replaces: a missing key yields a nil *V, matching Go's
// zero-value-on-missing-key map semantics, so callers that previously
// tolerated a nil result unchanged.
func tableGet[V any](t *store.Table[V], key string) *V {
	v, _ := t.Get(key)

	return v
}

// sagemakerListPaged paginates a store.Table using index-based tokens.
// clone must return a deep copy of its argument.
// less defines the sort order.
func sagemakerListPaged[T any](
	tbl *store.Table[T],
	nextToken string,
	clone func(*T) *T,
	less func(a, b *T) bool,
) ([]*T, string) {
	return sagemakerListPagedSlice(tbl.All(), nextToken, clone, less)
}

// sagemakerListPagedMap paginates a plain map[string]*T using index-based
// tokens. It preserves the pre-conversion behavior for callers that build a
// local, already-filtered map (e.g. a subset matching a query filter) rather
// than listing a store.Table directly.
func sagemakerListPagedMap[T any](
	m map[string]*T,
	nextToken string,
	clone func(*T) *T,
	less func(a, b *T) bool,
) ([]*T, string) {
	items := make([]*T, 0, len(m))
	for _, item := range m {
		items = append(items, item)
	}

	return sagemakerListPagedSlice(items, nextToken, clone, less)
}

// sagemakerListPagedSlice is the shared index-based-token pagination core
// used by sagemakerListPaged and sagemakerListPagedMap.
func sagemakerListPagedSlice[T any](
	items []*T,
	nextToken string,
	clone func(*T) *T,
	less func(a, b *T) bool,
) ([]*T, string) {
	list := make([]*T, 0, len(items))
	for _, item := range items {
		list = append(list, clone(item))
	}

	sort.Slice(list, func(i, j int) bool { return less(list[i], list[j]) })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*T{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize

	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// sagemakerListKeyPagedMap paginates a plain map[string]*T using name-key-based
// tokens. It preserves the pre-conversion behavior for the small number of
// resource collections not (yet) backed by a store.Table — e.g. a Cluster's
// Nodes, which is itself a value nested inside the Cluster store.Table entry
// rather than its own top-level registered table.
func sagemakerListKeyPagedMap[T any](
	m map[string]*T,
	nextToken string,
	clone func(*T) *T,
) ([]*T, string) {
	keys := collections.SortedKeys(m)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*T, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, clone(m[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// sagemakerListKeyPaged paginates a store.Table using name-key-based tokens.
// keyFn must reproduce the exact same primary key the table was registered
// with (i.e. its keyFn), so nextToken values remain meaningful key strings.
// clone must return a deep copy of its argument.
func sagemakerListKeyPaged[T any](
	tbl *store.Table[T],
	nextToken string,
	clone func(*T) *T,
	keyFn func(*T) string,
) ([]*T, string) {
	// tbl.Snapshot() is already sorted ascending by the table's own primary
	// key, i.e. by keyFn(item) — the same order collections.SortedKeys(map)
	// produced over the raw map this table replaced.
	items := tbl.Snapshot()

	start := 0
	if nextToken != "" {
		for i, item := range items {
			if keyFn(item) == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(items))

	out := make([]*T, 0, end-start)
	for _, item := range items[start:end] {
		out = append(out, clone(item))
	}

	next := ""
	if end < len(items) {
		next = keyFn(items[end])
	}

	return out, next
}

// sagemakerCreate handles the common create-resource-by-name pattern:
// acquire lock, check for duplicate, build ARN, build item, store, return clone.
func sagemakerCreate[T any](
	ctx context.Context,
	b *InMemoryBackend,
	opName, name, arnResource string,
	storeOf func(string) *store.Table[T],
	dupErr func(string) error,
	build func(arnStr string, now time.Time) *T,
	clone func(*T) *T,
) (*T, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock(opName)
	defer b.mu.Unlock()

	tbl := storeOf(region)
	if _, ok := tbl.Get(name); ok {
		return nil, dupErr(name)
	}

	arnStr := arn.Build("sagemaker", region, b.accountID, arnResource+"/"+name)
	now := time.Now()

	item := build(arnStr, now)
	tbl.Put(item)

	return clone(item), nil
}

// sagemakerDupErr returns a formatted "already exists" error wrapping ErrValidation.
func sagemakerDupErr(kind, name string) error {
	return fmt.Errorf("%w: %s %q already exists", ErrValidation, kind, name)
}

// compareTimes returns -1, 0 or 1 depending on whether a is before, equal to,
// or after b. It is used to build strict weak orderings for sort.Slice-style
// comparators that need to support both ascending and descending order.
func compareTimes(a, b time.Time) int {
	switch {
	case a.Before(b):
		return -1
	case a.After(b):
		return 1
	default:
		return 0
	}
}

// paginateSlice applies index-based-token pagination to an already-sorted
// slice, capping the page size at maxResults (if positive) or
// sagemakerDefaultPageSize.
func paginateSlice[T any](list []T, nextToken string, maxResults int32) ([]T, string) {
	pageSize := sagemakerDefaultPageSize
	if maxResults > 0 && int(maxResults) < pageSize {
		pageSize = int(maxResults)
	}

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []T{}, ""
	}

	end := startIdx + pageSize

	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}
