package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// sortOrderDescending is the "Descending" SortOrder value shared by every
// List* op family that supports SortBy/SortOrder filtering (AIBenchmarkJob,
// AIRecommendationJob, AIWorkloadConfig, Job, TrainingPlan, ...) — pulled
// out to a constant so those call sites don't each repeat the same string
// literal (goconst).
const sortOrderDescending = "Descending"

// sortOrderAscending is the "Ascending" SortOrder value shared by every
// List* op family whose real default is Descending (so the code compares
// against the non-default value explicitly) — pulled out so those call
// sites don't each repeat the same string literal (goconst).
const sortOrderAscending = "Ascending"

// sortByLastModifiedTime is the "LAST_MODIFIED_TIME" SortBy value shared by
// the Image/ImageVersion/EdgeDeploymentPlan sort-by enums (each its own
// distinct type, but spelled identically) — pulled out so those call sites
// don't each repeat the same string literal (goconst).
const sortByLastModifiedTime = "LAST_MODIFIED_TIME"

// sortByName is the "NAME" SortBy value shared by the
// Cluster/DeviceFleet/EdgeDeploymentPlan sort-by enums (each its own
// distinct type, but spelled identically) — pulled out so those call sites
// don't each repeat the same string literal (goconst).
const sortByName = "NAME"

// sortByCreationTime and sortByStatus are two of AutoMLSortBy's three values
// (types/enums.go:1417-1424, "Name"|"CreationTime"|"Status" — mixed case,
// distinct from the ALL_CAPS sortByName above which belongs to a different
// enum family); "Name" needs no constant since it is each caller's default
// branch.
const (
	sortByCreationTime = "CreationTime"
	sortByStatus       = "Status"
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

// sagemakerListKeyPagedN is [sagemakerListKeyPaged] with a caller-supplied
// page size — maxResults <= 0 falls back to sagemakerDefaultPageSize,
// matching every other List op's undocumented-MaxResults behavior in this
// service.
func sagemakerListKeyPagedN[T any](
	tbl *store.Table[T],
	nextToken string,
	maxResults int32,
	clone func(*T) *T,
	keyFn func(*T) string,
) ([]*T, string) {
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

	pageSize := int(maxResults)
	if pageSize <= 0 {
		pageSize = sagemakerDefaultPageSize
	}

	end := min(start+pageSize, len(items))

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

// timeWindowOK reports whether t falls on or after `after` and on or before
// `before` — either bound nil means unconstrained on that side. Shared by
// every List op's CreationTime*/LastModifiedTime* window filter added in the
// gopherstack-oc9v inline-struct campaign, collapsing what would otherwise
// be a repeated pair of if-continue checks per time field.
func timeWindowOK(t time.Time, after, before *time.Time) bool {
	if after != nil && !t.After(*after) {
		return false
	}

	if before != nil && !t.Before(*before) {
		return false
	}

	return true
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

// parseNextToken parses a pagination token (integer offset) into a slice index.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// nameWindowSortParams bundles the CreationTime-window filter and name-only
// SortOrder shared by ListFlowDefinitions and ListHumanTaskUIs — both ops
// declare no SortBy field at all (only SortOrder), so it is applied to each
// backend's own pre-existing default order (ascending by name) rather than
// an invented sort dimension neither op's own doc supports.
type nameWindowSortParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	SortOrder          string
	MaxResults         int32
}

// filterSortPaginateByNameWindow filters items by params.CreationTime window,
// sorts by nameOf ascending (or descending if params.SortOrder is
// "Descending"), then paginates.
func filterSortPaginateByNameWindow[T any](
	items []*T,
	nextToken string,
	params nameWindowSortParams,
	creationTimeOf func(*T) time.Time,
	nameOf func(*T) string,
	clone func(*T) *T,
) ([]*T, string) {
	list := make([]*T, 0, len(items))

	for _, item := range items {
		if timeWindowOK(creationTimeOf(item), params.CreationTimeAfter, params.CreationTimeBefore) {
			list = append(list, clone(item))
		}
	}

	desc := params.SortOrder == sortOrderDescending
	sort.SliceStable(list, func(i, k int) bool {
		less := nameOf(list[i]) < nameOf(list[k])
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, params.MaxResults)
}

// nameOrTimeSortParams bundles the type/source-URI/creation-window filter and
// Name-or-CreationTime sort criteria shared by ListContexts and ListActions
// (both real ops support exactly this shape: SortContextsBy/SortActionsBy
// are each Name|CreationTime — types/enums.go:9037-9044,9141-9148).
type nameOrTimeSortParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	TypeFilter    string
	SourceURI     string
	NextToken     string
	SortBy        string
	SortOrder     string
	MaxResults    int32
}

// filterSortPaginateByNameOrTime filters items by params.TypeFilter/SourceURI/
// creation-time window, sorts by Name (SortBy="Name") or CreationTime
// (default), applying SortOrder (default Descending), then paginates.
func filterSortPaginateByNameOrTime[T any](
	items []*T,
	params nameOrTimeSortParams,
	typeOf, sourceURIOf, nameOf func(*T) string,
	creationTimeOf func(*T) time.Time,
	clone func(*T) *T,
) ([]*T, string) {
	list := make([]*T, 0, len(items))

	for _, item := range items {
		if params.TypeFilter != "" && typeOf(item) != params.TypeFilter {
			continue
		}

		if params.SourceURI != "" && sourceURIOf(item) != params.SourceURI {
			continue
		}

		ct := creationTimeOf(item)
		if params.CreatedAfter != nil && !ct.After(*params.CreatedAfter) {
			continue
		}

		if params.CreatedBefore != nil && !ct.Before(*params.CreatedBefore) {
			continue
		}

		list = append(list, clone(item))
	}

	desc := !strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		var less bool
		if params.SortBy == keyGenericName {
			less = nameOf(list[i]) < nameOf(list[j])
		} else {
			less = creationTimeOf(list[i]).Before(creationTimeOf(list[j]))
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}
