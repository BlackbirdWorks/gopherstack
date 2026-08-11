package route53resolver

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
)

// wireFilter is the JSON shape of types.Filter (aws-sdk-go-v2/service/
// route53resolver@v1.48.4 types/types.go): Name/Values, shared by
// ListResolverEndpoints, ListResolverRules, and ListResolverQueryLogConfigs.
type wireFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// Canonical filter field names shared by more than one operation's alias
// map, plus their legacy uppercase-with-underscore forms (see
// resolverEndpointFilterAliases's doc comment) -- pulled out as constants so
// the same literal isn't repeated across the three per-operation files.
const (
	filterFieldCreatorRequestID  = "CreatorRequestId"
	legacyFilterCreatorRequestID = "CREATOR_REQUEST_ID"
	filterFieldName              = "Name"
	legacyFilterName             = "NAME"
	filterFieldStatus            = "Status"
	legacyFilterStatus           = "STATUS"
)

// applyFilters keeps only items matching every filter (AND across filters);
// a filter matches if the item's value is among its Values (OR within one
// filter's Values) -- the standard AWS list-filter convention, not spelled
// out per-operation in the route53resolver model but consistent with the
// shared Filter/FilterValues shapes reused across every AWS list-filter API.
//
// An unrecognized Filter.Name is rejected with InvalidParameterException:
// InvalidParameterException is modelled as an error on all three ops
// (service-2.json.gz operations.ListResolver{Endpoints,Rules,QueryLogConfigs}.errors,
// botocore route53resolver 2018-04-01), its shape carries a FieldName member
// meant for exactly this case, and the Filter.Name doc closes over an exact
// enumerated set per operation -- see each caller's alias map.
//
// An empty Values list has no stated meaning in the model; matching nothing
// is the conservative reading here, chosen so an empty filter cannot
// silently degrade into "no filter" and return more than was asked for.
func applyFilters[T any](
	items []T,
	filters []wireFilter,
	aliases map[string]string,
	match func(item T, canonicalName string, values []string) bool,
) ([]T, error) {
	if len(filters) == 0 {
		return items, nil
	}

	type resolved struct {
		name   string
		values []string
	}

	resolvedFilters := make([]resolved, len(filters))
	for i, f := range filters {
		canon, ok := aliases[f.Name]
		if !ok {
			return nil, fmt.Errorf("%w: unrecognized filter name %q", ErrInvalidParameter, f.Name)
		}
		resolvedFilters[i] = resolved{name: canon, values: f.Values}
	}

	out := make([]T, 0, len(items))
	for _, item := range items {
		matched := true
		for _, f := range resolvedFilters {
			if len(f.values) == 0 || !match(item, f.name, f.values) {
				matched = false

				break
			}
		}
		if matched {
			out = append(out, item)
		}
	}

	return out, nil
}

const (
	sortOrderAscending  = "ASCENDING"
	sortOrderDescending = "DESCENDING"
)

// sortLess is an ascending-comparator table keyed by SortByKey value.
type sortLess[T any] map[string]func(a, b T) bool

// applySort stable-sorts items by sortBy/sortOrder -- the ListResolverQueryLogConfigs
// and ListResolverQueryLogConfigAssociations request members (service-2.json.gz
// operations.ListResolverQueryLogConfig{s,Associations}.input.SortBy/SortOrder,
// botocore route53resolver 2018-04-01); no other operation in this service models
// either field. Must run before paginate(): pkgs/page.New documents itself as
// pagination over "a fully sorted slice," so sorting after paginate would order
// each page independently instead of the whole result set, breaking global order
// across NextToken pages.
//
// SortByKey is a free-form string in the model (shape SortByKey: max 64/min 1, no
// enum) -- the valid names are enumerated only in each operation's SortBy doc
// comment, not in the shape itself, and differ between the two operations (see the
// per-operation sortLess tables). An unrecognized value is rejected with
// InvalidParameterException, the same treatment applyFilters gives an unrecognized
// Filter.Name: that error is modelled on both ops and its shape carries a
// FieldName member for exactly this case.
//
// SortOrder has no documented default (unlike SortBy's per-op enumeration, its doc
// only says "ASCENDING or DESCENDING" with no stated fallback). Empty/ASCENDING
// sorts ascending -- the conservative, least-surprising reading, not a verified
// fact -- and DESCENDING reverses it; any other value is rejected the same way as
// an unrecognized SortBy.
func applySort[T any](items []T, sortBy, sortOrder string, less sortLess[T]) ([]T, error) {
	if sortBy == "" {
		return items, nil
	}

	cmp, ok := less[sortBy]
	if !ok {
		return nil, fmt.Errorf("%w: unrecognized SortBy value %q", ErrInvalidParameter, sortBy)
	}

	switch sortOrder {
	case "", sortOrderAscending:
	case sortOrderDescending:
		asc := cmp
		cmp = func(a, b T) bool { return asc(b, a) }
	default:
		return nil, fmt.Errorf("%w: unrecognized SortOrder value %q", ErrInvalidParameter, sortOrder)
	}

	out := slices.Clone(items)
	sort.SliceStable(out, func(i, j int) bool { return cmp(out[i], out[j]) })

	return out, nil
}

func int32ToString(n int32) string {
	return strconv.FormatInt(int64(n), 10)
}

// containsAny reports whether haystack and needles share any element -- the
// list-vs-list membership test SecurityGroupIds needs (Values matches if any
// of the item's list values is among the filter's Values).
func containsAny(haystack, needles []string) bool {
	for _, n := range needles {
		if slices.Contains(haystack, n) {
			return true
		}
	}

	return false
}
