package inspector2

import (
	"slices"
	"strings"
)

// cisFilter is one member of a Cis*FilterCriteria list (inspector2@v1.54.1 serializers.go
// CisStringFilter, CisNumberFilter, CisDateFilter, TagFilter and the enum filters).
type cisFilter struct {
	Lower      *float64 `json:"lowerInclusive"`
	Upper      *float64 `json:"upperInclusive"`
	Earliest   *float64 `json:"earliestScanStartTime"`
	Latest     *float64 `json:"latestScanStartTime"`
	Comparison string   `json:"comparison"`
	Value      string   `json:"value"`
}

// cisCriteria maps a filter-list member name to its filters: OR within a list, AND across lists.
type cisCriteria map[string][]cisFilter

type cisAccessors[T any] struct {
	strs map[string]func(T) string
	nums map[string]func(T) float64
}

func (f cisFilter) matchesString(got string) bool {
	switch f.Comparison {
	case "PREFIX":
		return strings.HasPrefix(got, f.Value)
	case "NOT_EQUALS":
		return got != f.Value
	default:
		return got == f.Value
	}
}

func (f cisFilter) matchesNumber(got float64) bool {
	lo, hi := f.Lower, f.Upper
	if lo == nil {
		lo = f.Earliest
	}

	if hi == nil {
		hi = f.Latest
	}

	return (lo == nil || got >= *lo) && (hi == nil || got <= *hi)
}

func filterCis[T any](items []T, c cisCriteria, acc cisAccessors[T]) []T {
	if len(c) == 0 {
		return items
	}

	out := make([]T, 0, len(items))

	for _, it := range items {
		if cisItemMatches(it, c, acc) {
			out = append(out, it)
		}
	}

	return out
}

func cisItemMatches[T any](it T, c cisCriteria, acc cisAccessors[T]) bool {
	for name, filters := range c {
		if len(filters) == 0 {
			continue
		}

		if get, ok := acc.strs[name]; ok {
			got := get(it)
			if !slices.ContainsFunc(filters, func(f cisFilter) bool { return f.matchesString(got) }) {
				return false
			}

			continue
		}

		if get, ok := acc.nums[name]; ok {
			got := get(it)
			if !slices.ContainsFunc(filters, func(f cisFilter) bool { return f.matchesNumber(got) }) {
				return false
			}
		}
	}

	return true
}

func mapNum(field string) func(map[string]any) float64 {
	return func(m map[string]any) float64 {
		switch v := m[field].(type) {
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case float64:
			return v
		}

		return 0
	}
}

func statusCount(status string) func(map[string]any) float64 {
	return func(m map[string]any) float64 {
		counts, _ := m["statusCounts"].(map[string]any)
		n, _ := counts[status].(int64)

		return float64(n)
	}
}

func scanTargetAccount(m map[string]any) string {
	targets, _ := m["targets"].(map[string]any)
	ids, _ := targets[keyAccountIDs].([]string)

	if len(ids) == 0 {
		return ""
	}

	return ids[0]
}

// Only members the backend has state for are applied; the rest are recorded in PARITY.md.
func cisScanConfigAccessors() cisAccessors[*CisScanConfiguration] {
	return cisAccessors[*CisScanConfiguration]{
		strs: map[string]func(*CisScanConfiguration) string{
			"scanConfigurationArnFilters": func(c *CisScanConfiguration) string { return c.Arn },
			"scanNameFilters":             func(c *CisScanConfiguration) string { return c.Name },
		},
	}
}

func cisScanAccessors() cisAccessors[map[string]any] {
	return cisAccessors[map[string]any]{
		strs: map[string]func(map[string]any) string{
			"scanArnFilters":              mapKey(keyScanArn),
			"scanConfigurationArnFilters": mapKey(keyScanConfigurationArn),
			"scanNameFilters":             mapKey("scanName"),
			"scanStatusFilters":           mapKey(keyStatus),
			"targetAccountIdFilters":      scanTargetAccount,
		},
		nums: map[string]func(map[string]any) float64{
			"failedChecksFilters": mapNum("failedChecks"),
			"scanAtFilters":       mapNum("scanDate"),
		},
	}
}

func cisCheckAccessors() cisAccessors[map[string]any] {
	return cisAccessors[map[string]any]{
		strs: map[string]func(map[string]any) string{
			"checkIdFilters":       mapKey(keyCheckID),
			"platformFilters":      mapKey(keyPlatform),
			"securityLevelFilters": mapKey(keyLevel),
		},
		nums: map[string]func(map[string]any) float64{"failedResourcesFilters": statusCount("failed")},
	}
}

func cisTargetAccessors() cisAccessors[map[string]any] {
	return cisAccessors[map[string]any]{
		strs: map[string]func(map[string]any) string{
			"accountIdFilters":        mapKey(keyAccountID),
			"platformFilters":         mapKey(keyPlatform),
			"targetResourceIdFilters": mapKey(keyTargetResourceID),
		},
		nums: map[string]func(map[string]any) float64{"failedChecksFilters": statusCount("failed")},
	}
}

func cisDetailAccessors() cisAccessors[map[string]any] {
	return cisAccessors[map[string]any]{
		strs: map[string]func(map[string]any) string{
			"checkIdFilters":       mapKey(keyCheckID),
			"findingStatusFilters": mapKey(keyStatus),
			"securityLevelFilters": mapKey(keyLevel),
		},
	}
}
