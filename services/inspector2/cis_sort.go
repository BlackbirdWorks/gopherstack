package inspector2

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

const (
	cisSortAsc  = "ASC"
	cisSortDesc = "DESC"

	cisTargetStatusCompleted = "COMPLETED"
)

// cisSorter orders one CIS list op by its SortBy enum; comparers is keyed by the enum values.
type cisSorter[T any] struct {
	comparers map[string]func(a, b T) int
	key       func(T) string
}

func validCisSortOrder(order string) bool {
	return order == "" || order == cisSortAsc || order == cisSortDesc
}

// sort validates sortBy/sortOrder and returns items ordered accordingly; the item key breaks ties.
func (s cisSorter[T]) sort(items []T, sortBy, sortOrder string) ([]T, error) {
	if !validCisSortOrder(sortOrder) {
		return nil, fmt.Errorf("%w: sortOrder must be ASC or DESC, got %q", ErrValidation, sortOrder)
	}

	byKey := func(a, b T) int { return strings.Compare(s.key(a), s.key(b)) }

	if sortBy == "" {
		out := slices.Clone(items)
		slices.SortStableFunc(out, byKey)

		return out, nil
	}

	compare, ok := s.comparers[sortBy]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported sortBy %q", ErrValidation, sortBy)
	}

	sign := 1
	if sortOrder == cisSortDesc {
		sign = -1
	}

	out := slices.Clone(items)
	slices.SortStableFunc(out, func(a, b T) int {
		return cmp.Or(sign*compare(a, b), byKey(a, b))
	})

	return out, nil
}

func mapStrCmp(field string) func(a, b map[string]any) int {
	get := mapKey(field)

	return func(a, b map[string]any) int { return strings.Compare(get(a), get(b)) }
}

func mapNumCmp(num func(map[string]any) float64) func(a, b map[string]any) int {
	return func(a, b map[string]any) int { return cmp.Compare(num(a), num(b)) }
}

func cisScanSorter() cisSorter[map[string]any] {
	return cisSorter[map[string]any]{
		key: mapKey(keyScanArn),
		comparers: map[string]func(a, b map[string]any) int{
			"STATUS":          mapStrCmp(keyStatus),
			"SCHEDULED_BY":    mapStrCmp("scheduledBy"),
			"SCAN_START_DATE": mapNumCmp(mapNum("scanDate")),
			"FAILED_CHECKS":   mapNumCmp(mapNum("failedChecks")),
		},
	}
}

func cisScanConfigSorter() cisSorter[*CisScanConfiguration] {
	return cisSorter[*CisScanConfiguration]{
		key: func(c *CisScanConfiguration) string { return c.Arn },
		comparers: map[string]func(a, b *CisScanConfiguration) int{
			"SCAN_NAME":              func(a, b *CisScanConfiguration) int { return strings.Compare(a.Name, b.Name) },
			"SCAN_CONFIGURATION_ARN": func(a, b *CisScanConfiguration) int { return strings.Compare(a.Arn, b.Arn) },
		},
	}
}

func cisDetailSorter() cisSorter[map[string]any] {
	return cisSorter[map[string]any]{
		key: cisDetailKey,
		comparers: map[string]func(a, b map[string]any) int{
			"CHECK_ID": mapStrCmp(keyCheckID),
			"STATUS":   mapStrCmp(keyStatus),
		},
	}
}

func cisCheckSorter() cisSorter[map[string]any] {
	return cisSorter[map[string]any]{
		key: mapKey(keyCheckID),
		comparers: map[string]func(a, b map[string]any) int{
			"CHECK_ID":       mapStrCmp(keyCheckID),
			"TITLE":          mapStrCmp("checkDescription"),
			"PLATFORM":       mapStrCmp(keyPlatform),
			"FAILED_COUNTS":  mapNumCmp(failedCount),
			"SECURITY_LEVEL": mapStrCmp(keyLevel),
		},
	}
}

func cisTargetSorter() cisSorter[map[string]any] {
	return cisSorter[map[string]any]{
		key: mapKey(keyTargetResourceID),
		comparers: map[string]func(a, b map[string]any) int{
			"RESOURCE_ID":          mapStrCmp(keyTargetResourceID),
			"FAILED_COUNTS":        mapNumCmp(failedCount),
			"ACCOUNT_ID":           mapStrCmp(keyAccountID),
			"PLATFORM":             mapStrCmp(keyPlatform),
			"TARGET_STATUS":        mapStrCmp("targetStatus"),
			"TARGET_STATUS_REASON": mapStrCmp("targetStatusReason"),
		},
	}
}

// cisDetailLevelValid reports whether level is a ListCisScansDetailLevel value (or unset).
func cisDetailLevelValid(level string) bool {
	return level == "" || level == "ORGANIZATION" || level == "MEMBER"
}
