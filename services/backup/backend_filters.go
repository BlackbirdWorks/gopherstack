package backup

import "time"

const (
	keySummaryCount  = "Count"
	keySummaryRegion = "Region"
)

const (
	defaultMaxResults = 1000
	maxAllowedResults = 1000
)

// ---- New types for batch-1 ops ----

// inTimeRange returns false if t is outside the [after, before) window.
// Either bound may be nil (meaning "no bound").
func inTimeRange(t time.Time, after, before *time.Time) bool {
	if after != nil && !t.After(*after) {
		return false
	}
	if before != nil && !t.Before(*before) {
		return false
	}

	return true
}

// paginateByID applies cursor-based pagination to a pre-sorted slice.
// keyFn extracts the string key for each item (used as the pagination cursor).
// Returns (page, nextToken). nextToken is "" when no more pages remain.
func paginateByID[T any](list []T, keyFn func(T) string, maxResults int, nextToken string) ([]T, string) {
	if maxResults <= 0 || maxResults > maxAllowedResults {
		maxResults = defaultMaxResults
	}

	// Advance to the cursor item.
	start := 0
	if nextToken != "" {
		found := false
		for i, item := range list {
			if keyFn(item) == nextToken {
				start = i
				found = true

				break
			}
		}
		if !found {
			return []T{}, ""
		}
	}

	list = list[start:]
	if len(list) <= maxResults {
		return list, ""
	}

	// NextToken is the key of the first item of the next page.
	return list[:maxResults], keyFn(list[maxResults])
}

// ParseTimeFilter parses an RFC3339 timestamp string into a *time.Time.
// Returns nil if the string is empty or invalid.
func ParseTimeFilter(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}

	return &t
}
