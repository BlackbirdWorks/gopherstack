package inspector2

import (
	"sort"
	"time"
)

// groupKeyScanStatusCode, etc. are the real GroupKey enum values accepted by
// ListCoverageStatistics's groupBy request field.
const (
	groupKeyScanStatusCode    = "SCAN_STATUS_CODE"
	groupKeyAccountID         = "ACCOUNT_ID"
	groupKeyResourceType      = "RESOURCE_TYPE"
	groupKeyEcrRepositoryName = "ECR_REPOSITORY_NAME"

	defaultCoveragePageSize = 50
)

// SeedCoverage injects a coverage entry into the backend so ListCoverage/
// ListCoverageStatistics return realistic data, mirroring the SeedFinding
// precedent: real AWS populates coverage automatically as resources are
// scanned, which gopherstack has no scanning engine to emulate, so this is
// the additive capability that lets tests/fixtures/the dashboard populate it
// directly instead of ListCoverage being permanently hardwired empty.
func (b *InMemoryBackend) SeedCoverage(e CoverageEntry) (*CoverageEntry, error) {
	b.mu.Lock("SeedCoverage")
	defer b.mu.Unlock()

	if e.ResourceID == "" || e.ResourceType == "" || e.ScanType == "" {
		return nil, ErrValidation
	}

	stored := e
	if stored.AccountID == "" {
		stored.AccountID = b.accountID
	}

	if stored.LastScannedAt.IsZero() {
		stored.LastScannedAt = time.Now().UTC()
	}

	if stored.ScanStatus == nil {
		stored.ScanStatus = &CoverageScanStatus{StatusCode: "ACTIVE"}
	}

	clone := stored
	b.coverageEntries.Put(&clone)

	out := stored

	return &out, nil
}

// coverageStringFilters extracts the subset of CoverageFilterCriteria's
// filters that ListCoverage/ListCoverageStatistics can genuinely evaluate
// against real, stored CoverageEntry data: the string-comparison facets
// (accountId/resourceId/resourceType/scanType/scanStatusCode/
// scanStatusReason/scanMode) and the lastScannedAt date-range facet, which
// CoverageEntry.LastScannedAt/ScanStatus/ScanMode genuinely track. Every
// other real filter facet (cloudContainerImageTags, ec2InstanceTags,
// ecrImageTags, lambdaFunctionTags, ecrImageInUseCount, ecrImageLastInUseAt,
// imagePulledAt, and the rest of the cloud*/code*/lambda* facets) is real on
// the wire but tied to CoveredResource.resourceMetadata, a nested per-
// resource-type union this backend does not model at all (see PARITY.md
// gaps) -- accepting those in the request shape without narrowing on them is
// an honest limitation (no data to filter against), not a fixable bug,
// unlike the facets below, which previously had real backing data but were
// silently never wired to the filter.
type coverageStringFilters struct {
	accountID        []stringFilter
	resourceID       []stringFilter
	resourceType     []stringFilter
	scanType         []stringFilter
	scanStatusCode   []stringFilter
	scanStatusReason []stringFilter
	scanMode         []stringFilter
	lastScannedAt    []dateRangeFilter
}

// dateRangeFilter mirrors one CoverageDateFilter entry: startInclusive/
// endInclusive bounds (either may be absent), matching real AWS's
// startInclusive <= value <= endInclusive semantics.
type dateRangeFilter struct {
	start    time.Time
	end      time.Time
	hasStart bool
	hasEnd   bool
}

// extractDateFilters decodes CoverageFilterCriteria's date-range filter
// shape ({"startInclusive": <epoch-seconds>, "endInclusive": <epoch-seconds>})
// for the given key. Real AWS encodes these as epoch-seconds numbers on the
// wire (confirmed via serializers.go's
// awsRestjson1_serializeDocumentCoverageDateFilter), not RFC3339 strings.
func extractDateFilters(criteria map[string]any, key string) []dateRangeFilter {
	raw, ok := criteria[key].([]any)
	if !ok {
		return nil
	}

	filters := make([]dateRangeFilter, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		var f dateRangeFilter

		if start, hasStart := m["startInclusive"].(float64); hasStart {
			f.start = time.Unix(int64(start), 0).UTC()
			f.hasStart = true
		}

		if end, hasEnd := m["endInclusive"].(float64); hasEnd {
			f.end = time.Unix(int64(end), 0).UTC()
			f.hasEnd = true
		}

		if f.hasStart || f.hasEnd {
			filters = append(filters, f)
		}
	}

	return filters
}

// matchDateFilters reports whether actual falls within any of filters'
// [start, end] inclusive ranges (multiple filters on the same field are a
// logical OR, matching matchStringFilters' semantics).
func matchDateFilters(filters []dateRangeFilter, actual time.Time) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if f.hasStart && actual.Before(f.start) {
			continue
		}

		if f.hasEnd && actual.After(f.end) {
			continue
		}

		return true
	}

	return false
}

func parseCoverageFilterCriteria(criteria map[string]any) coverageStringFilters {
	return coverageStringFilters{
		accountID:        extractStringFilters(criteria, "accountId"),
		resourceID:       extractStringFilters(criteria, "resourceId"),
		resourceType:     extractStringFilters(criteria, "resourceType"),
		scanType:         extractStringFilters(criteria, "scanType"),
		scanStatusCode:   extractStringFilters(criteria, "scanStatusCode"),
		scanStatusReason: extractStringFilters(criteria, "scanStatusReason"),
		scanMode:         extractStringFilters(criteria, "scanMode"),
		lastScannedAt:    extractDateFilters(criteria, "lastScannedAt"),
	}
}

func (f coverageStringFilters) matches(e *CoverageEntry) bool {
	statusCode, statusReason := "", ""
	if e.ScanStatus != nil {
		statusCode = e.ScanStatus.StatusCode
		statusReason = e.ScanStatus.Reason
	}

	return matchStringFilters(f.accountID, e.AccountID) &&
		matchStringFilters(f.resourceID, e.ResourceID) &&
		matchStringFilters(f.resourceType, e.ResourceType) &&
		matchStringFilters(f.scanType, e.ScanType) &&
		matchStringFilters(f.scanStatusCode, statusCode) &&
		matchStringFilters(f.scanStatusReason, statusReason) &&
		matchStringFilters(f.scanMode, e.ScanMode) &&
		matchDateFilters(f.lastScannedAt, e.LastScannedAt)
}

// ListCoverage returns a page of seeded coverage entries filtered by the
// supplied filterCriteria (accountId/resourceId/resourceType/scanType).
// Pagination uses the composite resourceId/scanType key as a stable cursor,
// mirroring ListFindings.
func (b *InMemoryBackend) ListCoverage(
	criteria map[string]any, maxResults int32, nextToken string,
) ([]*CoverageEntry, string, error) {
	b.mu.RLock("ListCoverage")
	defer b.mu.RUnlock()

	matched := b.filteredCoverage(criteria)

	pageSize := int(maxResults)
	if pageSize <= 0 {
		pageSize = defaultCoveragePageSize
	}

	start := 0

	if nextToken != "" {
		for i, e := range matched {
			if coverageEntryKeyFn(e) == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+pageSize, len(matched))
	page := matched[start:end]

	next := ""
	if end < len(matched) {
		next = coverageEntryKeyFn(matched[end])
	}

	return page, next, nil
}

// filteredCoverage returns every stored coverage entry matching criteria,
// sorted by its composite key for stable pagination. Callers must hold
// b.mu (either lock).
func (b *InMemoryBackend) filteredCoverage(criteria map[string]any) []*CoverageEntry {
	fc := parseCoverageFilterCriteria(criteria)

	matched := make([]*CoverageEntry, 0, b.coverageEntries.Len())

	b.coverageEntries.Range(func(e *CoverageEntry) bool {
		if fc.matches(e) {
			clone := *e
			matched = append(matched, &clone)
		}

		return true
	})

	sort.Slice(matched, func(i, j int) bool {
		return coverageEntryKeyFn(matched[i]) < coverageEntryKeyFn(matched[j])
	})

	return matched
}

// ListCoverageStatistics returns real aggregate counts over seeded coverage
// entries. When groupBy is empty (as real AWS allows), it returns only the
// overall totalCounts with no per-group breakdown; otherwise countsByGroup
// buckets by the requested GroupKey.
func (b *InMemoryBackend) ListCoverageStatistics(criteria map[string]any, groupBy string) (map[string]any, error) {
	b.mu.RLock("ListCoverageStatistics")
	defer b.mu.RUnlock()

	matched := b.filteredCoverage(criteria)

	resp := map[string]any{"totalCounts": int64(len(matched))}

	if groupBy == "" {
		resp["countsByGroup"] = []any{}

		return resp, nil
	}

	counts := make(map[string]int64)

	for _, e := range matched {
		counts[coverageGroupKey(e, groupBy)]++
	}

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	groups := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		groups = append(groups, map[string]any{"groupKey": k, "count": counts[k]})
	}

	resp["countsByGroup"] = groups

	return resp, nil
}

// coverageGroupKey returns the bucket a coverage entry falls into for the
// given real GroupKey value.
func coverageGroupKey(e *CoverageEntry, groupBy string) string {
	switch groupBy {
	case groupKeyAccountID:
		return e.AccountID
	case groupKeyResourceType:
		return e.ResourceType
	case groupKeyScanStatusCode:
		if e.ScanStatus != nil {
			return e.ScanStatus.StatusCode
		}

		return ""
	case groupKeyEcrRepositoryName:
		// Not modeled (would require ResourceMetadata.EcrRepository), matching
		// the deliberate omission of the nested ResourceMetadata union noted
		// on CoverageEntry.
		return ""
	default:
		return ""
	}
}
