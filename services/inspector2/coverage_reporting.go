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
// facet backed by CoveredResource.resourceMetadata is applied through
// coverageMetadataFilters; only the multi-cloud cloud* facets have no data.
type coverageStringFilters struct {
	accountID        []stringFilter
	resourceID       []stringFilter
	resourceType     []stringFilter
	scanType         []stringFilter
	scanStatusCode   []stringFilter
	scanStatusReason []stringFilter
	scanMode         []stringFilter
	lastScannedAt    []dateRangeFilter
	metadata         coverageMetadataFilters
}

// coverageMetadataFilters are the CoverageFilterCriteria facets backed by CoveredResource.resourceMetadata.
type coverageMetadataFilters struct {
	ec2Tags       []mapFilter
	lambdaTags    []mapFilter
	ecrImageTags  []stringFilter
	ecrRepoName   []stringFilter
	lambdaName    []stringFilter
	lambdaRuntime []stringFilter
	projectName   []stringFilter
	providerType  []stringFilter
	visibility    []stringFilter
	lastCommitID  []stringFilter
	inUseCount    []numberRange
	lastInUseAt   []dateRangeFilter
	imagePulledAt []dateRangeFilter
}

// mapFilter mirrors one CoverageMapFilter: the tag key must exist and, when value is set, equal it.
type mapFilter struct {
	key      string
	value    string
	hasValue bool
}

// numberRange mirrors one CoverageNumberFilter (lowerInclusive/upperInclusive, either may be absent).
type numberRange struct {
	lower, upper       int64
	hasLower, hasUpper bool
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
		metadata: coverageMetadataFilters{
			ec2Tags:       extractMapFilters(criteria, "ec2InstanceTags"),
			lambdaTags:    extractMapFilters(criteria, "lambdaFunctionTags"),
			ecrImageTags:  extractStringFilters(criteria, "ecrImageTags"),
			ecrRepoName:   extractStringFilters(criteria, "ecrRepositoryName"),
			lambdaName:    extractStringFilters(criteria, "lambdaFunctionName"),
			lambdaRuntime: extractStringFilters(criteria, "lambdaFunctionRuntime"),
			projectName:   extractStringFilters(criteria, "codeRepositoryProjectName"),
			providerType:  extractStringFilters(criteria, "codeRepositoryProviderType"),
			visibility:    extractStringFilters(criteria, "codeRepositoryProviderTypeVisibility"),
			lastCommitID:  extractStringFilters(criteria, "lastScannedCommitId"),
			inUseCount:    extractNumberRanges(criteria, "ecrImageInUseCount"),
			lastInUseAt:   extractDateFilters(criteria, "ecrImageLastInUseAt"),
			imagePulledAt: extractDateFilters(criteria, "imagePulledAt"),
		},
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
		matchDateFilters(f.lastScannedAt, e.LastScannedAt) &&
		f.metadata.matches(e.ResourceMetadata)
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

	// matched is sorted by coverageEntryKeyFn ("<resourceId>/<scanType>", a
	// composite unique key), the same field the cursor carries, so this is a
	// threshold search: resume at the first entry whose key is strictly
	// greater than nextToken. An unresolvable token then resumes past
	// everything already served instead of restarting at page one.
	start := 0

	if nextToken != "" {
		start = len(matched)

		for i, e := range matched {
			if coverageEntryKeyFn(e) > nextToken {
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
		if e.ResourceMetadata != nil && e.ResourceMetadata.EcrRepository != nil {
			return e.ResourceMetadata.EcrRepository.Name
		}

		return ""
	default:
		return ""
	}
}

func extractMapFilters(criteria map[string]any, key string) []mapFilter {
	raw, _ := criteria[key].([]any)
	out := make([]mapFilter, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		k, _ := m["key"].(string)
		v, hasValue := m["value"].(string)
		out = append(out, mapFilter{key: k, value: v, hasValue: hasValue})
	}

	return out
}

func extractNumberRanges(criteria map[string]any, key string) []numberRange {
	raw, _ := criteria[key].([]any)
	out := make([]numberRange, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		var r numberRange

		if lo, has := m["lowerInclusive"].(float64); has {
			r.lower, r.hasLower = int64(lo), true
		}

		if hi, has := m["upperInclusive"].(float64); has {
			r.upper, r.hasUpper = int64(hi), true
		}

		out = append(out, r)
	}

	return out
}

func matchMapFilters(filters []mapFilter, tags map[string]string) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		v, ok := tags[f.key]
		if ok && (!f.hasValue || v == f.value) {
			return true
		}
	}

	return false
}

func matchNumberRanges(filters []numberRange, actual int64) bool {
	if len(filters) == 0 {
		return true
	}

	for _, f := range filters {
		if (!f.hasLower || actual >= f.lower) && (!f.hasUpper || actual <= f.upper) {
			return true
		}
	}

	return false
}

func matchAnyTag(filters []stringFilter, tags []string) bool {
	if len(filters) == 0 {
		return true
	}

	for _, t := range tags {
		if matchStringFilters(filters, t) {
			return true
		}
	}

	return false
}

// matches applies every metadata facet; a facet with no filter always passes, a set one needs matching metadata.
func (f coverageMetadataFilters) matches(md *CoverageResourceMetadata) bool {
	if md == nil {
		md = &CoverageResourceMetadata{}
	}

	ec2 := orZero(md.Ec2)
	img := orZero(md.EcrImage)
	repo := orZero(md.EcrRepository)
	fn := orZero(md.LambdaFunction)
	code := orZero(md.CodeRepository)

	return matchMapFilters(f.ec2Tags, ec2.Tags) &&
		matchMapFilters(f.lambdaTags, fn.FunctionTags) &&
		matchAnyTag(f.ecrImageTags, img.Tags) &&
		matchStringFilters(f.ecrRepoName, repo.Name) &&
		matchStringFilters(f.lambdaName, fn.FunctionName) &&
		matchStringFilters(f.lambdaRuntime, fn.Runtime) &&
		matchStringFilters(f.projectName, code.ProjectName) &&
		matchStringFilters(f.providerType, code.ProviderType) &&
		matchStringFilters(f.visibility, code.ProviderTypeVisibility) &&
		matchStringFilters(f.lastCommitID, code.LastScannedCommitID) &&
		matchNumberRanges(f.inUseCount, img.InUseCount) &&
		matchDateFilters(f.lastInUseAt, img.LastInUseAt) &&
		matchDateFilters(f.imagePulledAt, img.ImagePulledAt)
}

func orZero[T any](p *T) T {
	if p == nil {
		var zero T

		return zero
	}

	return *p
}
