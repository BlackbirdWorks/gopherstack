package securityhub

import (
	"fmt"
	"maps"
	"strings"
)

func findingKey(productArn, id string) string {
	return productArn + "|" + id
}

// validateASFFRequiredFields checks that a finding has the mandatory ASFF fields.
// AWS rejects findings missing any of these fields in BatchImportFindings.
// Returns a non-empty error message if validation fails.
func validateASFFRequiredFields(f map[string]any, productArn, id string) string {
	if productArn == "" {
		return "ProductArn is required"
	}

	if id == "" {
		return "Id is required"
	}

	if v, _ := f["AwsAccountId"].(string); v == "" {
		return "AwsAccountId is required"
	}

	if v, _ := f["GeneratorId"].(string); v == "" {
		return "GeneratorId is required"
	}

	if v, _ := f["Title"].(string); v == "" {
		return "Title is required"
	}

	if v, _ := f["Description"].(string); v == "" {
		return "Description is required"
	}

	// At least one of CreatedAt/UpdatedAt/FirstObservedAt/LastObservedAt must be present.
	hasTimestamp := false
	for _, k := range []string{keyCreatedAt, keyUpdatedAt, "FirstObservedAt", "LastObservedAt"} {
		if v, _ := f[k].(string); v != "" {
			hasTimestamp = true

			break
		}
	}

	if !hasTimestamp {
		return "At least one of CreatedAt, UpdatedAt, FirstObservedAt, or LastObservedAt is required"
	}

	// Resources must be a non-empty array.
	resources, _ := f["Resources"].([]any)
	if len(resources) == 0 {
		return "Resources must contain at least one entry"
	}

	return ""
}

func (b *InMemoryBackend) ImportFindings(findings []map[string]any) (int, int, []map[string]any) {
	b.mu.Lock("ImportFindings")
	defer b.mu.Unlock()

	successCount := 0
	failedCount := 0
	var failedFindings []map[string]any

	for _, f := range findings {
		productArn, _ := f["ProductArn"].(string)
		id, _ := f["Id"].(string)

		if msg := validateASFFRequiredFields(f, productArn, id); msg != "" {
			failedCount++
			failedFindings = append(failedFindings, map[string]any{
				"Id":            id,
				"ProductArn":    productArn, //nolint:goconst // existing issue.
				keyErrorCode:    "InvalidInputException",
				keyErrorMessage: msg,
			})

			continue
		}

		key := findingKey(productArn, id)

		// Copy the finding
		stored := make(map[string]any, len(f))
		maps.Copy(stored, f)

		b.findings[key] = stored
		successCount++
	}

	return successCount, failedCount, failedFindings
}

func (b *InMemoryBackend) GetFindings(
	filters map[string]any,
	_ []map[string]any,
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	b.mu.RLock("GetFindings")
	defer b.mu.RUnlock()

	var results []map[string]any

	for _, f := range b.findings {
		if matchesFindingFilters(f, filters) {
			results = append(results, f)
		}
	}

	if maxResults <= 0 || maxResults > 100 {
		maxResults = 100
	}

	start := decodeToken(nextToken)
	if start >= len(results) {
		return []map[string]any{}, ""
	}

	end := start + maxResults
	end = min(end, len(results))

	page := results[start:end]
	nextOut := ""

	if end < len(results) {
		nextOut = encodeToken(end)
	}

	return page, nextOut
}

// matchesFindingFilters is a simplified filter check.
// AWS SecurityHub finding filters are complex; we support a basic subset.
func matchesFindingFilters(finding, filters map[string]any) bool {
	if len(filters) == 0 {
		return true
	}

	fID, _ := finding["Id"].(string)
	if !matchesStringFilter(fID, filters["Id"]) {
		return false
	}

	fArn, _ := finding["ProductArn"].(string)
	if !matchesStringFilter(fArn, filters["ProductArn"]) {
		return false
	}

	// Additional string field filters
	for _, fieldKey := range []string{
		"AwsAccountId", "GeneratorId", "Title", "Description", //nolint:goconst // keyDescription lives in handler.go
		"RecordState", "WorkflowStatus", "SeverityLabel", "ComplianceStatus",
		"Type", "ResourceType", "ResourceId",
	} {
		fVal, _ := finding[fieldKey].(string)
		if !matchesStringFilter(fVal, filters[fieldKey]) {
			return false
		}
	}

	return true
}

// compareStringFilter evaluates one SecurityHub StringFilter comparison,
// reporting whether fieldVal matches val under comp. Unrecognized/empty comp
// defaults to EQUALS, matching the real API.
func compareStringFilter(comp, fieldVal, val string) bool {
	switch comp {
	case "NOT_EQUALS":
		return fieldVal != val
	case "PREFIX":
		return strings.HasPrefix(fieldVal, val)
	case "PREFIX_NOT_EQUALS":
		return !strings.HasPrefix(fieldVal, val)
	case "CONTAINS":
		return strings.Contains(fieldVal, val)
	case "NOT_CONTAINS":
		return !strings.Contains(fieldVal, val)
	default: // EQUALS
		return fieldVal == val
	}
}

// matchesStringFilter checks a single string field value against a SecurityHub filter value.
func matchesStringFilter(fieldVal string, filterVal any) bool {
	items, ok := filterVal.([]any)
	if !ok {
		return true
	}

	for _, item := range items {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		val, _ := m["Value"].(string)
		comp, _ := m["Comparison"].(string)

		if !compareStringFilter(comp, fieldVal, val) {
			return false
		}
	}

	return true
}

func (b *InMemoryBackend) UpdateFindings(filters map[string]any, note map[string]any, recordState string) error {
	b.mu.Lock("UpdateFindings")
	defer b.mu.Unlock()

	if !b.hubEnabled {
		return ErrHubNotEnabled
	}

	for key, f := range b.findings {
		if matchesFindingFilters(f, filters) {
			if note != nil {
				f["Note"] = note
			}

			if recordState != "" {
				f["RecordState"] = recordState
			}

			b.findings[key] = f
		}
	}

	return nil
}

func (b *InMemoryBackend) BatchUpdateFindings(
	findingIdentifiers []map[string]any,
	updates map[string]any,
) ([]map[string]any, []map[string]any) {
	b.mu.Lock("BatchUpdateFindings")
	defer b.mu.Unlock()

	var processedFindings []map[string]any
	var unprocessedFindings []map[string]any

	for _, ident := range findingIdentifiers {
		productArn, _ := ident["ProductArn"].(string)
		id, _ := ident["Id"].(string)
		key := findingKey(productArn, id)

		f, exists := b.findings[key]
		if !exists {
			unprocessedFindings = append(unprocessedFindings, map[string]any{
				"FindingIdentifier": ident,
				keyErrorCode:        errCodeInvalidInput,
				keyErrorMessage:     "Finding not found",
			})

			continue
		}

		// Apply updates
		maps.Copy(f, updates)

		b.findings[key] = f
		processedFindings = append(processedFindings, ident)
	}

	if processedFindings == nil {
		processedFindings = []map[string]any{}
	}

	if unprocessedFindings == nil {
		unprocessedFindings = []map[string]any{}
	}

	return processedFindings, unprocessedFindings
}

func (b *InMemoryBackend) GetFindingHistory(
	_ map[string]any,
	_, _ string,
	_ string,
	_ int,
) ([]map[string]any, string) {
	b.mu.RLock("GetFindingHistory")
	defer b.mu.RUnlock()

	// Return empty history since we don't track history
	return []map[string]any{}, ""
}

func (b *InMemoryBackend) GetFindingsV2(
	filters map[string]any,
	sortCriteria []map[string]any,
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	// Delegate to existing GetFindings – V2 uses same store
	return b.GetFindings(filters, sortCriteria, nextToken, maxResults)
}

func (b *InMemoryBackend) BatchUpdateFindingsV2(
	findingIdentifiers []map[string]any,
	updates map[string]any,
) ([]map[string]any, []map[string]any) {
	return b.BatchUpdateFindings(findingIdentifiers, updates)
}

func (b *InMemoryBackend) GetFindingStatisticsV2(groupByAttributes []string) []map[string]any {
	b.mu.RLock("GetFindingStatisticsV2")
	defer b.mu.RUnlock()

	type key struct{ attr, val string }
	counts := make(map[key]int)

	for _, finding := range b.findings {
		for _, attr := range groupByAttributes {
			val := ""
			if v, ok := finding[attr]; ok {
				val = fmt.Sprintf("%v", v)
			}

			counts[key{attr, val}]++
		}
	}

	var result []map[string]any

	seen := make(map[string]map[string]any)

	for k, count := range counts {
		if existing, ok := seen[k.attr]; ok {
			existing["Count"] = existing["Count"].(int) + count //nolint:errcheck // existing issue.
		} else {
			entry := map[string]any{
				"GroupByAttribute": k.attr, //nolint:goconst // existing issue.
				"GroupByValue":     k.val,
				keyCount:           count,
			}
			seen[k.attr] = entry
			result = append(result, entry)
		}
	}

	return result
}

func (b *InMemoryBackend) GetFindingsTrendsV2(
	groupByAttribute string,
	startTime, endTime string,
) []map[string]any {
	return []map[string]any{
		{
			"GroupByAttribute": groupByAttribute,
			"DateRanges": []map[string]any{
				{
					"DateRange": map[string]any{
						"StartDate": startTime,
						"EndDate":   endTime,
					},
					"Count": len(b.findings),
				},
			},
		},
	}
}
