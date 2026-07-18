package inspector2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// Inspector2 finding severities and statuses (AWS Inspector2 API).
const (
	severityInformational = "INFORMATIONAL"
	severityLow           = "LOW"
	severityMedium        = "MEDIUM"
	severityHigh          = "HIGH"
	severityCritical      = "CRITICAL"
	severityUntriaged     = "UNTRIAGED"

	findingStatusActive     = "ACTIVE"
	findingStatusSuppressed = "SUPPRESSED"
	findingStatusClosed     = "CLOSED"

	defaultFindingsPageSize = 50

	severityScoreCritical = 9.0
	severityScoreHigh     = 7.0
	severityScoreMedium   = 5.0
	severityScoreLow      = 3.0
)

// isValidFindingSeverity reports whether s is a recognized Inspector2 severity.
func isValidFindingSeverity(s string) bool {
	switch s {
	case severityInformational, severityLow, severityMedium,
		severityHigh, severityCritical, severityUntriaged:
		return true
	default:
		return false
	}
}

// isValidFindingStatus reports whether s is a recognized Inspector2 status.
func isValidFindingStatus(s string) bool {
	switch s {
	case findingStatusActive, findingStatusSuppressed, findingStatusClosed:
		return true
	default:
		return false
	}
}

// SeedFinding injects a finding into the backend so ListFindings/aggregations
// return realistic data. Unset fields are defaulted to AWS-plausible values. It
// returns the stored finding (with a generated ARN when none was supplied).
//
// This is the additive capability that lets gopherstack exceed LocalStack, whose
// Inspector2 ListFindings is hardwired to return an empty set.
func (b *InMemoryBackend) SeedFinding(f Finding) (*Finding, error) {
	b.mu.Lock("SeedFinding")
	defer b.mu.Unlock()

	stored := f
	if stored.Severity.Label == "" {
		stored.Severity = FindingSeverity{Label: severityMedium, Score: severityScore(severityMedium)}
	}

	if !isValidFindingSeverity(stored.Severity.Label) {
		return nil, fmt.Errorf("%w: invalid finding severity %q", ErrValidation, stored.Severity.Label)
	}

	if stored.Status == "" {
		stored.Status = findingStatusActive
	}

	if !isValidFindingStatus(stored.Status) {
		return nil, fmt.Errorf("%w: invalid finding status %q", ErrValidation, stored.Status)
	}

	if stored.AccountID == "" {
		stored.AccountID = b.accountID
	}

	if stored.Type == "" {
		stored.Type = "PACKAGE_VULNERABILITY"
	}

	now := time.Now().UTC()
	if stored.FirstObservedAt.IsZero() {
		stored.FirstObservedAt = now
	}

	if stored.LastObservedAt.IsZero() {
		stored.LastObservedAt = now
	}

	stored.UpdatedAt = now

	if stored.FindingArn == "" {
		stored.FindingArn = arn.Build(inspector2Service, b.region, stored.AccountID, "finding/"+uuid.NewString())
	}

	clone := stored
	b.findings.Put(&storedFinding{Finding: clone})

	out := stored

	return &out, nil
}

// AddFinding stores a finding and returns its ARN. Used to seed test state.
func (b *InMemoryBackend) AddFinding(
	findingType, severityLabel, status, title, description string,
	resources []FindingResource,
) string {
	b.mu.Lock("AddFinding")
	defer b.mu.Unlock()

	id := uuid.New().String()
	findingARN := arn.Build(inspector2Service, b.region, b.accountID, "finding/"+id)
	now := time.Now().UTC()

	b.findings.Put(&storedFinding{
		Finding: Finding{
			FindingArn:      findingARN,
			AccountID:       b.accountID,
			Type:            findingType,
			Severity:        FindingSeverity{Label: severityLabel, Score: severityScore(severityLabel)},
			Status:          status,
			Description:     description,
			Title:           title,
			Resources:       resources,
			FirstObservedAt: now,
			LastObservedAt:  now,
			UpdatedAt:       now,
		},
	})

	return findingARN
}

// severityScore returns a numeric score for a severity label.
func severityScore(label string) float64 {
	switch label {
	case "CRITICAL":
		return severityScoreCritical
	case "HIGH":
		return severityScoreHigh
	case "MEDIUM":
		return severityScoreMedium
	case "LOW":
		return severityScoreLow
	default:
		return 0.0
	}
}

// findingFilterCriteria captures the subset of the Inspector2 filterCriteria
// shape that ListFindings evaluates. Each slice is a set of string filters with
// a comparison and value, matching the AWS StringFilter wire shape.
type findingFilterCriteria struct {
	severities   []stringFilter
	findingTypes []stringFilter
	statuses     []stringFilter
	accountIDs   []stringFilter
}

type stringFilter struct {
	comparison string
	value      string
}

// parseFindingFilterCriteria decodes the AWS filterCriteria map into the subset
// of string filters ListFindings supports. Unknown criteria keys are ignored
// (AWS accepts a large criteria object; unsupported facets simply do not narrow
// the result here rather than erroring).
func parseFindingFilterCriteria(criteria map[string]any) findingFilterCriteria {
	var fc findingFilterCriteria

	fc.severities = extractStringFilters(criteria, "severity")
	fc.findingTypes = extractStringFilters(criteria, "findingType")
	fc.statuses = extractStringFilters(criteria, "findingStatus")
	fc.accountIDs = extractStringFilters(criteria, "awsAccountId")

	return fc
}

func extractStringFilters(criteria map[string]any, key string) []stringFilter {
	raw, ok := criteria[key].([]any)
	if !ok {
		return nil
	}

	filters := make([]stringFilter, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		cmp, _ := m["comparison"].(string)
		val, _ := m["value"].(string)

		if val == "" {
			continue
		}

		if cmp == "" {
			cmp = "EQUALS"
		}

		filters = append(filters, stringFilter{comparison: cmp, value: val})
	}

	return filters
}

func matchStringFilters(filters []stringFilter, actual string) bool {
	if len(filters) == 0 {
		return true
	}

	// AWS treats multiple filters on the same field as a logical OR.
	for _, f := range filters {
		switch f.comparison {
		case "PREFIX":
			if len(actual) >= len(f.value) && actual[:len(f.value)] == f.value {
				return true
			}
		case "NOT_EQUALS":
			if actual != f.value {
				return true
			}
		default: // EQUALS and any unrecognized comparison
			if actual == f.value {
				return true
			}
		}
	}

	return false
}

func (fc findingFilterCriteria) matches(f *Finding) bool {
	return matchStringFilters(fc.severities, f.Severity.Label) &&
		matchStringFilters(fc.findingTypes, f.Type) &&
		matchStringFilters(fc.statuses, f.Status) &&
		matchStringFilters(fc.accountIDs, f.AccountID)
}

// ListFindings returns a page of seeded findings filtered by the supplied
// filterCriteria. With no seeded findings it returns an empty page (preserving
// the prior always-empty contract for callers that never seed). Pagination uses
// the finding ARN as a stable cursor over the sorted result set.
func (b *InMemoryBackend) ListFindings(
	maxResults int32, nextToken string, criteria map[string]any,
) ([]*Finding, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	fc := parseFindingFilterCriteria(criteria)

	matched := make([]*Finding, 0, b.findings.Len())

	b.findings.Range(func(f *storedFinding) bool {
		if fc.matches(&f.Finding) {
			clone := f.Finding
			matched = append(matched, &clone)
		}

		return true
	})

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].FindingArn < matched[j].FindingArn
	})

	pageSize := int(maxResults)
	if pageSize <= 0 {
		pageSize = defaultFindingsPageSize
	}

	start := 0

	if nextToken != "" {
		for i, f := range matched {
			if f.FindingArn == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+pageSize, len(matched))

	page := matched[start:end]

	next := ""
	if end < len(matched) {
		next = matched[end].FindingArn
	}

	return page, next, nil
}

// FindingSeverityCounts returns the number of seeded findings grouped by
// severity, used by ListFindingAggregations.
func (b *InMemoryBackend) FindingSeverityCounts() map[string]int64 {
	b.mu.RLock("FindingSeverityCounts")
	defer b.mu.RUnlock()

	counts := make(map[string]int64, b.findings.Len())
	b.findings.Range(func(f *storedFinding) bool {
		counts[f.Severity.Label]++

		return true
	})

	return counts
}

// CreateFindingsReport creates an async findings report.
func (b *InMemoryBackend) CreateFindingsReport(destination map[string]any) (*FindingsReport, error) {
	b.mu.Lock("CreateFindingsReport")
	defer b.mu.Unlock()

	reportID := b.buildReportARN()
	report := &FindingsReport{
		ReportID:    reportID,
		Status:      "SUCCEEDED",
		Destination: destination,
		CreatedAt:   time.Now().UTC(),
	}
	b.findingsReports.Put(report)

	return report, nil
}

// CancelFindingsReport cancels a findings report.
func (b *InMemoryBackend) CancelFindingsReport(reportID string) error {
	b.mu.Lock("CancelFindingsReport")
	defer b.mu.Unlock()

	r, ok := b.findingsReports.Get(reportID)
	if !ok {
		return ErrReportNotFound
	}

	r.Status = "CANCELLED"

	return nil
}

// GetFindingsReportStatus returns the status of a findings report.
func (b *InMemoryBackend) GetFindingsReportStatus(reportID string) (*FindingsReport, error) {
	b.mu.RLock("GetFindingsReportStatus")
	defer b.mu.RUnlock()

	if reportID == "" {
		// Return the most recent report if no ID given.
		for _, r := range b.findingsReports.Snapshot() {
			cp := *r

			return &cp, nil
		}

		return nil, ErrReportNotFound
	}

	r, ok := b.findingsReports.Get(reportID)
	if !ok {
		return nil, ErrReportNotFound
	}

	cp := *r

	return &cp, nil
}

// ListFindingAggregations returns aggregated finding counts. When findings have
// been seeded it reports the real per-account severity breakdown; otherwise it
// returns an empty responses list (matching the prior empty-stub contract).
func (b *InMemoryBackend) ListFindingAggregations(aggregationType string, _ map[string]any) (map[string]any, error) {
	if aggregationType == "" {
		aggregationType = "ACCOUNT"
	}

	counts := b.FindingSeverityCounts()
	if len(counts) == 0 {
		return map[string]any{
			"aggregationType": aggregationType,
			"responses":       []any{},
		}, nil
	}

	var critical, high, medium, low, total int64
	for sev, n := range counts {
		total += n

		switch sev {
		case severityCritical:
			critical += n
		case severityHigh:
			high += n
		case severityMedium:
			medium += n
		case severityLow:
			low += n
		}
	}

	return map[string]any{
		"aggregationType": aggregationType,
		"responses": []map[string]any{
			{
				"accountAggregation": map[string]any{
					keyAccountID: b.accountID,
					"severityCounts": map[string]any{
						"all":      total,
						"critical": critical,
						"high":     high,
						"medium":   medium,
						"low":      low,
					},
				},
			},
		},
	}, nil
}

// SearchVulnerabilities returns matching vulnerabilities (stub).
func (b *InMemoryBackend) SearchVulnerabilities(_ map[string]any, _ string) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}

// BatchGetCodeSnippet returns code snippets for findings (stub).
func (b *InMemoryBackend) BatchGetCodeSnippet(_ []string) (map[string]any, error) {
	return map[string]any{
		"codeSnippetResults": []any{},
		"errors":             []any{},
	}, nil
}

// BatchGetFindingDetails returns finding details (stub).
func (b *InMemoryBackend) BatchGetFindingDetails(_ []map[string]any) (map[string]any, error) {
	return map[string]any{
		"findingDetails": []any{},
		"errors":         []any{},
	}, nil
}
