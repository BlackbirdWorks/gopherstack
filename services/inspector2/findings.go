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
func (b *InMemoryBackend) CreateFindingsReport(
	destination, filterCriteria map[string]any, reportFormat string,
) (*FindingsReport, error) {
	b.mu.Lock("CreateFindingsReport")
	defer b.mu.Unlock()

	reportID := b.buildReportARN()
	report := &FindingsReport{
		ReportID:       reportID,
		Status:         "SUCCEEDED",
		Destination:    destination,
		FilterCriteria: filterCriteria,
		ReportFormat:   reportFormat,
		CreatedAt:      time.Now().UTC(),
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

// SeedVulnerability injects a vulnerability into the backend so
// SearchVulnerabilities can look it up by ID, mirroring SeedFinding: real
// SearchVulnerabilities queries AWS's own global vulnerability intelligence
// database (CVE/NVD-derived), which gopherstack has no equivalent data
// source for, so this is the additive capability that lets tests/fixtures
// populate specific vulnerability IDs instead of the op being permanently
// hardwired empty.
func (b *InMemoryBackend) SeedVulnerability(v Vulnerability) (*Vulnerability, error) {
	b.mu.Lock("SeedVulnerability")
	defer b.mu.Unlock()

	if v.ID == "" {
		return nil, ErrValidation
	}

	clone := v
	b.vulnerabilities.Put(&clone)

	out := v

	return &out, nil
}

// SearchVulnerabilities looks up seeded vulnerabilities by ID. Real
// SearchVulnerabilitiesFilterCriteria.VulnerabilityIds is a required exact-ID
// lookup list (not a free-text query), so this returns exactly the seeded
// vulnerabilities whose ID was requested, in requested order, silently
// omitting any ID with no seeded match (matching real AWS's behavior for an
// unknown vulnerability ID: it is simply absent from the response, not an
// error).
func (b *InMemoryBackend) SearchVulnerabilities(
	filterCriteria map[string]any, nextToken string,
) ([]*Vulnerability, string, error) {
	b.mu.RLock("SearchVulnerabilities")
	defer b.mu.RUnlock()

	ids, _ := filterCriteria["vulnerabilityIds"].([]any)

	matched := make([]*Vulnerability, 0, len(ids))

	for _, raw := range ids {
		id, ok := raw.(string)
		if !ok || id == "" {
			continue
		}

		if v, found := b.vulnerabilities.Get(id); found {
			clone := *v
			matched = append(matched, &clone)
		}
	}

	// Pagination is a formality here (SearchVulnerabilities requests are
	// bounded by the caller-supplied ID list, typically small), but the
	// nextToken cursor is honored for shape-completeness.
	start := 0

	if nextToken != "" {
		for i, v := range matched {
			if v.ID == nextToken {
				start = i

				break
			}
		}
	}

	return matched[start:], "", nil
}

// SeedCodeSnippet attaches code snippet content to a finding ARN so
// BatchGetCodeSnippet can return it, mirroring the SeedFinding/SeedCoverage/
// SeedVulnerability additive-capability precedent: gopherstack has no static
// analysis engine to derive real snippet content.
func (b *InMemoryBackend) SeedCodeSnippet(findingARN string, lines []CodeLine, fixes []SuggestedFix) error {
	b.mu.Lock("SeedCodeSnippet")
	defer b.mu.Unlock()

	if findingARN == "" {
		return ErrValidation
	}

	startLine, endLine := int32(0), int32(0)
	if len(lines) > 0 {
		startLine = lines[0].LineNumber
		endLine = lines[len(lines)-1].LineNumber
	}

	b.codeSnippets.Put(&codeSnippet{
		FindingArn:     findingARN,
		Lines:          lines,
		StartLine:      startLine,
		EndLine:        endLine,
		SuggestedFixes: fixes,
	})

	return nil
}

// BatchGetCodeSnippet returns seeded code snippet content for each requested
// finding ARN, or a CODE_SNIPPET_NOT_FOUND error entry (the only error code
// meaningful here -- see types.CodeSnippetErrorCode) for any ARN with none
// seeded. This replaces the prior stub, which silently ignored its input
// entirely and always returned two empty lists regardless of what was asked
// for.
func (b *InMemoryBackend) BatchGetCodeSnippet(findingARNs []string) (map[string]any, error) {
	b.mu.RLock("BatchGetCodeSnippet")
	defer b.mu.RUnlock()

	results := make([]map[string]any, 0, len(findingARNs))
	errs := make([]map[string]any, 0)

	for _, findingARN := range findingARNs {
		snip, ok := b.codeSnippets.Get(findingARN)
		if !ok {
			errs = append(errs, map[string]any{
				keyFindingArn:  findingARN,
				keyErrorCode:   "CODE_SNIPPET_NOT_FOUND",
				"errorMessage": "no code snippet is available for this finding",
			})

			continue
		}

		results = append(results, map[string]any{
			keyFindingArn:    snip.FindingArn,
			"codeSnippet":    snip.Lines,
			"startLine":      snip.StartLine,
			"endLine":        snip.EndLine,
			"suggestedFixes": snip.SuggestedFixes,
		})
	}

	return map[string]any{
		"codeSnippetResults": results,
		"errors":             errs,
	}, nil
}

// findingDetailToWire renders a Finding's BatchGetFindingDetails-only fields
// (see Finding's doc comment) in the real FindingDetail wire shape.
func findingDetailToWire(f *Finding) map[string]any {
	detail := map[string]any{keyFindingArn: f.FindingArn}

	if f.EpssScore != 0 {
		detail["epssScore"] = f.EpssScore
	}

	if f.RiskScore != 0 {
		detail["riskScore"] = f.RiskScore
	}

	if len(f.Cwes) > 0 {
		detail["cwes"] = f.Cwes
	}

	if len(f.ReferenceUrls) > 0 {
		detail["referenceUrls"] = f.ReferenceUrls
	}

	if len(f.Tools) > 0 {
		detail["tools"] = f.Tools
	}

	return detail
}

// BatchGetFindingDetails returns extended vulnerability details for each
// requested finding ARN that exists in the backend, or a
// FINDING_DETAILS_NOT_FOUND error entry for any ARN that does not. This
// replaces the prior stub (which additionally decoded its request body into
// the wrong shape entirely -- real BatchGetFindingDetailsInput.findingArns is
// a plain string array, not an array of objects).
func (b *InMemoryBackend) BatchGetFindingDetails(findingARNs []string) (map[string]any, error) {
	b.mu.RLock("BatchGetFindingDetails")
	defer b.mu.RUnlock()

	details := make([]map[string]any, 0, len(findingARNs))
	errs := make([]map[string]any, 0)

	for _, findingARN := range findingARNs {
		f, ok := b.findings.Get(findingARN)
		if !ok {
			errs = append(errs, map[string]any{
				keyFindingArn:  findingARN,
				keyErrorCode:   "FINDING_DETAILS_NOT_FOUND",
				"errorMessage": "no finding details are available for this finding",
			})

			continue
		}

		details = append(details, findingDetailToWire(&f.Finding))
	}

	return map[string]any{
		"findingDetails": details,
		"errors":         errs,
	}, nil
}
