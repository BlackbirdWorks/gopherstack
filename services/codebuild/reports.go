package codebuild

import (
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildReportGroupARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "report-group/"+name)
}

// CreateReportGroup creates a new report group.
func (b *InMemoryBackend) CreateReportGroup(
	name, rtype string, exportConfig ReportExportConfig, tags map[string]string,
) (*ReportGroup, error) {
	b.mu.Lock("CreateReportGroup")
	defer b.mu.Unlock()

	if b.reportGroups.Has(name) {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	rg := &ReportGroup{
		Arn:          b.buildReportGroupARN(name),
		Name:         name,
		Type:         rtype,
		Status:       "ACTIVE",
		ExportConfig: exportConfig,
		Tags:         tagsCopy,
		Created:      now,
		LastModified: now,
	}
	b.reportGroups.Put(rg)

	out := *rg

	return &out, nil
}

// BatchGetReportGroups returns report groups by ARN. Missing ARNs are returned separately.
func (b *InMemoryBackend) BatchGetReportGroups(arns []string) ([]*ReportGroup, []string) {
	b.mu.RLock("BatchGetReportGroups")
	defer b.mu.RUnlock()

	found := make([]*ReportGroup, 0, len(arns))
	notFound := make([]string, 0, len(arns))

	for _, a := range arns {
		var (
			rg *ReportGroup
			ok bool
		)

		if matches := b.reportGroupsByARN.Get(a); len(matches) > 0 {
			rg, ok = matches[0], true
		} else if v, found2 := b.reportGroups.Get(a); found2 {
			// also try by name for convenience
			rg, ok = v, true
		}

		if ok {
			out := *rg
			found = append(found, &out)
		} else {
			notFound = append(notFound, a)
		}
	}

	return found, notFound
}

// AddReportInternal seeds a Report directly into the backend (test helper).
func (b *InMemoryBackend) AddReportInternal(r *Report) {
	b.mu.Lock("AddReportInternal")
	defer b.mu.Unlock()

	b.reports.Put(r)
}

// BatchGetReports returns reports by ARN. Missing ARNs are returned separately.
func (b *InMemoryBackend) BatchGetReports(arns []string) ([]*Report, []string) {
	b.mu.RLock("BatchGetReports")
	defer b.mu.RUnlock()

	found := make([]*Report, 0, len(arns))
	notFound := make([]string, 0, len(arns))

	for _, a := range arns {
		if r, ok := b.reports.Get(a); ok {
			out := *r
			found = append(found, &out)
		} else {
			notFound = append(notFound, a)
		}
	}

	return found, notFound
}

// DeleteReport removes a report by ARN.
func (b *InMemoryBackend) DeleteReport(arnStr string) error {
	b.mu.Lock("DeleteReport")
	defer b.mu.Unlock()

	if !b.reports.Delete(arnStr) {
		return ErrNotFound
	}

	return nil
}

// ListReports returns all report ARNs in sorted order.
func (b *InMemoryBackend) ListReports() []string {
	b.mu.RLock("ListReports")
	defer b.mu.RUnlock()

	items := b.reports.Snapshot()
	arns := make([]string, len(items))

	for i, r := range items {
		arns[i] = r.Arn
	}

	return arns
}

// ListReportsForReportGroup returns all report ARNs for the given report group ARN.
func (b *InMemoryBackend) ListReportsForReportGroup(reportGroupArn string) []string {
	b.mu.RLock("ListReportsForReportGroup")
	defer b.mu.RUnlock()

	group := b.reportsByGroup.Get(reportGroupArn)
	arns := make([]string, len(group))

	for i, r := range group {
		arns[i] = r.Arn
	}

	sort.Strings(arns)

	return arns
}

// DeleteReportGroup removes a report group by ARN.
func (b *InMemoryBackend) DeleteReportGroup(arnStr string) error {
	b.mu.Lock("DeleteReportGroup")
	defer b.mu.Unlock()

	matches := b.reportGroupsByARN.Get(arnStr)
	if len(matches) == 0 {
		return ErrNotFound
	}

	b.reportGroups.Delete(matches[0].Name)

	return nil
}

// UpdateReportGroup updates the export config of a report group.
func (b *InMemoryBackend) UpdateReportGroup(arnStr string, exportConfig *ReportExportConfig) (*ReportGroup, error) {
	b.mu.Lock("UpdateReportGroup")
	defer b.mu.Unlock()

	matches := b.reportGroupsByARN.Get(arnStr)
	if len(matches) == 0 {
		return nil, ErrNotFound
	}

	rg := matches[0]
	if exportConfig != nil {
		rg.ExportConfig = *exportConfig
	}

	rg.LastModified = float64(time.Now().Unix())
	out := *rg

	return &out, nil
}

// ListReportGroups returns all report group ARNs in sorted order.
func (b *InMemoryBackend) ListReportGroups() []string {
	b.mu.RLock("ListReportGroups")
	defer b.mu.RUnlock()

	items := b.reportGroups.All()
	arns := make([]string, 0, len(items))

	for _, rg := range items {
		arns = append(arns, rg.Arn)
	}

	sort.Strings(arns)

	return arns
}

// ListSharedReportGroups returns an empty list (no shared report groups in emulator).
func (b *InMemoryBackend) ListSharedReportGroups() []string {
	return []string{}
}

// DescribeCodeCoverages returns an empty list (no state needed).
func (b *InMemoryBackend) DescribeCodeCoverages(_ string) ([]CodeCoverage, error) {
	return []CodeCoverage{}, nil
}

// DescribeTestCases returns an empty list (no state needed).
func (b *InMemoryBackend) DescribeTestCases(_ string) ([]TestCase, error) {
	return []TestCase{}, nil
}

// GetReportGroupTrend returns an empty stats map (no state needed).
func (b *InMemoryBackend) GetReportGroupTrend(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}
