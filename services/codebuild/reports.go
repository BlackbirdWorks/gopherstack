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

// DeleteReport removes a report by ARN. Idempotent: real AWS's DeleteReport
// declares no ResourceNotFoundException (botocore
// codebuild/2016-10-06/service-2.json operations.DeleteReport.errors: only
// InvalidInputException), so deleting an already-gone report is not an error.
func (b *InMemoryBackend) DeleteReport(arnStr string) error {
	b.mu.Lock("DeleteReport")
	defer b.mu.Unlock()

	b.reports.Delete(arnStr)

	return nil
}

// ListReports returns all report ARNs in sorted order, optionally filtered by
// status (empty statusFilter returns every report).
func (b *InMemoryBackend) ListReports(statusFilter string) []string {
	b.mu.RLock("ListReports")
	defer b.mu.RUnlock()

	items := b.reports.Snapshot()
	arns := make([]string, 0, len(items))

	for _, r := range items {
		if statusFilter != "" && r.Status != statusFilter {
			continue
		}

		arns = append(arns, r.Arn)
	}

	return arns
}

// ListReportsForReportGroup returns all report ARNs for the given report
// group ARN, optionally filtered by status (empty statusFilter returns every
// report in the group). Unlike ListReports/ListReportGroups, real AWS
// declares ResourceNotFoundException for this op (botocore
// codebuild/2016-10-06/service-2.json operations.ListReportsForReportGroup.errors),
// so a nonexistent reportGroupArn is rejected here.
func (b *InMemoryBackend) ListReportsForReportGroup(reportGroupArn, statusFilter string) ([]string, error) {
	b.mu.RLock("ListReportsForReportGroup")
	defer b.mu.RUnlock()

	if matches := b.reportGroupsByARN.Get(reportGroupArn); len(matches) == 0 {
		return nil, ErrNotFound
	}

	group := b.reportsByGroup.Get(reportGroupArn)
	arns := make([]string, 0, len(group))

	for _, r := range group {
		if statusFilter != "" && r.Status != statusFilter {
			continue
		}

		arns = append(arns, r.Arn)
	}

	sort.Strings(arns)

	return arns, nil
}

// DeleteReportGroup removes a report group by ARN. Idempotent: real AWS's
// DeleteReportGroup declares no ResourceNotFoundException (same botocore
// evidence as DeleteReport above), so deleting an already-gone group is not
// an error.
func (b *InMemoryBackend) DeleteReportGroup(arnStr string) error {
	b.mu.Lock("DeleteReportGroup")
	defer b.mu.Unlock()

	if matches := b.reportGroupsByARN.Get(arnStr); len(matches) > 0 {
		b.reportGroups.Delete(matches[0].Name)
	}

	return nil
}

// UpdateReportGroup updates the export config of a report group.
func (b *InMemoryBackend) UpdateReportGroup(
	arnStr string,
	exportConfig *ReportExportConfig,
	tags map[string]string,
) (*ReportGroup, error) {
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

	if tags != nil {
		tagsCopy := make(map[string]string, len(tags))
		maps.Copy(tagsCopy, tags)
		rg.Tags = tagsCopy
	}

	rg.LastModified = float64(time.Now().Unix())
	out := *rg

	return &out, nil
}

// ListReportGroups returns all report group ARNs ordered by name, ascending.
func (b *InMemoryBackend) ListReportGroups() []string {
	return b.ListReportGroupsSortedBy("")
}

// ListReportGroupsSortedBy returns all report group ARNs ordered per sortBy
// (CREATED_TIME|LAST_MODIFIED_TIME|NAME; any other value, including "",
// defaults to NAME), always ascending. Callers apply sortOrder/pagination on
// top via [paginateIDs].
func (b *InMemoryBackend) ListReportGroupsSortedBy(sortBy string) []string {
	b.mu.RLock("ListReportGroupsSortedBy")
	defer b.mu.RUnlock()

	items := b.reportGroups.Snapshot() // NAME-ascending by construction

	switch sortBy {
	case sortByCreatedTime:
		sort.SliceStable(items, func(i, j int) bool { return items[i].Created < items[j].Created })
	case sortByLastModifiedTime:
		sort.SliceStable(items, func(i, j int) bool { return items[i].LastModified < items[j].LastModified })
	}

	arns := make([]string, len(items))
	for i, rg := range items {
		arns[i] = rg.Arn
	}

	return arns
}

// ListSharedReportGroups returns an empty list (no shared report groups in emulator).
func (b *InMemoryBackend) ListSharedReportGroups() []string {
	return []string{}
}

// DescribeCodeCoverages returns an empty list; no coverage-content ingestion
// pipeline exists. Unlike DescribeTestCases/GetReportGroupTrend below, this
// op's real error set has no ResourceNotFoundException (botocore
// codebuild/2016-10-06/service-2.json operations.DescribeCodeCoverages.errors:
// only InvalidInputException), so a nonexistent reportArn is correctly not
// rejected here.
func (b *InMemoryBackend) DescribeCodeCoverages(_ string) ([]CodeCoverage, error) {
	return []CodeCoverage{}, nil
}

// DescribeTestCases returns an empty list once reportArn is confirmed to
// exist; no test-case-content ingestion pipeline exists. Real AWS declares
// ResourceNotFoundException for this op (unlike DescribeCodeCoverages).
func (b *InMemoryBackend) DescribeTestCases(reportArn string) ([]TestCase, error) {
	b.mu.RLock("DescribeTestCases")
	defer b.mu.RUnlock()

	if _, ok := b.reports.Get(reportArn); !ok {
		return nil, ErrNotFound
	}

	return []TestCase{}, nil
}

// GetReportGroupTrend returns an empty stats map once reportGroupArn is
// confirmed to exist; no report-execution data is modeled. Real AWS declares
// ResourceNotFoundException for this op.
func (b *InMemoryBackend) GetReportGroupTrend(reportGroupArn string) (map[string]any, error) {
	b.mu.RLock("GetReportGroupTrend")
	defer b.mu.RUnlock()

	if matches := b.reportGroupsByARN.Get(reportGroupArn); len(matches) == 0 {
		return nil, ErrNotFound
	}

	return map[string]any{}, nil
}
