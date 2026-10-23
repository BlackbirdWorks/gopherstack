package servicequotas

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	reportIDHexLen = 32
	percent        = 100
	reportStages   = 2
)

func (b *InMemoryBackend) settleReportsLocked(now time.Time, dwell time.Duration) {
	for _, r := range b.reports.All() {
		if r.Status == reportPending && !now.Before(r.Created.Add(dwell)) {
			r.Status = reportInProgress
		}

		if r.Status == reportInProgress && !now.Before(r.Created.Add(reportStages*dwell)) {
			r.Status = reportCompleted
			r.Generated = r.Created.Add(reportStages * dwell)
			r.Quotas = b.buildReportLocked(scope{account: r.Account, region: r.Region})
		}
	}
}

func utilization(usage, applied float64) float64 {
	if applied <= 0 {
		return 0
	}

	return usage / applied * percent
}

func (b *InMemoryBackend) buildReportLocked(s scope) []UtilizationEntry {
	var out []UtilizationEntry

	for _, svc := range b.catalog.Services() {
		for i := range svc.Quotas {
			def := &svc.Quotas[i]

			var usage float64

			tracked := false
			if b.usage != nil {
				usage, tracked = b.usage.QuotaUsage(s.account, s.region, svc.Code, def.Code)
			}

			if def.Usage == nil && !tracked {
				continue
			}

			applied, _ := b.appliedValueLocked(s, svc.Code, def)
			entry := UtilizationEntry{
				ServiceCode: svc.Code, QuotaCode: def.Code, Applied: applied, Default: def.Default,
				Utilization: utilization(usage, applied), Adjustable: def.Adjustable,
			}

			if def.Usage != nil {
				entry.Namespace = def.Usage.Namespace
			}

			out = append(out, entry)
		}
	}

	slices.SortStableFunc(out, func(a, c UtilizationEntry) int { return cmp.Compare(c.Utilization, a.Utilization) })

	return out
}

// StartReport begins generating a utilization report.
func (b *InMemoryBackend) StartReport(ctx context.Context) (Report, error) {
	b.mu.Lock("StartQuotaUtilizationReport")
	defer b.mu.Unlock()

	b.settleLocked()

	s := b.scopeOf(ctx)

	for _, r := range b.reports.All() {
		if r.Account == s.account && r.Region == s.region && r.Status != reportCompleted {
			return Report{}, fail(ErrInvalidResourceState, "A utilization report is already being generated")
		}
	}

	r := &Report{
		ID: awsid.NewN("", reportIDHexLen), Account: s.account, Region: s.region,
		Status: reportPending, Created: b.now(), Seq: b.nextSeqLocked(),
	}
	b.reports.Put(r)

	return *r, nil
}

// ReportPage is a utilization report with one page of its quotas.
type ReportPage struct {
	Quotas page.Page[UtilizationEntry]
	Report Report
	Total  int
}

// GetReport returns a report and a page of its quotas.
func (b *InMemoryBackend) GetReport(ctx context.Context, id, token string, maxResults *int32) (ReportPage, error) {
	if id == "" {
		return ReportPage{}, fail(ErrIllegalArgument, "ReportId is required")
	}

	b.mu.Lock("GetQuotaUtilizationReport")
	defer b.mu.Unlock()

	b.settleLocked()

	s := b.scopeOf(ctx)

	r, ok := b.reports.Get(id)
	if !ok || r.Account != s.account || r.Region != s.region {
		return ReportPage{}, fail(ErrNoSuchResource, "The report %q was not found", id)
	}

	pg, err := paginate(r.Quotas, token, maxResults)
	if err != nil {
		return ReportPage{}, err
	}

	return ReportPage{Report: *r, Quotas: pg, Total: len(r.Quotas)}, nil
}
