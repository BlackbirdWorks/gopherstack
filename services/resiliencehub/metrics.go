package resiliencehub

import (
	"time"
)

// ListMetrics always returns an empty result set. The real operation
// queries a historical metrics store (resiliency-score/compliance trends
// over time, filterable/sortable/aggregatable per Condition/Field/Sort) that
// this backend does not maintain -- ResiliencyScore itself is a documented
// placeholder (see PARITY.md), so there is no real time-series data to
// report metrics over without fabricating one. An honestly-empty result is
// correct here, not a stub.
func (b *InMemoryBackend) ListMetrics() {
	// No validation possible: ListMetricsInput carries no resource ARN to
	// check against real backend state -- this op is a pure, honestly-empty
	// query result.
}

// StartMetricsExport starts a new (Pending -> Success) async export of the
// current application-configuration metrics. Real async task bookkeeping;
// ExportLocation is a synthetic, documented placeholder -- no actual S3
// object is written (matching CreateRecommendationTemplate's identical
// treatment of TemplatesLocation, see templates.go).
func (b *InMemoryBackend) StartMetricsExport(req *startMetricsExportRequest) *MetricsExport {
	b.mu.Lock("StartMetricsExport")
	defer b.mu.Unlock()

	m := &MetricsExport{
		ID:           newMetricsExportID(),
		Status:       AsyncStatusPending,
		BucketName:   req.BucketName,
		CreationTime: time.Now().UTC(),
	}
	b.metricsExports.Put(m)
	b.scheduleMetricsExport(m.ID)

	return m.clone()
}

func (b *InMemoryBackend) scheduleMetricsExport(id string) {
	b.work.After("MetricsExport", asyncTransitionDelay, func() {
		b.mu.Lock("MetricsExport-async")
		defer b.mu.Unlock()

		m, ok := b.metricsExports.Get(id)
		if !ok || m.Status != AsyncStatusPending {
			return
		}

		m.Status = AsyncStatusSuccess

		if m.BucketName != "" {
			m.ExportLocation = &S3Location{Bucket: m.BucketName, Prefix: "resiliencehub-metrics/" + m.ID + "/"}
		}
	})
}

// DescribeMetricsExport returns the export identified by metricsExportID.
func (b *InMemoryBackend) DescribeMetricsExport(metricsExportID string) (*MetricsExport, error) {
	b.mu.RLock("DescribeMetricsExport")
	defer b.mu.RUnlock()

	m, ok := b.metricsExports.Get(metricsExportID)
	if !ok {
		return nil, notFoundError(resourceMetricsExport, metricsExportID)
	}

	return m.clone(), nil
}
