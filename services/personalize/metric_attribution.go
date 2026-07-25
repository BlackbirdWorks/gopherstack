package personalize

import (
	"fmt"
	"sort"
	"time"
)

// --- MetricAttribution ---

// CreateMetricAttribution creates a new metric attribution. metrics is a
// required field on the real CreateMetricAttribution API (a list of the
// event-type/expression pairs to track), not an optional passthrough.
func (b *InMemoryBackend) CreateMetricAttribution(
	name, datasetGroupArn string,
	metrics []MetricAttribute,
	metricsOutputConfig map[string]any,
	tags map[string]string,
) (*MetricAttribution, error) {
	b.mu.Lock("CreateMetricAttribution")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if len(metrics) == 0 {
		return nil, fmt.Errorf("%w: metrics is required", ErrValidation)
	}
	if b.metricAttributions.Has(name) {
		return nil, fmt.Errorf("%w: metric attribution %q already exists", ErrAlreadyExists, name)
	}
	if b.findDatasetGroup(datasetGroupArn) == nil {
		return nil, fmt.Errorf("%w: dataset group %q not found", ErrNotFound, datasetGroupArn)
	}

	now := time.Now().UTC()
	ma := &MetricAttribution{
		MetricAttributionArn: b.personalizeARN("metric-attribution", name),
		Name:                 name,
		DatasetGroupArn:      datasetGroupArn,
		Metrics:              append([]MetricAttribute(nil), metrics...),
		MetricsOutputConfig:  metricsOutputConfig,
		Status:               statusActive,
		CreationDateTime:     now,
		LastUpdatedDateTime:  now,
	}
	b.metricAttributions.Put(ma)
	if len(tags) > 0 {
		b.tags[ma.MetricAttributionArn] = copyStringMap(tags)
	}

	return ma, nil
}

// DescribeMetricAttribution returns a metric attribution by name or ARN.
func (b *InMemoryBackend) DescribeMetricAttribution(nameOrArn string) (*MetricAttribution, error) {
	b.mu.RLock("DescribeMetricAttribution")
	defer b.mu.RUnlock()

	if ma := b.findMetricAttribution(nameOrArn); ma != nil {
		return ma, nil
	}

	return nil, fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, nameOrArn)
}

// UpdateMetricAttribution updates a metric attribution's tracked metrics
// and/or output config. The real UpdateMetricAttribution API has no single
// "metrics" replacement field -- it mutates the existing metric list via
// addMetrics/removeMetrics (by metricName), matching AddMetrics/RemoveMetrics
// on the request.
func (b *InMemoryBackend) UpdateMetricAttribution(
	nameOrArn string,
	addMetrics []MetricAttribute,
	removeMetrics []string,
	metricsOutputConfig map[string]any,
) (*MetricAttribution, error) {
	b.mu.Lock("UpdateMetricAttribution")
	defer b.mu.Unlock()

	ma := b.findMetricAttribution(nameOrArn)
	if ma == nil {
		return nil, fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, nameOrArn)
	}

	if len(removeMetrics) > 0 {
		removeSet := make(map[string]bool, len(removeMetrics))
		for _, name := range removeMetrics {
			removeSet[name] = true
		}
		kept := make([]MetricAttribute, 0, len(ma.Metrics))
		for _, m := range ma.Metrics {
			if !removeSet[m.MetricName] {
				kept = append(kept, m)
			}
		}
		ma.Metrics = kept
	}
	ma.Metrics = append(ma.Metrics, addMetrics...)

	if metricsOutputConfig != nil {
		ma.MetricsOutputConfig = metricsOutputConfig
	}
	ma.LastUpdatedDateTime = time.Now().UTC()

	return ma, nil
}

// DeleteMetricAttribution removes a metric attribution.
func (b *InMemoryBackend) DeleteMetricAttribution(nameOrArn string) error {
	b.mu.Lock("DeleteMetricAttribution")
	defer b.mu.Unlock()

	ma := b.findMetricAttribution(nameOrArn)
	if ma == nil {
		return fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, nameOrArn)
	}
	b.metricAttributions.Delete(ma.Name)
	delete(b.tags, ma.MetricAttributionArn)

	return nil
}

// ListMetricAttributions returns metric attributions, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListMetricAttributions(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*MetricAttribution, string) {
	b.mu.RLock("ListMetricAttributions")
	defer b.mu.RUnlock()

	all := b.metricAttributions.Snapshot()
	filtered := make([]*MetricAttribution, 0, len(all))
	for _, ma := range all {
		if datasetGroupArn == "" || ma.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, ma)
		}
	}

	return paginateItems(filtered, metricAttributionKeyFn, maxResults, nextToken)
}

// ListMetricAttributionMetrics returns the metric attribution's own tracked
// metrics (as configured via CreateMetricAttribution/UpdateMetricAttribution)
// with pagination, rather than a fabricated static list.
func (b *InMemoryBackend) ListMetricAttributionMetrics(
	metricAttributionArn string,
	maxResults int,
	nextToken string,
) ([]MetricAttribute, string, error) {
	b.mu.RLock("ListMetricAttributionMetrics")
	defer b.mu.RUnlock()

	ma := b.findMetricAttribution(metricAttributionArn)
	if ma == nil {
		return nil, "", fmt.Errorf("%w: metric attribution %q not found", ErrNotFound, metricAttributionArn)
	}

	// Build string keys for pagination helper (use metricName as key).
	keys := make([]string, len(ma.Metrics))
	byKey := make(map[string]MetricAttribute, len(ma.Metrics))
	for i, m := range ma.Metrics {
		keys[i] = m.MetricName
		byKey[m.MetricName] = m
	}
	sort.Strings(keys)

	paged, outToken := paginate(keys, func(k string) MetricAttribute { return byKey[k] }, maxResults, nextToken)

	return paged, outToken, nil
}

func (b *InMemoryBackend) findMetricAttribution(nameOrArn string) *MetricAttribution {
	if ma, ok := b.metricAttributions.Get(nameOrArn); ok {
		return ma
	}
	for _, ma := range b.metricAttributions.All() {
		if ma.MetricAttributionArn == nameOrArn {
			return ma
		}
	}

	return nil
}
