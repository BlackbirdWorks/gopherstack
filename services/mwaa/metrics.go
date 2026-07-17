package mwaa

import "context"

// PublishMetrics stores internal environment metrics for the specified environment.
// The total number of metrics per environment is capped at maxMetricsPerEnv.
func (b *InMemoryBackend) PublishMetrics(ctx context.Context, envName string, req *publishMetricsRequest) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("PublishMetrics")
	defer b.mu.Unlock()

	if !b.environments.Has(regionKey(region, envName)) {
		return ErrEnvironmentNotFound
	}

	metrics := b.metricsStore(region)
	metrics[envName] = append(metrics[envName], req.MetricData...)

	if data := metrics[envName]; len(data) > maxMetricsPerEnv {
		// Copy the surviving tail into a right-sized slice so the trimmed-off
		// prefix is released for GC instead of being pinned by an oversized
		// backing array.
		trimmed := make([]MetricDatum, maxMetricsPerEnv)
		copy(trimmed, data[len(data)-maxMetricsPerEnv:])
		metrics[envName] = trimmed
	}

	return nil
}

// GetMetrics returns the stored metrics for the specified environment.
func (b *InMemoryBackend) GetMetrics(ctx context.Context, envName string) ([]MetricDatum, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetMetrics")
	defer b.mu.RUnlock()

	if !b.environments.Has(regionKey(region, envName)) {
		return nil, ErrEnvironmentNotFound
	}

	data := b.metricsStore(region)[envName]
	result := make([]MetricDatum, len(data))
	copy(result, data)

	return result, nil
}
