package autoscaling

import "fmt"

// DescribeMetricCollectionTypes returns the supported metric collection types.
func (b *InMemoryBackend) DescribeMetricCollectionTypes() ([]MetricCollectionType, error) {
	return []MetricCollectionType{
		{Metric: "GroupMinSize", Granularity: granularity1Minute},
		{Metric: "GroupMaxSize", Granularity: granularity1Minute},
		{Metric: "GroupDesiredCapacity", Granularity: granularity1Minute},
		{Metric: "GroupInServiceInstances", Granularity: granularity1Minute},
		{Metric: "GroupPendingInstances", Granularity: granularity1Minute},
		{Metric: "GroupTerminatingInstances", Granularity: granularity1Minute},
	}, nil
}

// isKnownMetric reports whether metric is a valid Auto Scaling metric name.
func isKnownMetric(metric string) bool {
	switch metric {
	case "GroupMinSize", "GroupMaxSize", "GroupDesiredCapacity",
		"GroupInServiceInstances", "GroupPendingInstances", "GroupTerminatingInstances":
		return true
	}

	return false
}

// EnableMetricsCollection adds metrics to the ASG's enabled metrics list.
func (b *InMemoryBackend) EnableMetricsCollection(groupName string, metrics []string, _ string) error {
	b.mu.Lock("EnableMetricsCollection")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	// Validate provided metrics against known metrics
	if len(metrics) > 0 {
		for _, m := range metrics {
			if !isKnownMetric(m) {
				return fmt.Errorf("%w: unknown metric %q", ErrInvalidParameter, m)
			}
		}
	}

	existing := make(map[string]bool, len(g.EnabledMetrics))
	for _, m := range g.EnabledMetrics {
		existing[m] = true
	}

	for _, m := range metrics {
		if !existing[m] {
			g.EnabledMetrics = append(g.EnabledMetrics, m)
		}
	}

	return nil
}

// DisableMetricsCollection removes metrics from the ASG's enabled metrics list.
func (b *InMemoryBackend) DisableMetricsCollection(groupName string, metrics []string) error {
	b.mu.Lock("DisableMetricsCollection")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	if len(metrics) == 0 {
		g.EnabledMetrics = nil

		return nil
	}

	removeSet := make(map[string]bool, len(metrics))
	for _, m := range metrics {
		removeSet[m] = true
	}

	newMetrics := make([]string, 0, len(g.EnabledMetrics))
	for _, m := range g.EnabledMetrics {
		if !removeSet[m] {
			newMetrics = append(newMetrics, m)
		}
	}

	g.EnabledMetrics = newMetrics

	return nil
}
