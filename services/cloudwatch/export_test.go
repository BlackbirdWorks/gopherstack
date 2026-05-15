package cloudwatch

import "time"

// EvaluateAlarmRuleForTest is a test-visible wrapper around evaluateAlarmRule.
func EvaluateAlarmRuleForTest(rule string, states map[string]string) string {
	return evaluateAlarmRule(rule, func(name string) string {
		if s, ok := states[name]; ok {
			return s
		}

		return alarmStateInsufficientData
	})
}

// CwMaxMetricNamesPerNamespace exposes the constant for tests.
const CwMaxMetricNamesPerNamespace = cwMaxMetricNamesPerNamespace

// CwMetricRetentionDays exposes the constant for tests.
const CwMetricRetentionDays = cwMetricRetentionDays

// GetInsightRuleContributors is a test-visible wrapper that acquires the lock
// and delegates to the unexported implementation.
func (b *InMemoryBackend) GetInsightRuleContributorsForTest(
	ruleName string,
	startTime, endTime time.Time,
	maxContributorCount int,
	orderBy string,
) ([]AlarmContributor, error) {
	b.mu.RLock("GetInsightRuleContributorsForTest")
	defer b.mu.RUnlock()

	return b.GetInsightRuleContributors(ruleName, startTime, endTime, maxContributorCount, orderBy)
}

// DimensionSetKeyForTest exposes the internal dimensionSetKey for tests.
func DimensionSetKeyForTest(dims []Dimension) string {
	return dimensionSetKey(dims)
}

// StreamAllowsMetricForTest exposes the stream filter predicate for tests.
func StreamAllowsMetricForTest(s *MetricStream, namespace, metricName string) bool {
	return streamAllowsMetric(s, namespace, metricName)
}
