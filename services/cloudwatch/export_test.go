package cloudwatch

import "time"

// SignPageTokenForTest exposes signPageToken for unit testing.
func SignPageTokenForTest(offset int) string { return signPageToken(offset) }

// ParseSignedPageTokenForTest exposes parseSignedPageToken for unit testing.
func ParseSignedPageTokenForTest(token string) (int, error) { return parseSignedPageToken(token) }

// ValidateMetricDatumForTest exposes validateMetricDatum for unit testing.
func ValidateMetricDatumForTest(d MetricDatum) error { return validateMetricDatum(d) }

// ValidateStorageResolutionForTest exposes validateStorageResolution for unit testing.
func ValidateStorageResolutionForTest(res int32) error { return validateStorageResolution(res) }

// TopoSortExpressionsForTest exposes topoSortExpressions for unit testing.
func TopoSortExpressionsForTest(queries []MetricDataQuery) ([]string, error) {
	return topoSortExpressions(queries)
}

// ExtractExprDepsForTest exposes extractExprDeps for unit testing.
func ExtractExprDepsForTest(expr string, known map[string]bool) []string {
	return extractExprDeps(expr, known)
}

// RollingStatsForTest exposes rollingStats for unit testing.
func RollingStatsForTest(vals []float64) (float64, float64) { return rollingStats(vals) }

// ComputeAnomalyBandForTest exposes computeAnomalyBand for unit testing.
func ComputeAnomalyBandForTest(values []float64) ([]float64, []float64) {
	return computeAnomalyBand(values)
}

// EvalAnomalyDetectionBandForTest exposes evalAnomalyDetectionBand for unit testing.
func EvalAnomalyDetectionBandForTest(
	expr string,
	resolved map[string]MetricDataResult,
) (MetricDataResult, MetricDataResult, bool) {
	return evalAnomalyDetectionBand(expr, resolved)
}

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

// AlarmHistoryKeyCountForTest returns the number of distinct alarm names that
// currently have retained history, for leak-detection tests.
func (b *InMemoryBackend) AlarmHistoryKeyCountForTest() int {
	b.mu.RLock("AlarmHistoryKeyCountForTest")
	defer b.mu.RUnlock()

	return len(b.alarmHistory)
}

// MetricPointsCapForTest returns the backing-array capacity of the points slice
// for the named metric series in the given namespace and dimension key.
// Used to verify that the cap-and-copy path bounds allocation, not just length.
func (b *InMemoryBackend) MetricPointsCapForTest(namespace, metricKey string) int {
	b.mu.RLock("MetricPointsCapForTest")
	defer b.mu.RUnlock()

	nsMetrics, ok := b.metrics[namespace]
	if !ok {
		return -1
	}

	rec, ok := nsMetrics[metricKey]
	if !ok {
		return -1
	}

	return cap(rec.Points)
}

// CwMaxMetricDataPointsForTest exposes the per-series data-point cap for tests.
const CwMaxMetricDataPointsForTest = cwMaxMetricDataPoints

// ComputeExtendedStatsForTest exposes computeExtendedStats for unit testing.
func ComputeExtendedStatsForTest(sortedVals []float64, stats []string) map[string]float64 {
	return computeExtendedStats(sortedVals, stats)
}

// ComputeExtendedStatForTest exposes computeExtendedStat for unit testing.
func ComputeExtendedStatForTest(sortedVals []float64, stat string) (float64, bool) {
	return computeExtendedStat(sortedVals, stat)
}

// ExpandDatumValuesForTest exposes expandDatumValues for unit testing.
func ExpandDatumValuesForTest(d MetricDatum) []float64 { return expandDatumValues(d) }

// PaginateMetricDataForTest exposes paginateMetricData for unit testing.
func PaginateMetricDataForTest(
	all []MetricDataResult,
	maxDatapoints int,
	nextToken string,
) GetMetricDataPage {
	return paginateMetricData(all, maxDatapoints, nextToken)
}

// AnnotateArithmeticMessagesForTest exposes annotateArithmeticMessages for testing.
func AnnotateArithmeticMessagesForTest(r *MetricDataResult) { annotateArithmeticMessages(r) }

// ParseEC2AutomateVerbForTest exposes parseEC2AutomateVerb for unit testing.
func ParseEC2AutomateVerbForTest(action string) (string, bool) {
	return parseEC2AutomateVerb(action)
}

// ParseScalingPolicyARNForTest exposes parseScalingPolicyARN for unit testing.
func ParseScalingPolicyARNForTest(action string) (string, string, bool) {
	return parseScalingPolicyARN(action)
}

// ExtractAlarmRuleRefsForTest exposes extractAlarmRuleRefs, flattened to parallel
// func/name slices for the external test package.
func ExtractAlarmRuleRefsForTest(rule string) ([]string, []string) {
	refs := extractAlarmRuleRefs(rule)
	funcs := make([]string, 0, len(refs))
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		funcs = append(funcs, r.Func)
		names = append(names, r.Name)
	}

	return funcs, names
}

// ParseRelativeDurationForTest exposes parseRelativeDuration for unit testing.
func ParseRelativeDurationForTest(s string) (time.Duration, bool) {
	return parseRelativeDuration(s)
}

// RenderMetricWidgetPNGForTest exposes renderMetricWidgetPNG for unit testing.
func RenderMetricWidgetPNGForTest(
	b *InMemoryBackend,
	widgetJSON string,
	now time.Time,
) ([]byte, error) {
	return renderMetricWidgetPNG(b, widgetJSON, now)
}
