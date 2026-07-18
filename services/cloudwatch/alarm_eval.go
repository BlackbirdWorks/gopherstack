package cloudwatch

import (
	"context"
	"fmt"
	"time"
)

// EvaluateAlarms evaluates all metric alarms against recent metric data and
// transitions state (OK ↔ ALARM ↔ INSUFFICIENT_DATA) as appropriate. SNS and
// Lambda alarm actions are fired on state change. Intended to be called
// periodically by the Janitor.
func (b *InMemoryBackend) EvaluateAlarms(ctx context.Context, now time.Time) {
	// Snapshot alarms under a read-lock to avoid holding the lock during evaluation.
	type alarmSnap struct {
		alarm MetricAlarm
	}

	var snaps []alarmSnap

	func() {
		b.mu.RLock("EvaluateAlarms.snapshot")
		defer b.mu.RUnlock()

		for _, a := range b.alarms.All() {
			isMultiMetric := len(a.Metrics) > 0
			if !isMultiMetric && (a.MetricName == "" || a.Namespace == "" || a.Period <= 0) {
				continue
			}
			if a.EvaluationPeriods <= 0 {
				continue
			}

			cp := *a
			snaps = append(snaps, alarmSnap{alarm: cp})
		}
	}()

	for _, snap := range snaps {
		newState := b.evaluateMetricAlarmState(snap.alarm, now)
		if newState == snap.alarm.StateValue {
			continue
		}

		var reason string
		switch newState {
		case alarmStateInsufficientData:
			reason = "Insufficient Data: not enough datapoints to evaluate"
		case alarmStateOK:
			reason = "Threshold Crossed: datapoints within normal range"
		default:
			reason = fmt.Sprintf(
				"Threshold Crossed: %d datapoints breached the threshold",
				snap.alarm.DatapointsToAlarm,
			)
		}

		// SetAlarmState acquires its own lock and fires SNS/Lambda actions.
		_ = b.SetAlarmState(ctx, snap.alarm.AlarmName, newState, reason, "")
	}
}

// fetchMultiMetricBuckets fetches data via GetMetricData and returns per-period bucket values.
func (b *InMemoryBackend) fetchMultiMetricBuckets(
	alarm MetricAlarm, now time.Time, evalPeriods int,
) (map[int]float64, map[int][2]float64, error) {
	period := alarm.Period
	if period <= 0 {
		for _, q := range alarm.Metrics {
			if q.MetricStat.Period > 0 {
				period = q.MetricStat.Period

				break
			}
			if q.Period > 0 {
				period = q.Period

				break
			}
		}
	}

	periodDur := time.Duration(period) * time.Second
	startTime := now.Add(-periodDur * time.Duration(evalPeriods))

	results, err := b.GetMetricData(alarm.Metrics, startTime, now)
	if err != nil {
		return nil, nil, err
	}

	return buildBucketValuesFromMetricData(results, alarm.Metrics, startTime, periodDur, evalPeriods),
		make(map[int][2]float64),
		nil
}

// fetchSingleMetricBuckets fetches data via GetMetricStatistics and returns per-period bucket values.
func (b *InMemoryBackend) fetchSingleMetricBuckets(
	alarm MetricAlarm, now time.Time, evalPeriods int,
) (map[int]float64, map[int][2]float64, error) {
	periodDur := time.Duration(alarm.Period) * time.Second
	startTime := now.Add(-periodDur * time.Duration(evalPeriods))

	stats := []string{alarm.Statistic}
	extStats := []string{}

	if alarm.Statistic == "" && alarm.ExtendedStatistic != "" {
		stats = nil
		extStats = []string{alarm.ExtendedStatistic}
	}

	datapoints, err := b.GetMetricStatistics(
		alarm.Namespace, alarm.MetricName, alarm.Dimensions,
		startTime, now, alarm.Period, stats, extStats,
	)
	if err != nil {
		return nil, nil, err
	}

	return buildBucketValues(
			datapoints, startTime, periodDur, evalPeriods, alarm.Statistic, alarm.ExtendedStatistic,
		),
		buildBucketBands(datapoints, startTime, periodDur, evalPeriods),
		nil
}

// evaluateMetricAlarmState computes the new state for a metric alarm.
// It fetches the most recent EvaluationPeriods periods of data, counts
// breaching periods applying TreatMissingData logic, and returns the resulting state.
// When alarm.Metrics is set the alarm is a multi-metric / metric-math alarm and
// GetMetricData is used instead of GetMetricStatistics.
func (b *InMemoryBackend) evaluateMetricAlarmState(alarm MetricAlarm, now time.Time) string {
	evalPeriods := int(alarm.EvaluationPeriods)

	treatMissing := alarm.TreatMissingData
	if treatMissing == "" {
		treatMissing = "missing"
	}

	datapointsToAlarm := int(alarm.DatapointsToAlarm)
	if datapointsToAlarm <= 0 {
		datapointsToAlarm = evalPeriods
	}

	var bucketValues map[int]float64
	var bucketBands map[int][2]float64
	var err error

	if len(alarm.Metrics) > 0 {
		bucketValues, bucketBands, err = b.fetchMultiMetricBuckets(alarm, now, evalPeriods)
	} else {
		bucketValues, bucketBands, err = b.fetchSingleMetricBuckets(alarm, now, evalPeriods)
	}

	if err != nil {
		return alarm.StateValue
	}

	breachCount, evaluatedCount, realDataCount := countBreachingPeriods(
		bucketValues,
		bucketBands,
		evalPeriods,
		treatMissing,
		alarm.Threshold,
		alarm.ComparisonOperator,
	)

	// TreatMissingData=ignore: missing datapoints are disregarded and the alarm
	// is evaluated only against the datapoints that are present. When there is no
	// real data in the evaluation window, AWS maintains the current alarm state
	// rather than transitioning (it does NOT go to INSUFFICIENT_DATA on ignore).
	if treatMissing == "ignore" {
		if realDataCount == 0 {
			// No data to decide on — keep whatever state the alarm is in.
			if alarm.StateValue == "" {
				return alarmStateInsufficientData
			}

			return alarm.StateValue
		}

		if breachCount >= datapointsToAlarm {
			return alarmStateAlarm
		}

		return alarmStateOK
	}

	if breachCount >= datapointsToAlarm {
		return alarmStateAlarm
	}

	if evaluatedCount < datapointsToAlarm && treatMissing == "missing" {
		return alarmStateInsufficientData
	}

	return alarmStateOK
}

// buildBucketValuesFromMetricData maps GetMetricData results into per-period buckets.
// The "alarm metric" is the first result where the source query had ReturnData=true,
// or when ReturnData is absent (zero value), the first MetricStat query.
func buildBucketValuesFromMetricData(
	results []MetricDataResult,
	queries []MetricDataQuery,
	startTime time.Time,
	periodDur time.Duration,
	evalPeriods int,
) map[int]float64 {
	// Find the ID of the alarm metric: the query with ReturnData=true.
	alarmID := ""

	for _, q := range queries {
		if q.ReturnData {
			alarmID = q.ID

			break
		}
	}

	// Fall back to the first MetricStat query (no expression).
	if alarmID == "" {
		for _, q := range queries {
			if q.Expression == "" {
				alarmID = q.ID

				break
			}
		}
	}

	if alarmID == "" && len(results) > 0 {
		alarmID = results[0].ID
	}

	bucketValues := make(map[int]float64, evalPeriods)

	for _, r := range results {
		if r.ID != alarmID {
			continue
		}

		for i, ts := range r.Timestamps {
			idx := int(ts.Sub(startTime) / periodDur)

			if idx >= 0 && idx < evalPeriods {
				bucketValues[idx] = r.Values[i]
			}
		}

		break
	}

	return bucketValues
}

// buildBucketValues maps each datapoint into its evaluation-period bucket.
func buildBucketValues(
	datapoints []Datapoint,
	startTime time.Time,
	periodDur time.Duration,
	evalPeriods int,
	statistic, extStatistic string,
) map[int]float64 {
	bucketValues := make(map[int]float64, len(datapoints))

	for _, dp := range datapoints {
		idx := int(dp.Timestamp.Sub(startTime) / periodDur)

		if idx < 0 || idx >= evalPeriods {
			continue
		}

		if val := extractDatapointValue(dp, statistic, extStatistic); val != nil {
			bucketValues[idx] = *val
		}
	}

	return bucketValues
}

// buildBucketBands extracts per-bucket anomaly band bounds [lower, upper] from datapoints.
// Used by countBreachingPeriods for GreaterThanUpperThreshold and
// LessThanLowerOrGreaterThanUpperThreshold comparison operators.
func buildBucketBands(
	datapoints []Datapoint,
	startTime time.Time,
	periodDur time.Duration,
	evalPeriods int,
) map[int][2]float64 {
	bands := make(map[int][2]float64, len(datapoints))

	for _, dp := range datapoints {
		if dp.BandLower == nil || dp.BandUpper == nil {
			continue
		}

		idx := int(dp.Timestamp.Sub(startTime) / periodDur)

		if idx < 0 || idx >= evalPeriods {
			continue
		}

		bands[idx] = [2]float64{*dp.BandLower, *dp.BandUpper}
	}

	return bands
}

// countBreachingPeriods tallies breach, evaluated, and real-datapoint counts
// across all evaluation periods. The third return value (realDataCount) counts
// only periods that have an actual datapoint, independent of treatMissing —
// callers use it to implement TreatMissingData=ignore (maintain state when no
// real data is present).
//
// bucketBands provides per-bucket [lower, upper] anomaly band bounds for
// GreaterThanUpperThreshold and LessThanLowerOrGreaterThanUpperThreshold.
// When no band is available for a bucket, threshold is used for both bounds.
func countBreachingPeriods(
	bucketValues map[int]float64,
	bucketBands map[int][2]float64,
	evalPeriods int,
	treatMissing string,
	threshold float64,
	comparisonOperator string,
) (int, int, int) {
	var breachCount, evaluatedCount, realDataCount int

	for i := range evalPeriods {
		val, hasData := bucketValues[i]

		if !hasData {
			switch treatMissing {
			case "breaching":
				breachCount++
				evaluatedCount++
			case "notBreaching":
				evaluatedCount++
			}

			continue
		}

		realDataCount++
		evaluatedCount++

		lowerBound, upperBound := threshold, threshold
		if band, ok := bucketBands[i]; ok {
			lowerBound, upperBound = band[0], band[1]
		}

		if breachesThreshold(val, lowerBound, upperBound, comparisonOperator) {
			breachCount++
		}
	}

	return breachCount, evaluatedCount, realDataCount
}

// extractDatapointValue extracts the relevant statistic value from a Datapoint.
func extractDatapointValue(dp Datapoint, statistic, extendedStatistic string) *float64 {
	switch statistic {
	case "Average":
		return dp.Average
	case statSum:
		return dp.Sum
	case "Minimum":
		return dp.Minimum
	case "Maximum":
		return dp.Maximum
	case "SampleCount":
		return dp.SampleCount
	}

	if extendedStatistic != "" {
		if v, ok := dp.ExtendedStatistics[extendedStatistic]; ok {
			return &v
		}
	}

	return nil
}

// breachesThreshold reports whether value breaches the threshold for the given operator.
// lowerBound and upperBound are used for anomaly-detection operators:
//   - GreaterThanUpperThreshold: fires when value > upperBound
//   - LessThanLowerThreshold: fires when value < lowerBound
//   - LessThanLowerOrGreaterThanUpperThreshold: fires when value < lowerBound OR value > upperBound
//
// For non-anomaly alarms, pass threshold for both lowerBound and upperBound.
func breachesThreshold(value, lowerBound, upperBound float64, op string) bool {
	switch op {
	case "GreaterThanThreshold":
		return value > upperBound
	case "GreaterThanOrEqualToThreshold":
		return value >= upperBound
	case "LessThanThreshold":
		return value < lowerBound
	case "LessThanOrEqualToThreshold":
		return value <= lowerBound
	case "GreaterThanUpperThreshold":
		return value > upperBound
	case "LessThanLowerThreshold":
		return value < lowerBound
	case "LessThanLowerOrGreaterThanUpperThreshold":
		return value < lowerBound || value > upperBound
	default:
		return false
	}
}
