package cloudwatch

import (
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	percentHundred = 100.0
	fillArgCount   = 2
)

// metricTimeSeries maps timestamp (unix seconds) to value for a single metric result.
type metricTimeSeries map[int64]float64

// buildTimeSeries converts parallel Timestamps/Values slices into a map.
func buildTimeSeries(r MetricDataResult) metricTimeSeries {
	ts := make(metricTimeSeries, len(r.Timestamps))
	for i, t := range r.Timestamps {
		ts[t.Unix()] = r.Values[i]
	}

	return ts
}

// mergedKeys returns the sorted union of all timestamp keys across the given series.
func mergedKeys(series ...metricTimeSeries) []int64 {
	seen := make(map[int64]bool)
	for _, s := range series {
		for k := range s {
			seen[k] = true
		}
	}

	keys := make([]int64, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

// timeSeriesResult converts a time-series map into parallel slices.
func timeSeriesResult(ts metricTimeSeries) ([]time.Time, []float64) {
	keys := mergedKeys(ts)
	times := make([]time.Time, 0, len(keys))
	vals := make([]float64, 0, len(keys))

	for _, k := range keys {
		times = append(times, time.Unix(k, 0).UTC())
		vals = append(vals, ts[k])
	}

	return times, vals
}

// reID matches a bare metric ID reference like "m1" or "e2".
var reID = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

// evalExpression evaluates a CloudWatch metric math expression and returns a MetricDataResult.
// resolved maps query ID → already-computed MetricDataResult (non-expression queries first).
func evalExpression(q MetricDataQuery, resolved map[string]MetricDataResult) MetricDataResult {
	result := MetricDataResult{
		ID:         q.ID,
		Label:      q.Label,
		StatusCode: metricDataStatusComplete,
	}

	if result.Label == "" {
		result.Label = q.Expression
	}

	expr := strings.TrimSpace(q.Expression)
	upper := strings.ToUpper(expr)

	// ANOMALY_DETECTION_BAND returns the upper band; the lower band is stored in a synthetic key.
	if upperBand, _, ok := evalAnomalyDetectionBand(expr, resolved); ok {
		result.Timestamps = upperBand.Timestamps
		result.Values = upperBand.Values

		return result
	}

	if evalFillExpr(upper, resolved, &result) {
		return result
	}

	if evalMetricsAggExpr(upper, resolved, &result) {
		return result
	}

	if evalAvgExpr(upper, expr, resolved, &result) {
		return result
	}

	if evalRateExpr(upper, resolved, &result) {
		return result
	}

	if evalBinaryExpr(expr, resolved, &result) {
		return result
	}

	// Bare ID reference — just copy the resolved result.
	if reID.MatchString(expr) {
		if base, ok := resolved[expr]; ok {
			result.Timestamps = base.Timestamps
			result.Values = base.Values
		}
	}

	return result
}

// evalFillExpr handles FILL(id, value) expressions.
func evalFillExpr(upper string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	m := matchFill(upper)
	if m == nil {
		return false
	}

	base, ok := resolved[m.id]
	if !ok {
		return true
	}

	ts := buildTimeSeries(base)
	allSeries := make([]metricTimeSeries, 0, len(resolved))

	for _, r := range resolved {
		allSeries = append(allSeries, buildTimeSeries(r))
	}

	allKeys := mergedKeys(allSeries...)
	filled := make(metricTimeSeries, len(allKeys))

	for _, k := range allKeys {
		if v, ok2 := ts[k]; ok2 {
			filled[k] = v
		} else {
			filled[k] = m.fillVal
		}
	}

	result.Timestamps, result.Values = timeSeriesResult(filled)

	return true
}

// minStddevSampleCount is the minimum sample count for stddev computation.
const minStddevSampleCount = 2

// aggSum returns the sum of vals.
func aggSum(vals []float64) float64 {
	var s float64
	for _, v := range vals {
		s += v
	}

	return s
}

// aggAvg returns the average of vals.
func aggAvg(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}

	return aggSum(vals) / float64(len(vals))
}

// aggMin returns the minimum of vals.
func aggMin(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}

	return m
}

// aggMax returns the maximum of vals.
func aggMax(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}

	return m
}

// aggStddev returns the population standard deviation of vals.
func aggStddev(vals []float64) float64 {
	if len(vals) < minStddevSampleCount {
		return 0
	}
	mean := aggAvg(vals)
	var variance float64
	for _, v := range vals {
		d := v - mean
		variance += d * d
	}

	return math.Sqrt(variance / float64(len(vals)))
}

// metricsAggFn returns the aggregation function for the given METRICS() function name.
// Returns nil when fn is not recognised.
func metricsAggFn(fn string) func([]float64) float64 {
	switch fn {
	case "SUM":
		return aggSum
	case "AVG":
		return aggAvg
	case "MIN":
		return aggMin
	case "MAX":
		return aggMax
	case "STDDEV":
		return aggStddev
	}

	return nil
}

// evalMetricsAggExpr handles SUM/AVG/MIN/MAX/STDDEV(METRICS()) expressions.
func evalMetricsAggExpr(upper string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	for _, fn := range []string{"SUM", "AVG", "MIN", "MAX", "STDDEV"} {
		if upper != fn+"(METRICS())" {
			continue
		}
		agg := metricsAggFn(fn)
		if agg == nil {
			return false
		}
		result.Timestamps, result.Values = applyAggregation(resolved, agg)

		return true
	}

	return false
}

// evalAvgExpr handles AVG(id) expressions.
func evalAvgExpr(upper, expr string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	_ = expr
	arg := matchSingleArgFunc("AVG", upper)

	if arg == "" {
		return false
	}

	base, ok := resolved[arg]
	if !ok {
		return true
	}

	ts := buildTimeSeries(base)
	keys := mergedKeys(ts)

	var sum float64
	for _, v := range ts {
		sum += v
	}

	avg := 0.0
	if len(ts) > 0 {
		avg = sum / float64(len(ts))
	}

	out := make(metricTimeSeries, len(keys))
	for _, k := range keys {
		out[k] = avg
	}

	result.Timestamps, result.Values = timeSeriesResult(out)

	return true
}

// fillMatch holds the parsed components of a FILL() expression.
type fillMatch struct {
	id      string
	fillVal float64
}

// matchFill returns fillMatch when expr matches FILL(id, number) (case-insensitive).
func matchFill(upperExpr string) *fillMatch {
	if !strings.HasPrefix(upperExpr, "FILL(") || !strings.HasSuffix(upperExpr, ")") {
		return nil
	}

	inner := upperExpr[5 : len(upperExpr)-1]
	parts := strings.SplitN(inner, ",", fillArgCount)

	if len(parts) != fillArgCount {
		return nil
	}

	id := strings.TrimSpace(parts[0])
	valStr := strings.TrimSpace(parts[1])

	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		if valStr == "0" || valStr == "ZERO" {
			val = 0
		} else {
			return nil
		}
	}

	return &fillMatch{id: strings.ToLower(id), fillVal: val}
}

// matchSingleArgFunc returns the argument ID if upper matches FUNC(id).
func matchSingleArgFunc(funcName, upperExpr string) string {
	prefix := funcName + "("
	if !strings.HasPrefix(upperExpr, prefix) || !strings.HasSuffix(upperExpr, ")") {
		return ""
	}

	return strings.ToLower(strings.TrimSpace(upperExpr[len(prefix) : len(upperExpr)-1]))
}

// applyAggregation applies an aggregation function across all resolved series per timestamp.
func applyAggregation(resolved map[string]MetricDataResult, agg func([]float64) float64) ([]time.Time, []float64) {
	allSeries := make([]metricTimeSeries, 0, len(resolved))
	for _, r := range resolved {
		allSeries = append(allSeries, buildTimeSeries(r))
	}

	keys := mergedKeys(allSeries...)
	times := make([]time.Time, 0, len(keys))
	vals := make([]float64, 0, len(keys))

	for _, k := range keys {
		var bucket []float64
		for _, s := range allSeries {
			if v, ok := s[k]; ok {
				bucket = append(bucket, v)
			}
		}

		times = append(times, time.Unix(k, 0).UTC())
		vals = append(vals, agg(bucket))
	}

	return times, vals
}

// reBinaryIDID matches "id OP id" binary expressions.
var reBinaryIDID = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_]*)\s*([+\-*/])\s*([a-zA-Z][a-zA-Z0-9_]*)$`)

// reBinaryIDConst matches "id OP number" binary expressions.
var reBinaryIDConst = regexp.MustCompile(
	`^([a-zA-Z][a-zA-Z0-9_]*)\s*([+\-*/])\s*([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)$`,
)

// reBinaryConstID matches "number OP id" binary expressions.
var reBinaryConstID = regexp.MustCompile(
	`^([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)\s*([+\-*/])\s*([a-zA-Z][a-zA-Z0-9_]*)$`,
)

// evalBinaryIDID evaluates "id OP id" expressions.
func evalBinaryIDID(m []string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	leftID, opChar, rightID := m[1], m[2][0], m[3]
	left, ok1 := resolved[leftID]
	right, ok2 := resolved[rightID]

	if !ok1 || !ok2 {
		return false
	}

	lts := buildTimeSeries(left)
	rts := buildTimeSeries(right)
	keys := mergedKeys(lts, rts)
	out := make(metricTimeSeries, len(keys))

	for _, k := range keys {
		lv, lok := lts[k]
		rv, rok := rts[k]
		if !lok || !rok {
			continue
		}
		out[k] = applyBinaryOp(opChar, lv, rv)
	}

	result.Timestamps, result.Values = timeSeriesResult(out)

	return true
}

// evalBinaryIDConst evaluates "id OP constant" expressions.
func evalBinaryIDConst(m []string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	id, opChar := m[1], m[2][0]
	constVal, err := strconv.ParseFloat(m[3], 64)
	if err != nil {
		return false
	}

	base, ok := resolved[id]
	if !ok {
		return false
	}

	ts := buildTimeSeries(base)
	out := make(metricTimeSeries, len(ts))

	for k, v := range ts {
		out[k] = applyBinaryOp(opChar, v, constVal)
	}

	result.Timestamps, result.Values = timeSeriesResult(out)

	return true
}

// evalBinaryConstID evaluates "constant OP id" expressions.
func evalBinaryConstID(m []string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	constVal, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return false
	}

	opChar, id := m[2][0], m[3]
	base, ok := resolved[id]

	if !ok {
		return false
	}

	ts := buildTimeSeries(base)
	out := make(metricTimeSeries, len(ts))

	for k, v := range ts {
		out[k] = applyBinaryOp(opChar, constVal, v)
	}

	result.Timestamps, result.Values = timeSeriesResult(out)

	return true
}

// evalBinaryExpr evaluates element-wise binary expressions like "m1 + m2",
// "m1 * 2", "100 / m1", or "2 * m1".
func evalBinaryExpr(expr string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	if m := reBinaryIDID.FindStringSubmatch(expr); m != nil {
		return evalBinaryIDID(m, resolved, result)
	}

	if m := reBinaryIDConst.FindStringSubmatch(expr); m != nil {
		return evalBinaryIDConst(m, resolved, result)
	}

	if m := reBinaryConstID.FindStringSubmatch(expr); m != nil {
		return evalBinaryConstID(m, resolved, result)
	}

	return false
}

// applyBinaryOp computes lv OP rv for a single arithmetic operator.
func applyBinaryOp(op byte, lv, rv float64) float64 {
	switch op {
	case '+':
		return lv + rv
	case '-':
		return lv - rv
	case '*':
		return lv * rv
	case '/':
		if rv != 0 {
			return lv / rv
		}

		return math.NaN()
	}

	return 0
}

// computePercentiles computes percentile values from a sorted float64 slice.
// stat strings are expected in the form "p99", "p95.5", etc.
func computePercentiles(sortedVals []float64, stats []string) map[string]float64 {
	out := make(map[string]float64, len(stats))
	n := len(sortedVals)

	if n == 0 {
		return out
	}

	for _, s := range stats {
		lower := strings.ToLower(s)
		if !strings.HasPrefix(lower, "p") {
			continue
		}

		pct, err := strconv.ParseFloat(lower[1:], 64)
		if err != nil || pct < 0 || pct > percentHundred {
			continue
		}

		idx := pct / percentHundred * float64(n-1)
		lo := int(idx)
		hi := lo + 1

		if hi >= n {
			out[s] = sortedVals[n-1]
		} else {
			frac := idx - float64(lo)
			out[s] = sortedVals[lo]*(1-frac) + sortedVals[hi]*frac
		}
	}

	return out
}

// evalRateExpr handles RATE(id) expressions — per-second rate of change between consecutive points.
func evalRateExpr(upper string, resolved map[string]MetricDataResult, result *MetricDataResult) bool {
	arg := matchSingleArgFunc("RATE", upper)
	if arg == "" {
		return false
	}

	base, ok := resolved[arg]
	if !ok || len(base.Timestamps) < 2 {
		return true
	}

	n := len(base.Timestamps)
	times := make([]time.Time, 0, n-1)
	vals := make([]float64, 0, n-1)

	for i := 1; i < n; i++ {
		dt := base.Timestamps[i].Sub(base.Timestamps[i-1]).Seconds()
		if dt <= 0 {
			continue
		}
		dv := base.Values[i] - base.Values[i-1]
		times = append(times, base.Timestamps[i])
		vals = append(vals, dv/dt)
	}

	result.Timestamps = times
	result.Values = vals

	return true
}

// collectRawBuckets groups raw metric values into period-aligned buckets.
func collectRawBuckets(all []MetricDatum, startTime, endTime time.Time, period int32) map[int64][]float64 {
	buckets := make(map[int64][]float64)

	for _, d := range all {
		if d.Timestamp.Before(startTime) || !d.Timestamp.Before(endTime) {
			continue
		}

		idx := d.Timestamp.Unix() / int64(period)
		buckets[idx] = append(buckets[idx], d.Value)
	}

	return buckets
}
