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

	// iqmLowerPct and iqmUpperPct are the percentile boundaries for the
	// interquartile mean (IQM) statistic.
	iqmLowerPct = 25.0
	iqmUpperPct = 75.0

	// statSetMinInterior is the smallest sample count that has interior points
	// (beyond Min and Max) when reconstructing a StatisticValues distribution.
	statSetMinInterior = 2
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

// reExtendedStat matches the AWS extended-statistic bounded functions:
// TM/TC/TS/WM/PR with a parenthesised argument. The inner argument (group 2) is
// split on the colon separately so single-value forms like "TM(90%)" can be told
// apart from lower-only forms like "TM(10%:)".
var reExtendedStat = regexp.MustCompile(`^(?i)(TM|TC|TS|WM|PR)\(([^)]*)\)$`)

// statBounds holds the resolved lower/upper cutoff for a bounded extended stat.
// percent is true when the raw bounds were percentile (%) forms; the resolved
// lo/hi are always absolute value thresholds. loSet/hiSet track whether each
// side was supplied.
type statBounds struct {
	lo, hi       float64
	loSet, hiSet bool
	percent      bool
}

// percentile computes the linearly-interpolated pct-th percentile of a sorted slice.
func percentile(sortedVals []float64, pct float64) float64 {
	n := len(sortedVals)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sortedVals[0]
	}

	idx := pct / percentHundred * float64(n-1)
	lo := int(idx)
	hi := lo + 1

	if hi >= n {
		return sortedVals[n-1]
	}

	frac := idx - float64(lo)

	return sortedVals[lo]*(1-frac) + sortedVals[hi]*frac
}

// computeExtendedStats resolves each requested extended statistic string against a
// sorted value slice. Supported forms: pNN / pNN.N (percentile), TM/TC/TS/WM/PR
// with (x:y) bounds, and IQM (interquartile mean). Unrecognised stats are skipped.
func computeExtendedStats(sortedVals []float64, stats []string) map[string]float64 {
	out := make(map[string]float64, len(stats))
	if len(sortedVals) == 0 {
		return out
	}

	for _, s := range stats {
		if v, ok := computeExtendedStat(sortedVals, s); ok {
			out[s] = v
		}
	}

	return out
}

// computeExtendedStat computes a single extended statistic. Returns false when the
// stat string is not a recognised extended statistic.
func computeExtendedStat(sortedVals []float64, stat string) (float64, bool) {
	trimmed := strings.TrimSpace(stat)
	lower := strings.ToLower(trimmed)

	if lower == "iqm" {
		// Interquartile mean: trimmed mean between the 25th and 75th percentiles.
		return trimmedMean(sortedVals, statBounds{
			lo:    percentile(sortedVals, iqmLowerPct),
			hi:    percentile(sortedVals, iqmUpperPct),
			loSet: true,
			hiSet: true,
		}), true
	}

	if strings.HasPrefix(lower, "p") && !strings.HasPrefix(lower, "pr(") {
		pct, err := strconv.ParseFloat(lower[1:], 64)
		if err != nil || pct < 0 || pct > percentHundred {
			return 0, false
		}

		return percentile(sortedVals, pct), true
	}

	m := reExtendedStat.FindStringSubmatch(trimmed)
	if m == nil {
		return 0, false
	}

	fn := strings.ToUpper(m[1])
	bounds, ok := parseStatBounds(m[2], sortedVals)
	if !ok {
		return 0, false
	}

	switch fn {
	case "TM":
		return trimmedMean(sortedVals, bounds), true
	case "TC":
		return float64(trimmedCount(sortedVals, bounds)), true
	case "TS":
		return trimmedSum(sortedVals, bounds), true
	case "WM":
		return winsorizedMean(sortedVals, bounds), true
	case "PR":
		return percentileRank(sortedVals, bounds), true
	}

	return 0, false
}

// parseStatBounds resolves the "x:y" argument of a bounded extended statistic into
// absolute value thresholds. When the bounds use the percent form the cutoffs are
// resolved via percentile against the sorted data; absolute forms are used
// directly. A single value with no colon (e.g. "90%") is treated as the upper
// bound per AWS semantics.
func parseStatBounds(arg string, sortedVals []float64) (statBounds, bool) {
	arg = strings.TrimSpace(arg)

	var rawLo, rawHi string
	if lo, hi, hasColon := strings.Cut(arg, ":"); hasColon {
		rawLo, rawHi = strings.TrimSpace(lo), strings.TrimSpace(hi)
	} else {
		// Colonless single value maps to the upper bound.
		rawHi = arg
	}

	lo, loSet, loPct, ok1 := parseBound(rawLo)
	hi, hiSet, hiPct, ok2 := parseBound(rawHi)
	if !ok1 || !ok2 {
		return statBounds{}, false
	}

	// At least one bound must be present.
	if !loSet && !hiSet {
		return statBounds{}, false
	}

	percent := loPct || hiPct
	b := statBounds{loSet: loSet, hiSet: hiSet, percent: percent}

	if percent {
		if loSet {
			b.lo = percentile(sortedVals, lo)
		}
		if hiSet {
			b.hi = percentile(sortedVals, hi)
		}
	} else {
		b.lo, b.hi = lo, hi
	}

	return b, true
}

// parseBound parses a single bound token ("", "90", "90%"). It returns the numeric
// value, whether the bound was set, whether it used the percent form, and whether
// the token was well-formed.
func parseBound(tok string) (float64, bool, bool, bool) {
	if tok == "" {
		return 0, false, false, true
	}

	percent := strings.HasSuffix(tok, "%")
	numStr := strings.TrimSuffix(tok, "%")
	if numStr == "" {
		return 0, false, false, false
	}

	v, err := strconv.ParseFloat(numStr, 64)
	if err != nil || v < 0 {
		return 0, false, false, false
	}
	if percent && v > percentHundred {
		return 0, false, false, false
	}

	return v, true, percent, true
}

// inBounds reports whether v falls within the (inclusive) resolved cutoffs.
func (b statBounds) inBounds(v float64) bool {
	if b.loSet && v < b.lo {
		return false
	}
	if b.hiSet && v > b.hi {
		return false
	}

	return true
}

// trimmedValues returns the subset of sortedVals that fall within the bounds.
func trimmedValues(sortedVals []float64, b statBounds) []float64 {
	out := make([]float64, 0, len(sortedVals))
	for _, v := range sortedVals {
		if b.inBounds(v) {
			out = append(out, v)
		}
	}

	return out
}

// trimmedMean is the mean of the values inside the bounds (TM / IQM).
func trimmedMean(sortedVals []float64, b statBounds) float64 {
	vals := trimmedValues(sortedVals, b)
	if len(vals) == 0 {
		return 0
	}

	var sum float64
	for _, v := range vals {
		sum += v
	}

	return sum / float64(len(vals))
}

// trimmedCount is the number of values inside the bounds (TC).
func trimmedCount(sortedVals []float64, b statBounds) int {
	return len(trimmedValues(sortedVals, b))
}

// trimmedSum is the sum of the values inside the bounds (TS).
func trimmedSum(sortedVals []float64, b statBounds) float64 {
	var sum float64
	for _, v := range trimmedValues(sortedVals, b) {
		sum += v
	}

	return sum
}

// winsorizedMean clamps out-of-bound values to the nearest cutoff (rather than
// discarding them) and returns the mean over all points (WM).
func winsorizedMean(sortedVals []float64, b statBounds) float64 {
	if len(sortedVals) == 0 {
		return 0
	}

	var sum float64
	for _, v := range sortedVals {
		switch {
		case b.loSet && v < b.lo:
			sum += b.lo
		case b.hiSet && v > b.hi:
			sum += b.hi
		default:
			sum += v
		}
	}

	return sum / float64(len(sortedVals))
}

// percentileRank returns the percentage of data points whose value falls within
// the (absolute) bounds (PR). PR always uses absolute value thresholds.
func percentileRank(sortedVals []float64, b statBounds) float64 {
	if len(sortedVals) == 0 {
		return 0
	}

	count := 0
	for _, v := range sortedVals {
		if b.inBounds(v) {
			count++
		}
	}

	return float64(count) / float64(len(sortedVals)) * percentHundred
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

// maxStatSetExpand caps the number of synthetic samples produced when expanding a
// single StatisticValues datum, bounding memory for pathologically large counts.
const maxStatSetExpand = 100000

// collectRawBuckets groups raw metric values into period-aligned buckets. Datums
// carrying a StatisticValues set are expanded into a synthetic sample distribution
// (see expandDatumValues) so percentile and extended statistics remain meaningful
// instead of collapsing the set to a single point.
func collectRawBuckets(all []MetricDatum, startTime, endTime time.Time, period int32) map[int64][]float64 {
	buckets := make(map[int64][]float64)

	for _, d := range all {
		if d.Timestamp.Before(startTime) || !d.Timestamp.Before(endTime) {
			continue
		}

		idx := d.Timestamp.Unix() / int64(period)
		buckets[idx] = append(buckets[idx], expandDatumValues(d)...)
	}

	return buckets
}

// expandDatumValues reconstructs a sample distribution for a metric datum. A
// plain datum contributes its single Value. A Values/Counts datum is expanded
// exactly (each value repeated its count times, proportionally scaled down
// under maxStatSetExpand) since the real per-value distribution is known. A
// StatisticValues datum (SampleCount/Sum/Min/Max) is expanded into SampleCount
// synthetic samples: one at Min, one at Max, and the remainder at the residual
// mean so that the reconstructed set preserves the datum's count, sum,
// minimum, and maximum exactly. The sample count is capped at maxStatSetExpand
// to bound memory.
func expandDatumValues(d MetricDatum) []float64 {
	if d.HasValuesArray {
		return expandValuesCounts(d.Values, d.Counts)
	}

	if !d.HasStatisticSet {
		return []float64{d.Value}
	}

	n := int(d.Count)
	if n <= 0 {
		return nil
	}

	if n == 1 {
		return []float64{d.Sum}
	}

	if n == statSetMinInterior {
		return []float64{d.Min, d.Max}
	}

	capped := min(n, maxStatSetExpand)

	// Residual mean for the interior points keeps the total sum exact for the true
	// count; when capping is applied the proportions (and thus percentiles) are
	// preserved even though the absolute count is reduced.
	mid := (d.Sum - d.Min - d.Max) / float64(n-statSetMinInterior)

	vals := make([]float64, 0, capped)
	vals = append(vals, d.Min)
	for range capped - statSetMinInterior {
		vals = append(vals, mid)
	}
	vals = append(vals, d.Max)

	return vals
}

// expandValuesCounts reconstructs raw samples from a PutMetricData Values/Counts
// array pair: each values[i] occurred counts[i] times. When the total
// occurrence count is within maxStatSetExpand, expansion is exact. Otherwise
// each value's count is scaled down proportionally (rounded, minimum 1 for any
// value with a positive original count) so the relative frequencies — and
// therefore percentiles computed over the result — are preserved.
func expandValuesCounts(values, counts []float64) []float64 {
	total := 0.0
	for _, c := range counts {
		total += c
	}

	if total <= 0 {
		return nil
	}

	scale := 1.0
	capacity := int(total)

	if total > maxStatSetExpand {
		scale = float64(maxStatSetExpand) / total
		capacity = maxStatSetExpand
	}

	vals := make([]float64, 0, capacity)

	for i, v := range values {
		n := int(math.Round(counts[i] * scale))
		if n < 1 && counts[i] > 0 {
			n = 1
		}

		for range n {
			vals = append(vals, v)
		}
	}

	return vals
}
