package cloudwatch

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dimensionSetKey returns a stable string key for a slice of Dimensions,
// sorting by Name so that dim order in caller input does not affect the key.
// Uses a single strings.Builder to avoid intermediate slice + Join alloc (#60).
func dimensionSetKey(dims []Dimension) string {
	if len(dims) == 0 {
		return ""
	}

	sorted := make([]Dimension, len(dims))
	copy(sorted, dims)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	var b strings.Builder
	for i, d := range sorted {
		if i > 0 {
			b.WriteByte(',')
		}

		b.WriteString(d.Name)
		b.WriteByte('=')
		b.WriteString(d.Value)
	}

	return b.String()
}

// metricStorageKey returns the composite inner-map key for a metric series.
func metricStorageKey(metricName string, dims []Dimension) string {
	dk := dimensionSetKey(dims)
	if dk == "" {
		return metricName
	}

	return metricName + "#" + dk
}

// dimsContainAll returns true when all dimensions in filter are present with
// matching values in stored. An empty filter always matches. Stored may contain
// additional dimensions beyond those in filter (partial/subset match).
func dimsContainAll(stored, filter []Dimension) bool {
	if len(filter) == 0 {
		return true
	}
	if len(stored) < len(filter) {
		return false
	}
	storedMap := make(map[string]string, len(stored))
	for _, d := range stored {
		storedMap[d.Name] = d.Value
	}
	for _, d := range filter {
		if v, ok := storedMap[d.Name]; !ok || v != d.Value {
			return false
		}
	}

	return true
}

// dimsMatchListFilter is like dimsContainAll but supports name-only dimension filters:
// when a filter entry has an empty Value, it matches any stored dimension with that Name.
// This matches the AWS ListMetrics DimensionFilter behaviour.
func dimsMatchListFilter(stored, filter []Dimension) bool {
	if len(filter) == 0 {
		return true
	}
	if len(stored) < len(filter) {
		return false
	}
	storedMap := make(map[string]string, len(stored))
	for _, d := range stored {
		storedMap[d.Name] = d.Value
	}
	for _, d := range filter {
		v, ok := storedMap[d.Name]
		if !ok {
			return false
		}
		// Empty Value in filter = match any value (name-only filter).
		if d.Value != "" && v != d.Value {
			return false
		}
	}

	return true
}

// validatePutMetricDataBatch checks every datum in a PutMetricData request for
// shape/range validity and confirms the batch would not push the namespace or
// account past its distinct-time-series cap. It performs no mutation, so a
// failing batch leaves backend state untouched — real CloudWatch has no
// partial-success shape for PutMetricData (PutMetricDataOutput carries no
// fields besides the request ID), so the whole request must be validated
// before any of it is committed.
// Caller must hold b.mu (write lock).
func (b *InMemoryBackend) validatePutMetricDataBatch(namespace string, data []MetricDatum) error {
	existing := len(b.metrics[namespace])
	newKeys := make(map[string]bool)
	now := time.Now().UTC()

	for _, d := range data {
		if err := validateMetricDatum(d, now); err != nil {
			return err
		}

		if err := validateStorageResolution(d.StorageResolution); err != nil {
			return err
		}

		key := metricStorageKey(d.MetricName, d.Dimensions)
		if _, ok := b.metrics[namespace][key]; !ok {
			newKeys[key] = true
		}
	}

	if existing+len(newKeys) > cwMaxMetricNamesPerNamespace {
		return fmt.Errorf("%w: namespace metric series limit reached", ErrMetricSeriesLimitExceeded)
	}

	if b.countTotalMetrics()+len(newKeys) > cwMaxTotalMetricRecords {
		return fmt.Errorf("%w: global metric series limit reached", ErrMetricSeriesLimitExceeded)
	}

	return nil
}

// storeDatum stores an already-validated MetricDatum into the namespace map.
// Caller must hold b.mu (write lock) and must have already validated the full
// batch via validatePutMetricDataBatch.
//
// A Values/Counts datum carries no pre-aggregated Sum/SampleCount/Min/Max (unlike
// a StatisticSet, whose caller supplies them directly), so this is the single
// place that derives them from the raw array pair before the point is stored;
// every caller of PutMetricData — the form handler, the rpc-v2-cbor handler, and
// direct backend callers/tests — gets consistent aggregation this way.
func (b *InMemoryBackend) storeDatum(namespace string, d MetricDatum) {
	if d.HasValuesArray && len(d.Values) == len(d.Counts) {
		d.Sum, d.Count, d.Min, d.Max = aggregateValuesCounts(d.Values, d.Counts)
	}

	key := metricStorageKey(d.MetricName, d.Dimensions)
	rec, exists := b.metrics[namespace][key]

	if !exists {
		dims := make([]Dimension, len(d.Dimensions))
		copy(dims, d.Dimensions)
		rec = &metricRecord{MetricName: d.MetricName, Dimensions: dims}
		b.metrics[namespace][key] = rec
		b.totalMetrics++ // #60: maintain running total
	}

	rec.Points = append(rec.Points, d)

	// Cap data points: copy the tail into a fresh slice so the old backing
	// array (which may be 2× or larger after repeated appends) can be GC'd.
	if len(rec.Points) > cwMaxMetricDataPoints {
		fresh := make([]MetricDatum, cwMaxMetricDataPoints)
		copy(fresh, rec.Points[len(rec.Points)-cwMaxMetricDataPoints:])
		rec.Points = fresh
	}
}

// PutMetricData stores metric data points for the given namespace.
//
// Real CloudWatch has no partial-failure response for this operation: the
// request either succeeds in full or fails in full with a single API error
// (PutMetricDataOutput has no members other than the request ID). This
// validates the entire batch before storing any of it, matching that
// all-or-nothing contract.
func (b *InMemoryBackend) PutMetricData(
	namespace string,
	data []MetricDatum,
) error {
	if len(data) > cwMaxMetricDataPerRequest {
		return fmt.Errorf(
			"%w: PutMetricData accepts at most %d MetricDatum entries per request",
			ErrValidation,
			cwMaxMetricDataPerRequest,
		)
	}

	var matchingStreams []string

	err := func() error {
		b.mu.Lock("PutMetricData")
		defer b.mu.Unlock()

		if b.metrics[namespace] == nil {
			b.metrics[namespace] = make(map[string]*metricRecord)
		}

		if err := b.validatePutMetricDataBatch(namespace, data); err != nil {
			return err
		}

		for _, d := range data {
			d.Namespace = namespace
			b.storeDatum(namespace, d)
		}

		// Collect matching running stream names while holding the write lock; the
		// actual timestamp update happens in a second, shorter lock acquisition so
		// the main metrics write lock is not held during filter iteration.
		matchingStreams = b.matchingRunningStreamNames(namespace, data)

		return nil
	}()
	if err != nil {
		return err
	}

	// Update LastUpdateDate for matched streams outside the metrics write lock.
	if len(matchingStreams) > 0 {
		now := time.Now().UTC()
		b.mu.Lock("PutMetricData.streamDelivery")
		defer b.mu.Unlock()

		for _, name := range matchingStreams {
			if s, ok := b.metricStreams.Get(name); ok && s.State == metricStreamStateRunning {
				s.LastUpdateDate = now
			}
		}
	}

	return nil
}

// sweepCandidate is a snapshot of a metric series that contains at least one
// expired data point, captured under a read lock for out-of-lock filtering.
type sweepCandidate struct {
	ns, key string
	points  []MetricDatum
}

// sweepResult holds the alive-filtered point set for a single candidate series.
type sweepResult struct {
	ns, key string
	alive   []MetricDatum
}

// sweepScanCandidates snapshots metric series that contain at least one expired
// data point. Acquires and releases the read lock internally.
func (b *InMemoryBackend) sweepScanCandidates(cutoff time.Time) []sweepCandidate {
	b.mu.RLock("SweepExpiredMetrics.scan")
	defer b.mu.RUnlock()

	var candidates []sweepCandidate

	for ns, nsMap := range b.metrics {
		for key, rec := range nsMap {
			if hasExpiredPoint(rec.Points, cutoff) {
				pts := make([]MetricDatum, len(rec.Points))
				copy(pts, rec.Points)
				candidates = append(candidates, sweepCandidate{ns, key, pts})
			}
		}
	}

	return candidates
}

// hasExpiredPoint reports whether any point in pts is older than cutoff.
func hasExpiredPoint(pts []MetricDatum, cutoff time.Time) bool {
	for _, pt := range pts {
		if pt.Timestamp.Before(cutoff) {
			return true
		}
	}

	return false
}

// sweepApplyResults applies pre-computed alive sets under the write lock.
// Each series is re-filtered to account for points that may have arrived
// between the read-lock snapshot and the write-lock apply phase.
func (b *InMemoryBackend) sweepApplyResults(cutoff time.Time, results []sweepResult) {
	b.mu.Lock("SweepExpiredMetrics.apply")
	defer b.mu.Unlock()

	for _, r := range results {
		nsMap, ok := b.metrics[r.ns]
		if !ok {
			continue
		}

		rec, ok := nsMap[r.key]
		if !ok {
			continue
		}

		alive := filterAlivePoints(rec.Points, cutoff)
		if len(alive) == 0 {
			delete(nsMap, r.key)
			b.totalMetrics-- // #60: maintain running total
		} else {
			rec.Points = alive
		}

		if len(nsMap) == 0 {
			delete(b.metrics, r.ns)
		}
	}
}

// SweepExpiredMetrics removes metric data points older than cwMetricRetentionDays.
// It is intended to be called periodically (e.g., by a janitor goroutine).
//
// Uses a two-phase approach: snapshot candidate series under a read lock, then
// apply deletions under a write lock. This avoids holding the write lock during
// the full O(series × points) filter scan.
func (b *InMemoryBackend) SweepExpiredMetrics() {
	cutoff := time.Now().UTC().AddDate(0, 0, -cwMetricRetentionDays)

	candidates := b.sweepScanCandidates(cutoff)
	if len(candidates) == 0 {
		return
	}

	results := make([]sweepResult, 0, len(candidates))
	for _, c := range candidates {
		results = append(results, sweepResult{c.ns, c.key, filterAlivePoints(c.points, cutoff)})
	}

	b.sweepApplyResults(cutoff, results)
}

// filterAlivePoints returns the subset of pts whose Timestamp is not before cutoff.
// Data points may arrive out of order, so a linear scan is used rather than binary search.
// When more than half the points have expired a fresh backing slice is allocated so
// that the old (larger) array can be released to the GC.
func filterAlivePoints(pts []MetricDatum, cutoff time.Time) []MetricDatum {
	surviving := 0
	for _, p := range pts {
		if !p.Timestamp.Before(cutoff) {
			surviving++
		}
	}
	if surviving == 0 {
		return nil
	}
	// Allocate a fresh slice only when over half are expired to avoid retaining
	// the old backing array when memory savings would be significant.
	var alive []MetricDatum
	if surviving < len(pts)/2 {
		alive = make([]MetricDatum, 0, surviving)
	} else {
		alive = pts[:0]
	}
	for _, p := range pts {
		if !p.Timestamp.Before(cutoff) {
			alive = append(alive, p)
		}
	}

	return alive
}

// metricBucket holds aggregated data for a single time bucket.
type metricBucket struct {
	ts    time.Time
	unit  string
	sum   float64
	min   float64
	max   float64
	count float64
}

// populateBuckets groups metric data into period-aligned time buckets.
func populateBuckets(
	all []MetricDatum,
	startTime, endTime time.Time,
	period int32,
) map[int64]*metricBucket {
	buckets := make(map[int64]*metricBucket)

	for _, d := range all {
		if d.Timestamp.Before(startTime) || !d.Timestamp.Before(endTime) {
			continue
		}

		idx := d.Timestamp.Unix() / int64(period)
		if _, ok := buckets[idx]; !ok {
			buckets[idx] = &metricBucket{
				min: math.MaxFloat64,
				max: -math.MaxFloat64,
				ts:  time.Unix(idx*int64(period), 0).UTC(),
			}
		}

		bk := buckets[idx]
		bk.sum += d.Sum
		bk.count += d.Count

		if d.Min < bk.min {
			bk.min = d.Min
		}

		if d.Max > bk.max {
			bk.max = d.Max
		}

		if bk.unit == "" {
			bk.unit = d.Unit
		}
	}

	return buckets
}

// buildDatapoint converts a bucket into a Datapoint with requested statistics.
func buildDatapoint(bk *metricBucket, statSet map[string]bool) Datapoint {
	dp := Datapoint{Timestamp: bk.ts, Unit: bk.unit}

	if statSet["Average"] {
		avg := bk.sum / bk.count
		dp.Average = &avg
	}

	if statSet[statSum] {
		s := bk.sum
		dp.Sum = &s
	}

	if statSet["Minimum"] {
		dp.Minimum = &bk.min
	}

	if statSet["Maximum"] {
		dp.Maximum = &bk.max
	}

	if statSet["SampleCount"] {
		dp.SampleCount = &bk.count
	}

	return dp
}

// GetMetricStatistics aggregates data for a metric over a time range into period-sized buckets.
// extendedStatistics supports percentile expressions such as "p99", "p95", "p50".
// dimensions filters to the specific metric series; an empty slice matches the dimensionless series.
func (b *InMemoryBackend) GetMetricStatistics(
	namespace, metricName string,
	dimensions []Dimension,
	startTime, endTime time.Time,
	period int32,
	statistics []string,
	extendedStatistics []string,
) ([]Datapoint, error) {
	b.mu.RLock("GetMetricStatistics")
	defer b.mu.RUnlock()

	var all []MetricDatum
	if nsMap, ok := b.metrics[namespace]; ok {
		key := metricStorageKey(metricName, dimensions)
		if rec, found := nsMap[key]; found {
			all = rec.Points
		}
	}

	buckets := populateBuckets(all, startTime, endTime, period)

	statSet := make(map[string]bool, len(statistics))
	for _, s := range statistics {
		statSet[s] = true
	}

	// Build raw-values map per bucket for percentile computation.
	var rawBuckets map[int64][]float64
	if len(extendedStatistics) > 0 {
		rawBuckets = collectRawBuckets(all, startTime, endTime, period)
	}

	datapoints := make([]Datapoint, 0, len(buckets))
	for idx, bk := range buckets {
		if bk.count == 0 {
			continue
		}

		dp := buildDatapoint(bk, statSet)

		if len(extendedStatistics) > 0 {
			raw := rawBuckets[idx]
			sort.Float64s(raw)
			dp.ExtendedStatistics = computeExtendedStats(raw, extendedStatistics)
		}

		datapoints = append(datapoints, dp)
	}

	sort.Slice(datapoints, func(i, j int) bool {
		return datapoints[i].Timestamp.Before(datapoints[j].Timestamp)
	})

	// Annotate datapoints with anomaly band if a detector exists for this metric.
	b.annotateAnomalyBand(namespace, metricName, dimensions, datapoints)

	return datapoints, nil
}

// annotateAnomalyBand adds BandLower/BandUpper to each Datapoint when a matching
// AnomalyDetector exists. Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) annotateAnomalyBand(
	namespace, metricName string,
	dimensions []Dimension,
	datapoints []Datapoint,
) {
	if len(datapoints) == 0 {
		return
	}

	// Look for a detector matching this metric (any stat matches since band is stat-agnostic).
	var matchedDetector *AnomalyDetector

	for _, d := range b.anomalyDetectors.All() {
		if d.Namespace != namespace || d.MetricName != metricName {
			continue
		}

		// Dimension match: stored detector must have a subset of the request dimensions.
		if !dimsContainAll(dimensions, d.Dimensions) {
			continue
		}

		matchedDetector = d

		break
	}

	if matchedDetector == nil {
		return
	}

	// Compute band from ALL stored historical raw points (training data).
	// Using stored raw points avoids the outlier-inflates-its-own-band problem
	// that occurs when computing from the current eval-window aggregates.
	var histVals []float64

	if nsMap, ok := b.metrics[namespace]; ok {
		key := metricStorageKey(metricName, dimensions)
		if rec, found := nsMap[key]; found {
			histVals = make([]float64, 0, len(rec.Points))
			for _, pt := range rec.Points {
				histVals = append(histVals, pt.Value)
			}
		}
	}

	if len(histVals) == 0 {
		return
	}

	bandWidth := matchedDetector.BandWidth
	if bandWidth <= 0 {
		bandWidth = defaultAnomalyBandStdDevs
	}

	mean, stddev := rollingStats(histVals)
	halfWidth := bandWidth * stddev

	for i := range datapoints {
		lo := mean - halfWidth
		hi := mean + halfWidth
		datapoints[i].BandLower = &lo
		datapoints[i].BandUpper = &hi
	}
}

// statValue extracts a single float value from a Datapoint based on the requested statistic.
func statValue(dp Datapoint, stat string) float64 {
	switch stat {
	case statSum:
		if dp.Sum != nil {
			return *dp.Sum
		}
	case "Average":
		if dp.Average != nil {
			return *dp.Average
		}
	case "Minimum", "Min":
		if dp.Minimum != nil {
			return *dp.Minimum
		}
	case "Maximum", "Max":
		if dp.Maximum != nil {
			return *dp.Maximum
		}
	case "SampleCount":
		if dp.SampleCount != nil {
			return *dp.SampleCount
		}
	}

	return 0
}

// cwRecentlyActiveValue is the only value CloudWatch accepts for ListMetrics'
// RecentlyActive parameter, filtering results to metrics that have had data
// points published in the past three hours.
const cwRecentlyActiveValue = "PT3H"

// cwRecentlyActiveWindow is the lookback window RecentlyActive=PT3H applies.
const cwRecentlyActiveWindow = 3 * time.Hour

// recordHasRecentPoint reports whether rec has at least one datapoint with a
// timestamp within the last cwRecentlyActiveWindow of now. Caller must hold
// b.mu (read or write lock).
func recordHasRecentPoint(rec *metricRecord, now time.Time) bool {
	cutoff := now.Add(-cwRecentlyActiveWindow)
	for _, pt := range rec.Points {
		if !pt.Timestamp.Before(cutoff) {
			return true
		}
	}

	return false
}

// listMetricsFilter bundles the ListMetrics query filters so the namespace/record
// matching loop can be factored out of ListMetrics itself.
type listMetricsFilter struct {
	now                   time.Time
	namespace, metricName string
	recentlyActive        string
	dimensions            []Dimension
}

// matches reports whether rec (in namespace ns) satisfies the filter.
func (f listMetricsFilter) matches(rec *metricRecord) bool {
	if f.metricName != "" && rec.MetricName != f.metricName {
		return false
	}
	if !dimsMatchListFilter(rec.Dimensions, f.dimensions) {
		return false
	}
	if f.recentlyActive != "" && !recordHasRecentPoint(rec, f.now) {
		return false
	}

	return true
}

// matchingMetricsInNamespace returns the Metric entries in nsMap that satisfy
// the filter, or nil immediately when ns itself is excluded by the namespace filter.
func (f listMetricsFilter) matchingMetricsInNamespace(
	ns string,
	nsMap map[string]*metricRecord,
) []Metric {
	if f.namespace != "" && ns != f.namespace {
		return nil
	}

	var result []Metric

	for _, rec := range nsMap {
		if !f.matches(rec) {
			continue
		}

		dims := make([]Dimension, len(rec.Dimensions))
		copy(dims, rec.Dimensions)
		result = append(result, Metric{Namespace: ns, MetricName: rec.MetricName, Dimensions: dims})
	}

	return result
}

// ListMetrics returns a page of unique metrics matching optional namespace, metricName, and
// dimension filters. dimensions specifies an exact set that must match (all filter dims present
// with matching values and no extra dims in the stored record). recentlyActive, when set, must be
// "PT3H" (the only value CloudWatch documents) and restricts results to metrics that received a
// data point in the last 3 hours.
func (b *InMemoryBackend) ListMetrics(
	namespace, metricName string,
	dimensions []Dimension,
	recentlyActive, nextToken string,
	maxResults int,
) (page.Page[Metric], error) {
	if recentlyActive != "" && recentlyActive != cwRecentlyActiveValue {
		return page.Page[Metric]{}, fmt.Errorf(
			"%w: RecentlyActive must be %q, got %q", ErrValidation, cwRecentlyActiveValue, recentlyActive,
		)
	}

	b.mu.RLock("ListMetrics")
	defer b.mu.RUnlock()

	filter := listMetricsFilter{
		namespace: namespace, metricName: metricName,
		dimensions: dimensions, recentlyActive: recentlyActive,
		now: time.Now().UTC(),
	}

	var result []Metric
	for ns, nsMap := range b.metrics {
		result = append(result, filter.matchingMetricsInNamespace(ns, nsMap)...)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}
		if result[i].MetricName != result[j].MetricName {
			return result[i].MetricName < result[j].MetricName
		}

		return dimensionSetKey(result[i].Dimensions) < dimensionSetKey(result[j].Dimensions)
	})

	return page.New(result, nextToken, maxResults, cwDefaultListMetricsLimit), nil
}

// cwMaxValuesArraySize is the documented cap on unique entries in a MetricDatum's
// Values array (and, correspondingly, its Counts array).
const cwMaxValuesArraySize = 150

// metricValueExponent is the documented CloudWatch metric value range exponent:
// values must fall within -2^metricValueExponent to 2^metricValueExponent.
const metricValueExponent = 360

// metricValueBound is CloudWatch's documented acceptable range for any metric
// value: -2^360 to 2^360. NaN and +/-Infinity are rejected outright.
var metricValueBound = math.Ldexp(1, metricValueExponent) //nolint:gochecknoglobals // fixed AWS-documented constant

// validMetricValue reports whether v is a value CloudWatch would accept for
// Value, Sum, Min, Max, or an entry of the Values array.
func validMetricValue(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= -metricValueBound && v <= metricValueBound
}

// cwMetricTimestampPastWindow is the maximum age (relative to now) CloudWatch
// accepts for a PutMetricData datapoint's Timestamp before rejecting it with
// InvalidParameterValue.
const cwMetricTimestampPastWindow = 14 * 24 * time.Hour

// cwMetricTimestampFutureWindow is the maximum distance into the future
// (relative to now) CloudWatch accepts for a PutMetricData datapoint's
// Timestamp before rejecting it with InvalidParameterValue.
const cwMetricTimestampFutureWindow = 2 * time.Hour

// validMetricTimestamp reports whether ts falls within CloudWatch's documented
// PutMetricData acceptance window relative to now: no more than two weeks in
// the past, and no more than two hours in the future.
func validMetricTimestamp(ts, now time.Time) bool {
	earliest := now.Add(-cwMetricTimestampPastWindow)
	latest := now.Add(cwMetricTimestampFutureWindow)

	return !ts.Before(earliest) && !ts.After(latest)
}

// validateMetricDatum enforces the shape and range rules AWS applies to a single
// PutMetricData MetricDatum entry:
//   - Value, StatisticValues, and the Values/Counts array are mutually exclusive
//     (InvalidParameterCombination)
//   - a Values array carries at most 150 entries, and Counts (when supplied) must
//     have the same length (InvalidParameterValue)
//   - every numeric value (Value, Sum, Min, Max, or a Values entry) must be
//     finite and within +/-2^360 (InvalidParameterValue)
//   - Timestamp must be no more than two weeks in the past and no more than two
//     hours in the future, relative to now (InvalidParameterValue)
//
// This must be called before the handler normalizes the datum (before
// Count/Sum/Min/Max are derived from Value for single-value points).
func validateMetricDatum(d MetricDatum, now time.Time) error {
	if !validMetricTimestamp(d.Timestamp, now) {
		return fmt.Errorf("%w: MetricName=%s", ErrMetricTimestampOutOfRange, d.MetricName)
	}

	if datumShapeCount(d) > 1 {
		return fmt.Errorf("%w: MetricName=%s", ErrValueAndStatisticSet, d.MetricName)
	}

	if d.HasValuesArray {
		return validateValuesArray(d)
	}

	if d.HasValue && !validMetricValue(d.Value) {
		return fmt.Errorf("%w: MetricName=%s", ErrInvalidMetricValue, d.MetricName)
	}

	if d.HasStatisticSet {
		return validateStatisticSetRange(d)
	}

	return nil
}

// datumShapeCount counts how many of the three mutually-exclusive PutMetricData
// input shapes (Value, StatisticValues, Values/Counts) are present on d.
func datumShapeCount(d MetricDatum) int {
	shapes := 0
	if d.HasValue {
		shapes++
	}
	if d.HasStatisticSet {
		shapes++
	}
	if d.HasValuesArray {
		shapes++
	}

	return shapes
}

// validateValuesArray checks the size and per-entry range of a Values/Counts datum.
func validateValuesArray(d MetricDatum) error {
	if len(d.Values) > cwMaxValuesArraySize {
		return fmt.Errorf("%w: MetricName=%s", ErrTooManyValues, d.MetricName)
	}
	if len(d.Counts) != len(d.Values) {
		return fmt.Errorf("%w: MetricName=%s", ErrValuesCountsLengthMismatch, d.MetricName)
	}
	for _, v := range d.Values {
		if !validMetricValue(v) {
			return fmt.Errorf("%w: MetricName=%s", ErrInvalidMetricValue, d.MetricName)
		}
	}

	return nil
}

// validateStatisticSetRange checks that a StatisticValues datum's Sum/Min/Max
// are all within CloudWatch's acceptable metric value range.
func validateStatisticSetRange(d MetricDatum) error {
	for _, v := range []float64{d.Sum, d.Min, d.Max} {
		if !validMetricValue(v) {
			return fmt.Errorf("%w: MetricName=%s", ErrInvalidMetricValue, d.MetricName)
		}
	}

	return nil
}

// storageResolutionStandard is the standard (60-second) storage resolution.
const storageResolutionStandard int32 = 60

// storageResolutionHighRes is the high-resolution (1-second) storage resolution.
const storageResolutionHighRes int32 = 1

// validateStorageResolution returns an error when StorageResolution is not 1 or 60 (or 0 = default 60).
func validateStorageResolution(res int32) error {
	switch res {
	case 0, storageResolutionHighRes, storageResolutionStandard:
		return nil
	default:
		return fmt.Errorf("%w: StorageResolution must be %d or %d, got %d",
			ErrValidation, storageResolutionHighRes, storageResolutionStandard, res)
	}
}
