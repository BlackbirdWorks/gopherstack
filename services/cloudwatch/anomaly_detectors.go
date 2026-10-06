package cloudwatch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// anomalyDetectorKey returns a stable map key for an anomaly detector.
// Dimensions are included so different dimension sets produce distinct detectors.
func anomalyDetectorKey(namespace, metricName, stat string, dims []Dimension) string {
	return namespace + "/" + metricName + "/" + stat + "/" + dimensionSetKey(dims)
}

// detectorKey keys metric-math detectors by their queries and single-metric ones by metric identity.
func detectorKey(d *AnomalyDetector) string {
	if len(d.MetricMath) == 0 {
		return anomalyDetectorKey(d.Namespace, d.MetricName, d.Stat, d.Dimensions)
	}

	raw, err := json.Marshal(d.MetricMath)
	if err != nil {
		return "math/" + d.ID
	}

	sum := sha256.Sum256(raw)

	return "math/" + hex.EncodeToString(sum[:])
}

// DeleteAnomalyDetectorByID removes the detector with the given AnomalyDetectorId.
func (b *InMemoryBackend) DeleteAnomalyDetectorByID(id string) error {
	b.mu.Lock("DeleteAnomalyDetectorByID")
	defer b.mu.Unlock()

	for _, d := range b.anomalyDetectors.All() {
		if d.ID == id {
			b.anomalyDetectors.Delete(detectorKey(d))

			return nil
		}
	}

	return fmt.Errorf("%w: anomaly detector %s", ErrAnomalyDetectorNotFound, id)
}

// DeleteMetricMathAnomalyDetector removes the metric-math detector defined by queries.
func (b *InMemoryBackend) DeleteMetricMathAnomalyDetector(queries []MetricDataQuery) error {
	b.mu.Lock("DeleteMetricMathAnomalyDetector")
	defer b.mu.Unlock()

	key := detectorKey(&AnomalyDetector{MetricMath: queries})
	if !b.anomalyDetectors.Has(key) {
		return fmt.Errorf("%w: metric math anomaly detector", ErrAnomalyDetectorNotFound)
	}

	b.anomalyDetectors.Delete(key)

	return nil
}

// DeleteAnomalyDetector removes an anomaly detector.
// Returns ErrAnomalyDetectorNotFound if the detector does not exist.
func (b *InMemoryBackend) DeleteAnomalyDetector(namespace, metricName, stat string, dims []Dimension) error {
	b.mu.Lock("DeleteAnomalyDetector")
	defer b.mu.Unlock()

	key := anomalyDetectorKey(namespace, metricName, stat, dims)

	if !b.anomalyDetectors.Has(key) {
		return fmt.Errorf("%w: %s/%s/%s", ErrAnomalyDetectorNotFound, namespace, metricName, stat)
	}

	b.anomalyDetectors.Delete(key)

	return nil
}

// PutAnomalyDetectorInternal creates or updates an anomaly detector (used for test seeding).
// The AnomalyDetectorId is generated once on first creation and preserved across
// updates to the same namespace/metric/stat/dimensions combination, then mirrored
// back onto detector so callers (e.g. cborPutAnomalyDetector) can read it after
// the call returns without a follow-up lookup.
func (b *InMemoryBackend) PutAnomalyDetectorInternal(detector *AnomalyDetector) {
	b.mu.Lock("PutAnomalyDetectorInternal")
	defer b.mu.Unlock()

	key := detectorKey(detector)
	if existing, ok := b.anomalyDetectors.Get(key); ok && existing.ID != "" {
		detector.ID = existing.ID
	} else if detector.ID == "" {
		detector.ID = uuid.New().String()
	}

	cp := *detector
	if cp.StateValue == "" {
		// TRAINED_INSUFFICIENT_DATA is the realistic initial state for a new detector.
		cp.StateValue = statusTrainedInsufficient
	}

	b.anomalyDetectors.Put(&cp)
}

// AnomalyDetectorFilter holds the DescribeAnomalyDetectors request filters.
type AnomalyDetectorFilter struct {
	Namespace  string
	MetricName string
	IDs        []string
	Types      []string
	Dimensions []Dimension
}

// DescribeAnomalyDetectors returns a filtered, paginated list of anomaly detectors.
func (b *InMemoryBackend) DescribeAnomalyDetectors(
	namespace, metricName, nextToken string,
	maxResults int,
) (page.Page[AnomalyDetector], error) {
	return b.DescribeAnomalyDetectorsFiltered(
		AnomalyDetectorFilter{Namespace: namespace, MetricName: metricName}, nextToken, maxResults,
	)
}

// matches reports whether d passes the filter; IDs, when set, replace every other filter.
func (f AnomalyDetectorFilter) matches(d *AnomalyDetector) bool {
	if len(f.IDs) > 0 {
		return slices.Contains(f.IDs, d.ID)
	}

	isMath := len(d.MetricMath) > 0
	if isMath && !slices.Contains(f.Types, anomalyTypeMetricMath) {
		return false
	}

	if !isMath && len(f.Types) > 0 && !slices.Contains(f.Types, anomalyTypeSingleMetric) {
		return false
	}

	if len(f.Dimensions) > 0 && dimensionSetKey(d.Dimensions) != dimensionSetKey(f.Dimensions) {
		return false
	}

	return (f.Namespace == "" || d.Namespace == f.Namespace) && (f.MetricName == "" || d.MetricName == f.MetricName)
}

// DescribeAnomalyDetectorsFiltered applies Ids, Types, Dimensions, Namespace and MetricName filters.
func (b *InMemoryBackend) DescribeAnomalyDetectorsFiltered(
	f AnomalyDetectorFilter,
	nextToken string,
	maxResults int,
) (page.Page[AnomalyDetector], error) {
	b.mu.RLock("DescribeAnomalyDetectors")
	defer b.mu.RUnlock()

	type entry struct {
		key      string
		detector AnomalyDetector
	}

	var entries []entry

	for _, d := range b.anomalyDetectors.All() {
		if !f.matches(d) {
			continue
		}

		k := detectorKey(d)
		entries = append(entries, entry{key: k, detector: *d})
	}

	// Sort by pre-computed key (namespace/metricName/stat) for deterministic output.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	result := make([]AnomalyDetector, 0, len(entries))
	for _, e := range entries {
		result = append(result, e.detector)
	}

	return page.New(result, nextToken, maxResults, cwDefaultDescribeAnomalyDetectorLimit), nil
}

const (
	anomalyTypeSingleMetric = "SINGLE_METRIC"
	anomalyTypeMetricMath   = "METRIC_MATH"
)

func (b *InMemoryBackend) PutAnomalyDetector(detector *AnomalyDetector) error {
	if len(detector.MetricMath) == 0 && (detector.Namespace == "" || detector.MetricName == "") {
		return fmt.Errorf("%w: Namespace and MetricName are required", ErrValidation)
	}

	b.PutAnomalyDetectorInternal(detector)

	return nil
}

// anomalyBandWidth returns the half-width of an anomaly band given stddev and an
// optional detector configuration band width. Default is 2 standard deviations.
const defaultAnomalyBandStdDevs = 2.0

// computeAnomalyBand calculates an anomaly band (lower, upper) using rolling
// Z-score statistics over a window of data points.
// Returns (lower values, upper values) aligned to the input timestamps.
func computeAnomalyBand(values []float64) ([]float64, []float64) {
	if len(values) == 0 {
		return nil, nil
	}

	mean, stddev := rollingStats(values)
	lower := make([]float64, len(values))
	upper := make([]float64, len(values))
	halfWidth := defaultAnomalyBandStdDevs * stddev

	for i := range values {
		lower[i] = mean - halfWidth
		upper[i] = mean + halfWidth
	}

	return lower, upper
}

// rollingStats computes mean and population standard deviation of a float slice.
func rollingStats(vals []float64) (float64, float64) {
	if len(vals) == 0 {
		return 0, 0
	}

	sum := 0.0
	for _, v := range vals {
		sum += v
	}

	mean := sum / float64(len(vals))

	variance := 0.0
	for _, v := range vals {
		diff := v - mean
		variance += diff * diff
	}

	variance /= float64(len(vals))

	return mean, math.Sqrt(variance)
}

// evalAnomalyDetectionBand evaluates an ANOMALY_DETECTION_BAND(id, [stdDevs]) expression.
// Returns the MetricDataResult for the band (values are the upper bound; a companion
// lower-band result is computed separately by the caller).
// Returns ok=false if the expression does not match the ANOMALY_DETECTION_BAND pattern.
func evalAnomalyDetectionBand(
	expr string,
	resolved map[string]MetricDataResult,
) (MetricDataResult, MetricDataResult, bool) {
	upper := strings.ToUpper(strings.TrimSpace(expr))

	const prefix = "ANOMALY_DETECTION_BAND("

	if !strings.HasPrefix(upper, prefix) || !strings.HasSuffix(upper, ")") {
		return MetricDataResult{}, MetricDataResult{}, false
	}

	inner := strings.TrimSpace(expr[len(prefix) : len(expr)-1])
	refID, stdDevsStr, hasStdDevs := strings.Cut(inner, ",")
	refID = strings.TrimSpace(refID)

	nStdDevs := defaultAnomalyBandStdDevs
	if hasStdDevs {
		s := strings.TrimSpace(stdDevsStr)
		if v, err := parseFloat(s); err == nil && v > 0 {
			nStdDevs = v
		}
	}

	base, baseOK := resolved[refID]
	if !baseOK || len(base.Values) == 0 {
		return MetricDataResult{}, MetricDataResult{}, true
	}

	mean, stddev := rollingStats(base.Values)
	halfWidth := nStdDevs * stddev

	uVals := make([]float64, len(base.Values))
	lVals := make([]float64, len(base.Values))

	for i := range base.Values {
		uVals[i] = mean + halfWidth
		lVals[i] = mean - halfWidth
	}

	upperResult := MetricDataResult{
		Timestamps: base.Timestamps,
		Values:     uVals,
		StatusCode: metricDataStatusComplete,
	}
	lowerResult := MetricDataResult{
		Timestamps: base.Timestamps,
		Values:     lVals,
		StatusCode: metricDataStatusComplete,
	}

	return upperResult, lowerResult, true
}

// parseFloat is a thin wrapper for strconv.ParseFloat to avoid importing strconv in this file.
func parseFloat(s string) (float64, error) {
	var v float64
	_, err := fmt.Sscanf(s, "%f", &v)
	if err != nil {
		return 0, err
	}

	return v, nil
}
