package cloudwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

const (
	statusTrainedInsufficient = "TRAINED_INSUFFICIENT_DATA"
	metricDataStatusComplete  = "Complete"
)

const (
	errResourceNotFoundException = "ResourceNotFoundException"
)

const (
	keyAlarmName        = "AlarmName"
	keyAlarmDescription = "AlarmDescription"
	keyAlarmArn         = "AlarmArn"
)

// ErrAlarmNotFound is returned when a requested alarm does not exist.
var ErrAlarmNotFound = errors.New(errResourceNotFoundException)

// ErrAlarmNameRequired is returned when an alarm name is missing.
var ErrAlarmNameRequired = errors.New("AlarmName is required")

// ErrAlarmRuleRequired is returned when a composite alarm rule is missing.
var ErrAlarmRuleRequired = errors.New("AlarmRule is required")

// ErrDashboardNotFound is returned when a requested dashboard does not exist.
var ErrDashboardNotFound = errors.New(errResourceNotFoundException)

// ErrDashboardNameRequired is returned when a dashboard name is missing.
var ErrDashboardNameRequired = errors.New("DashboardName is required")

// ErrAlarmMuteRuleNotFound is returned when a requested alarm mute rule does not exist.
var ErrAlarmMuteRuleNotFound = errors.New(errResourceNotFoundException)

// ErrAnomalyDetectorNotFound is returned when a requested anomaly detector does not exist.
var ErrAnomalyDetectorNotFound = errors.New(errResourceNotFoundException)

// ErrMetricStreamNotFound is returned when a requested metric stream does not exist.
var ErrMetricStreamNotFound = errors.New(errResourceNotFoundException)

// ErrInsightRuleNotFound is returned when a requested insight rule does not exist.
var ErrInsightRuleNotFound = errors.New(errResourceNotFoundException)

// ErrMetricFilterNotFound is returned when a requested metric filter does not exist.
var ErrMetricFilterNotFound = errors.New(errResourceNotFoundException)

// ErrValidation is returned when a caller provides an invalid or missing parameter.
var ErrValidation = errors.New("InvalidParameterValue")

const (
	cwDefaultListMetricsLimit               = 500
	cwDefaultDescribeAlarmsLimit            = 100
	cwDefaultAlarmHistoryLimit              = 100
	cwDefaultDescribeForMetricLimit         = 100
	cwDefaultListDashboardsLimit            = 300
	cwDefaultDescribeAnomalyDetectorLimit   = 100
	cwDefaultDescribeInsightRulesLimit      = 100
	cwDefaultDescribeAlarmContributorsLimit = 100
	cwDefaultListMetricStreamsLimit         = 500
	cwDefaultDescribeMetricFiltersLimit     = 50
	cwMaxMetricDataPoints                   = 1000 // maximum data points retained per metric
	cwMaxMetricNamesPerNamespace            = 500  // maximum unique metric names per namespace
	cwMaxAlarmHistory                       = 100  // maximum alarm history entries per alarm
	cwMetricRetentionDays                   = 15   // data points older than this are evicted
	cwMaxCompositeEvalDepth                 = 10   // maximum recursion depth for composite alarm evaluation
	// cwMaxMetricDataPerRequest mirrors the AWS CloudWatch PutMetricData hard
	// limit on the number of MetricDatum entries accepted per request (1000).
	cwMaxMetricDataPerRequest = 1000
	// cwMaxTotalMetricRecords is a cluster-wide safety cap on distinct metric time series.
	cwMaxTotalMetricRecords = 10000

	alarmStateAlarm            = "ALARM"
	alarmStateOK               = "OK"
	alarmStateInsufficientData = "INSUFFICIENT_DATA"

	historyTypeStateUpdate         = "StateUpdate"
	historyTypeConfigurationUpdate = "ConfigurationUpdate"
	historyTypeAction              = "Action"
)

// SNSPublisher can publish a message to an SNS topic by ARN.
type SNSPublisher interface {
	PublishToTopic(topicARN, message string) error
}

// LambdaInvoker can invoke a Lambda function by ARN or name.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
}

// StorageBackend is the interface for the CloudWatch in-memory store.
type StorageBackend interface {
	PutMetricData(namespace string, data []MetricDatum) ([]UnprocessedMetricDatum, error)
	GetMetricStatistics(
		namespace, metricName string,
		dimensions []Dimension,
		startTime, endTime time.Time,
		period int32,
		statistics []string,
		extendedStatistics []string,
	) ([]Datapoint, error)
	GetMetricData(queries []MetricDataQuery, startTime, endTime time.Time) ([]MetricDataResult, error)
	ListMetrics(
		namespace, metricName string,
		dimensions []Dimension,
		nextToken string,
		maxResults int,
	) (page.Page[Metric], error)
	PutMetricAlarm(alarm *MetricAlarm) error
	PutCompositeAlarm(alarm *CompositeAlarm) error
	DescribeAlarms(
		alarmNames []string,
		alarmTypes []string,
		alarmNamePrefix, stateValue, nextToken string,
		maxRecords int,
	) (page.Page[MetricAlarm], page.Page[CompositeAlarm], error)
	DescribeAlarmsForMetric(
		namespace, metricName string,
		dimensions []Dimension,
		alarmNames []string,
		nextToken string,
		maxRecords int,
	) (page.Page[MetricAlarm], error)
	DescribeAlarmHistory(
		alarmName, alarmType, historyItemType, nextToken string,
		startDate, endDate time.Time,
		maxRecords int,
	) (page.Page[AlarmHistoryItem], error)
	DeleteAlarms(alarmNames []string) error
	SetAlarmState(ctx context.Context, alarmName, stateValue, stateReason, stateReasonData string) error
	EnableAlarmActions(alarmNames []string) error
	DisableAlarmActions(alarmNames []string) error
	PutDashboard(name, body string) error
	GetDashboard(name string) (DashboardEntry, string, error)
	ListDashboards(prefix, nextToken string) (page.Page[DashboardEntry], error)
	DeleteDashboards(names []string) error
	PutAlarmMuteRule(rule *AlarmMuteRule) error
	DeleteAlarmMuteRule(muteName string) error
	GetAlarmMuteRule(muteName string) (*AlarmMuteRule, error)
	PutAnomalyDetector(detector *AnomalyDetector) error
	DeleteAnomalyDetector(namespace, metricName, stat string) error
	DescribeAnomalyDetectors(
		namespace, metricName, nextToken string,
		maxResults int,
	) (page.Page[AnomalyDetector], error)
	DeleteInsightRules(ruleNames []string) ([]InsightRuleFailure, error)
	PutInsightRule(rule *InsightRule) error
	GetInsightRule(name string) (*InsightRule, error)
	DescribeInsightRules(nextToken string, maxResults int) (page.Page[InsightRule], error)
	DisableInsightRules(ruleNames []string) ([]InsightRuleFailure, error)
	EnableInsightRules(ruleNames []string) ([]InsightRuleFailure, error)
	PutMetricStream(stream *MetricStream) error
	GetMetricStream(name string) (*MetricStream, error)
	ListMetricStreams(nextToken string, maxResults int) (page.Page[MetricStream], error)
	DeleteMetricStream(name string) error
	DescribeAlarmContributors(alarmName, nextToken string) (page.Page[AlarmContributor], error)
	PutMetricFilter(filter *MetricFilter) error
	DescribeMetricFilters(
		filterNamePrefix, logGroupName, nextToken string,
		maxResults int,
	) (page.Page[MetricFilter], error)
	DeleteMetricFilter(filterName, logGroupName string) error
	StartMetricStreams(names []string) error
	StopMetricStreams(names []string) error
}

// metricRecord holds time-series data for a single (MetricName, Dimensions) combination.
type metricRecord struct {
	MetricName string        `json:"MetricName"`
	Dimensions []Dimension   `json:"Dimensions,omitempty"`
	Points     []MetricDatum `json:"Points"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
// metrics is a two-level map: namespace -> composite-key -> *metricRecord.
// The composite key is produced by metricStorageKey(metricName, dims) so that
// different dimension sets for the same metric name are stored separately.
type InMemoryBackend struct {
	metrics          map[string]map[string]*metricRecord
	alarms           map[string]*MetricAlarm
	compositeAlarms  map[string]*CompositeAlarm
	alarmHistory     map[string][]AlarmHistoryItem
	dashboards       map[string]*dashboardRecord
	anomalyDetectors map[string]*AnomalyDetector
	insightRules     map[string]*InsightRule
	metricStreams    map[string]*MetricStream
	alarmMuteRules   map[string]*AlarmMuteRule
	metricFilters    map[string]*MetricFilter
	snsPublisher     SNSPublisher
	lambdaInvoker    LambdaInvoker
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
	// totalMetrics is the running count of distinct metric series across all
	// namespaces, maintained on insert/delete to avoid O(namespaces) walks (#60).
	totalMetrics int
}

// dashboardRecord holds dashboard body and metadata.
type dashboardRecord struct {
	LastModified time.Time `json:"LastModified"`
	Name         string    `json:"Name"`
	Body         string    `json:"Body"`
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:        accountID,
		region:           region,
		metrics:          make(map[string]map[string]*metricRecord),
		alarms:           make(map[string]*MetricAlarm),
		compositeAlarms:  make(map[string]*CompositeAlarm),
		alarmHistory:     make(map[string][]AlarmHistoryItem),
		dashboards:       make(map[string]*dashboardRecord),
		anomalyDetectors: make(map[string]*AnomalyDetector),
		insightRules:     make(map[string]*InsightRule),
		metricStreams:    make(map[string]*MetricStream),
		alarmMuteRules:   make(map[string]*AlarmMuteRule),
		metricFilters:    make(map[string]*MetricFilter),
		mu:               lockmetrics.New("cloudwatch"),
	}
}

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

// countTotalMetrics returns the total number of distinct metric time series
// across all namespaces. Uses the running counter (#60) maintained on insert.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) countTotalMetrics() int {
	return b.totalMetrics
}

// SetSNSPublisher registers an SNS publisher used to fire alarm action notifications.
func (b *InMemoryBackend) SetSNSPublisher(pub SNSPublisher) {
	b.mu.Lock("SetSNSPublisher")
	defer b.mu.Unlock()
	b.snsPublisher = pub
}

// SetLambdaInvoker registers a Lambda invoker used to fire alarm action Lambda invocations.
func (b *InMemoryBackend) SetLambdaInvoker(inv LambdaInvoker) {
	b.mu.Lock("SetLambdaInvoker")
	defer b.mu.Unlock()
	b.lambdaInvoker = inv
}

// PutMetricData stores metric data points for the given namespace.
// Returns a slice of UnprocessedMetricDatum for any entries that could not be stored.
func (b *InMemoryBackend) PutMetricData(namespace string, data []MetricDatum) ([]UnprocessedMetricDatum, error) {
	if len(data) > cwMaxMetricDataPerRequest {
		return nil, fmt.Errorf("%w: PutMetricData accepts at most %d MetricDatum entries per request",
			ErrValidation, cwMaxMetricDataPerRequest)
	}

	b.mu.Lock("PutMetricData")
	defer b.mu.Unlock()

	if b.metrics[namespace] == nil {
		b.metrics[namespace] = make(map[string]*metricRecord)
	}

	var unprocessed []UnprocessedMetricDatum

	for _, d := range data {
		d.Namespace = namespace

		// Reject entries that set both Value and StatisticSet.
		if err := validateMetricDatum(d); err != nil {
			unprocessed = append(unprocessed, UnprocessedMetricDatum{
				MetricName:   d.MetricName,
				ErrorCode:    "InvalidParameterCombination",
				ErrorMessage: err.Error(),
			})

			continue
		}

		// Validate StorageResolution is 1 or 60 (or 0 = default).
		if err := validateStorageResolution(d.StorageResolution); err != nil {
			unprocessed = append(unprocessed, UnprocessedMetricDatum{
				MetricName:   d.MetricName,
				ErrorCode:    "InvalidParameterValue",
				ErrorMessage: err.Error(),
			})

			continue
		}

		key := metricStorageKey(d.MetricName, d.Dimensions)
		rec, exists := b.metrics[namespace][key]

		if !exists {
			// Enforce namespace-level unique metric series limit.
			if len(b.metrics[namespace]) >= cwMaxMetricNamesPerNamespace {
				unprocessed = append(unprocessed, UnprocessedMetricDatum{
					MetricName:   d.MetricName,
					ErrorCode:    "LimitExceeded",
					ErrorMessage: "namespace metric series limit reached",
				})

				continue
			}
			// Enforce global metric series cap.
			if b.countTotalMetrics() >= cwMaxTotalMetricRecords {
				unprocessed = append(unprocessed, UnprocessedMetricDatum{
					MetricName:   d.MetricName,
					ErrorCode:    "LimitExceeded",
					ErrorMessage: "global metric series limit reached",
				})

				continue
			}
			dims := make([]Dimension, len(d.Dimensions))
			copy(dims, d.Dimensions)
			rec = &metricRecord{MetricName: d.MetricName, Dimensions: dims}
			b.metrics[namespace][key] = rec
			b.totalMetrics++ // #60: maintain running total
		}

		rec.Points = append(rec.Points, d)

		// Cap data points to prevent unbounded memory growth.
		if len(rec.Points) > cwMaxMetricDataPoints {
			rec.Points = rec.Points[len(rec.Points)-cwMaxMetricDataPoints:]
		}
	}

	// Record delivery to any running metric streams.
	b.recordStreamDelivery(namespace, data)

	return unprocessed, nil
}

// filterExcludesMetric returns true when an ExcludeFilters entry denies the metric.
func filterExcludesMetric(filters []MetricStreamFilter, namespace, metricName string) bool {
	for _, f := range filters {
		if f.Namespace != namespace {
			continue
		}
		if len(f.MetricNames) == 0 {
			return true
		}
		if filterNamesContain(f.MetricNames, metricName) {
			return true
		}
	}

	return false
}

// filterIncludesMetric returns true when at least one IncludeFilters entry allows the metric.
func filterIncludesMetric(filters []MetricStreamFilter, namespace, metricName string) bool {
	for _, f := range filters {
		if f.Namespace != namespace {
			continue
		}
		if len(f.MetricNames) == 0 {
			return true
		}
		if filterNamesContain(f.MetricNames, metricName) {
			return true
		}
	}

	return false
}

// filterNamesContain returns true when name is in the names list.
func filterNamesContain(names []string, name string) bool {
	return slices.Contains(names, name)
}

// streamAllowsMetric returns true when the given namespace/metricName passes the
// stream's IncludeFilters and ExcludeFilters. An empty IncludeFilters means "all
// namespaces allowed"; ExcludeFilters override IncludeFilters when both are set.
func streamAllowsMetric(s *MetricStream, namespace, metricName string) bool {
	if filterExcludesMetric(s.ExcludeFilters, namespace, metricName) {
		return false
	}
	if len(s.IncludeFilters) > 0 {
		return filterIncludesMetric(s.IncludeFilters, namespace, metricName)
	}

	return true
}

// recordStreamDelivery notes that data was delivered to running metric streams.
// This is a best-effort in-memory record; no actual Firehose call is made.
// Caller must hold b.mu (write lock).
func (b *InMemoryBackend) recordStreamDelivery(namespace string, data []MetricDatum) {
	for _, s := range b.metricStreams {
		if s.State != metricStreamStateRunning {
			continue
		}
		for _, d := range data {
			if streamAllowsMetric(s, namespace, d.MetricName) {
				s.LastUpdateDate = time.Now().UTC()

				break
			}
		}
	}
}

// SweepExpiredMetrics removes metric data points older than cwMetricRetentionDays.
// It is intended to be called periodically (e.g., by a janitor goroutine).
func (b *InMemoryBackend) SweepExpiredMetrics() {
	b.mu.Lock("SweepExpiredMetrics")
	defer b.mu.Unlock()

	cutoff := time.Now().UTC().AddDate(0, 0, -cwMetricRetentionDays)

	for ns, nsMap := range b.metrics {
		before := len(nsMap)
		sweepMetricNamespace(nsMap, cutoff)
		b.totalMetrics -= before - len(nsMap) // #60: maintain running total
		if len(nsMap) == 0 {
			delete(b.metrics, ns)
		}
	}
}

// sweepMetricNamespace removes expired data points from every metric record in nsMap.
// It deletes records whose entire point set has expired.
func sweepMetricNamespace(nsMap map[string]*metricRecord, cutoff time.Time) {
	for key, rec := range nsMap {
		alive := filterAlivePoints(rec.Points, cutoff)
		if len(alive) == 0 {
			delete(nsMap, key)
		} else {
			rec.Points = alive
		}
	}
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
func populateBuckets(all []MetricDatum, startTime, endTime time.Time, period int32) map[int64]*metricBucket {
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

	if statSet["Sum"] {
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
			dp.ExtendedStatistics = computePercentiles(raw, extendedStatistics)
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

	for _, d := range b.anomalyDetectors {
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

	// Extract values from datapoints for band computation.
	vals := make([]float64, len(datapoints))
	for i, dp := range datapoints {
		switch {
		case dp.Average != nil:
			vals[i] = *dp.Average
		case dp.Sum != nil:
			vals[i] = *dp.Sum
		case dp.Maximum != nil:
			vals[i] = *dp.Maximum
		default:
			vals[i] = 0
		}
	}

	bandWidth := matchedDetector.BandWidth
	if bandWidth <= 0 {
		bandWidth = defaultAnomalyBandStdDevs
	}

	mean, stddev := rollingStats(vals)
	halfWidth := bandWidth * stddev

	for i := range datapoints {
		lo := mean - halfWidth
		hi := mean + halfWidth
		datapoints[i].BandLower = &lo
		datapoints[i].BandUpper = &hi
	}
}

// GetMetricData executes multiple metric queries and returns results.
// Queries with a MetricStat are resolved first; expression queries are evaluated
// after all metric-stat results are available so expressions can reference them.
// scanBy controls sort order: "TimestampDescending" reverses each result; default ascending.
func (b *InMemoryBackend) GetMetricData(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
) ([]MetricDataResult, error) {
	return b.GetMetricDataWithOptions(queries, startTime, endTime, "")
}

// GetMetricDataWithOptions is GetMetricData extended with scan order control.
// scanBy may be "TimestampDescending" or "" / "TimestampAscending" (default).
func (b *InMemoryBackend) GetMetricDataWithOptions(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
	scanBy string,
) ([]MetricDataResult, error) {
	b.mu.RLock("GetMetricData")
	defer b.mu.RUnlock()

	resolved := make(map[string]MetricDataResult, len(queries))
	// Preserve original query order for the returned slice.
	ordered := make([]string, 0, len(queries))

	// First pass: resolve MetricStat queries.
	for _, q := range queries {
		ordered = append(ordered, q.ID)
		if q.Expression != "" {
			continue
		}
		resolved[q.ID] = b.resolveMetricStat(q, startTime, endTime)
	}

	// Second pass: evaluate expression queries in topological order so forward
	// references work regardless of declaration order.
	exprOrder, _ := topoSortExpressions(queries)

	exprByID := make(map[string]MetricDataQuery, len(queries))
	for _, q := range queries {
		if q.Expression != "" {
			exprByID[q.ID] = q
		}
	}

	for _, id := range exprOrder {
		q, ok := exprByID[id]
		if !ok {
			continue
		}
		resolved[id] = evalExpression(q, resolved)
	}

	descending := strings.EqualFold(scanBy, "TimestampDescending")

	results := make([]MetricDataResult, 0, len(queries))
	for i, id := range ordered {
		q := queries[i]
		r := resolved[id]
		// ReturnData defaults to true when the field is its zero value (false) AND it's not
		// an expression-only query. AWS semantics: omitting ReturnData means return it.
		// We model ReturnData=false as the caller explicitly setting it; since Go zero is false,
		// we cannot distinguish "not set" from "set false" without a pointer. We treat false as
		// "return data" for MetricStat queries, consistent with AWS SDK defaults.
		// For expression queries, ReturnData=false suppresses output.
		if q.Expression != "" && !q.ReturnData {
			continue
		}
		if descending && len(r.Timestamps) > 1 {
			reverseMetricDataResult(&r)
		}
		results = append(results, r)
	}

	return results, nil
}

// reverseMetricDataResult reverses the timestamp and value slices in-place.
func reverseMetricDataResult(r *MetricDataResult) {
	n := len(r.Timestamps)
	for i := range n / 2 {
		j := n - 1 - i
		r.Timestamps[i], r.Timestamps[j] = r.Timestamps[j], r.Timestamps[i]
		r.Values[i], r.Values[j] = r.Values[j], r.Values[i]
	}
}

// resolveMetricStat fetches and aggregates data for a single MetricStat query.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) resolveMetricStat(q MetricDataQuery, startTime, endTime time.Time) MetricDataResult {
	// Cross-account queries reference metrics from another AWS account.
	// Return empty data gracefully — cross-account is not supported locally.
	if q.AccountID != "" && q.AccountID != b.accountID {
		label := q.Label
		if label == "" {
			label = q.MetricStat.MetricName
		}

		return MetricDataResult{
			ID:         q.ID,
			Label:      label,
			Timestamps: []time.Time{},
			Values:     []float64{},
			StatusCode: metricDataStatusComplete,
		}
	}

	ns := q.MetricStat.Namespace
	metricName := q.MetricStat.MetricName
	period := q.MetricStat.Period
	stat := q.MetricStat.Stat

	var all []MetricDatum
	if nsMap, ok := b.metrics[ns]; ok {
		key := metricStorageKey(metricName, q.MetricStat.Dimensions)
		if rec, found := nsMap[key]; found {
			all = rec.Points
		}
	}

	buckets := populateBuckets(all, startTime, endTime, period)

	statSet := map[string]bool{stat: true}
	var timestamps []time.Time
	var values []float64

	for _, bk := range buckets {
		if bk.count == 0 {
			continue
		}

		dp := buildDatapoint(bk, statSet)
		v := statValue(dp, stat)
		timestamps = append(timestamps, dp.Timestamp)
		values = append(values, v)
	}

	// Sort by timestamp ascending — AWS guarantees ascending order for GetMetricData.
	if len(timestamps) > 1 {
		type tsv struct {
			ts  time.Time
			val float64
		}
		pairs := make([]tsv, len(timestamps))
		for i := range timestamps {
			pairs[i] = tsv{timestamps[i], values[i]}
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].ts.Before(pairs[j].ts) })
		for i := range pairs {
			timestamps[i] = pairs[i].ts
			values[i] = pairs[i].val
		}
	}

	label := q.Label
	if label == "" {
		label = metricName
	}

	return MetricDataResult{
		ID:         q.ID,
		Label:      label,
		Timestamps: timestamps,
		Values:     values,
		StatusCode: metricDataStatusComplete,
	}
}

// statValue extracts a single float value from a Datapoint based on the requested statistic.
func statValue(dp Datapoint, stat string) float64 {
	switch stat {
	case "Sum":
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

// ListMetrics returns a page of unique metrics matching optional namespace, metricName, and
// dimension filters. dimensions specifies an exact set that must match (all filter dims present
// with matching values and no extra dims in the stored record).
func (b *InMemoryBackend) ListMetrics(
	namespace, metricName string,
	dimensions []Dimension,
	nextToken string,
	maxResults int,
) (page.Page[Metric], error) {
	b.mu.RLock("ListMetrics")
	defer b.mu.RUnlock()

	var result []Metric
	for ns, nsMap := range b.metrics {
		if namespace != "" && ns != namespace {
			continue
		}
		for _, rec := range nsMap {
			if metricName != "" && rec.MetricName != metricName {
				continue
			}
			if !dimsContainAll(rec.Dimensions, dimensions) {
				continue
			}
			dims := make([]Dimension, len(rec.Dimensions))
			copy(dims, rec.Dimensions)
			result = append(result, Metric{Namespace: ns, MetricName: rec.MetricName, Dimensions: dims})
		}
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

// PutMetricAlarm creates or updates an alarm.
func (b *InMemoryBackend) PutMetricAlarm(alarm *MetricAlarm) error {
	if alarm.AlarmName == "" {
		return ErrAlarmNameRequired
	}

	// AWS validation: Statistic and ExtendedStatistic are mutually exclusive.
	if alarm.Statistic != "" && alarm.ExtendedStatistic != "" {
		return fmt.Errorf("%w: Statistic and ExtendedStatistic are mutually exclusive", ErrValidation)
	}

	// AWS validation: DatapointsToAlarm must not exceed EvaluationPeriods.
	if alarm.DatapointsToAlarm > 0 && alarm.DatapointsToAlarm > alarm.EvaluationPeriods {
		return fmt.Errorf(
			"%w: DatapointsToAlarm (%d) must not exceed EvaluationPeriods (%d)",
			ErrValidation,
			alarm.DatapointsToAlarm,
			alarm.EvaluationPeriods,
		)
	}

	b.mu.Lock("PutMetricAlarm")
	defer b.mu.Unlock()

	isNew := b.alarms[alarm.AlarmName] == nil

	if alarm.AlarmArn == "" {
		alarm.AlarmArn = arn.Build("cloudwatch", b.region, b.accountID, "alarm:"+alarm.AlarmName)
	}
	if alarm.StateValue == "" {
		alarm.StateValue = alarmStateInsufficientData
	}
	now := time.Now().UTC()
	if alarm.CreatedAt.IsZero() {
		alarm.CreatedAt = now
	}
	// Preserve the state-transitioned timestamp from an existing alarm if the state did not change.
	if existing, ok := b.alarms[alarm.AlarmName]; ok {
		if existing.StateValue == alarm.StateValue {
			alarm.StateTransitionedTimestamp = existing.StateTransitionedTimestamp
		} else {
			alarm.StateTransitionedTimestamp = now
		}
	} else {
		alarm.StateTransitionedTimestamp = now
	}
	alarm.AlarmConfigurationUpdatedTimestamp = now

	cp := *alarm
	b.alarms[alarm.AlarmName] = &cp

	histType := historyTypeConfigurationUpdate
	historySummary := fmt.Sprintf("Alarm %q updated", alarm.AlarmName)
	if isNew {
		historySummary = fmt.Sprintf("Alarm %q created", alarm.AlarmName)
	}
	b.appendHistory(alarm.AlarmName, "MetricAlarm", histType, historySummary, "")

	return nil
}

// PutCompositeAlarm creates or updates a composite alarm and evaluates its state.
func (b *InMemoryBackend) PutCompositeAlarm(alarm *CompositeAlarm) error {
	if alarm.AlarmName == "" {
		return ErrAlarmNameRequired
	}
	if alarm.AlarmRule == "" {
		return ErrAlarmRuleRequired
	}

	b.mu.Lock("PutCompositeAlarm")
	defer b.mu.Unlock()

	isNew := b.compositeAlarms[alarm.AlarmName] == nil

	if alarm.AlarmArn == "" {
		alarm.AlarmArn = arn.Build("cloudwatch", b.region, b.accountID, "alarm:"+alarm.AlarmName)
	}
	if alarm.CreatedAt.IsZero() {
		alarm.CreatedAt = time.Now()
	}

	// Evaluate state based on AlarmRule and current child alarm states.
	newState := b.evalCompositeRule(alarm.AlarmRule)
	if existing, ok := b.compositeAlarms[alarm.AlarmName]; ok {
		if existing.StateTransitionedTimestamp.IsZero() || newState != existing.StateValue {
			alarm.StateTransitionedTimestamp = time.Now().UTC()
		} else {
			alarm.StateTransitionedTimestamp = existing.StateTransitionedTimestamp
		}
	} else {
		alarm.StateTransitionedTimestamp = time.Now().UTC()
	}
	alarm.StateValue = newState
	if alarm.StateReason == "" {
		alarm.StateReason = "Rule evaluated to " + newState
	}

	cp := *alarm
	b.compositeAlarms[alarm.AlarmName] = &cp

	histType := historyTypeConfigurationUpdate
	historySummary := fmt.Sprintf("Composite alarm %q updated", alarm.AlarmName)
	if isNew {
		historySummary = fmt.Sprintf("Composite alarm %q created", alarm.AlarmName)
	}
	b.appendHistory(alarm.AlarmName, "CompositeAlarm", histType, historySummary, "")

	return nil
}

// evalCompositeRule evaluates the composite alarm rule using current alarm states.
// It guards against circular composite alarm references by tracking visited names.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) evalCompositeRule(rule string) string {
	return b.evalCompositeRuleDepth(rule, make(map[string]bool), 0)
}

// evalCompositeRuleDepth is the recursive implementation of evalCompositeRule.
// visited tracks composite alarm names currently on the call stack to detect cycles.
// depth enforces an absolute recursion cap as a secondary safety measure.
// This function is always called while b.mu is held, so visited is accessed
// single-threadedly and does not require additional synchronisation.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) evalCompositeRuleDepth(rule string, visited map[string]bool, depth int) string {
	if depth > cwMaxCompositeEvalDepth {
		return alarmStateInsufficientData
	}

	resolve := func(name string) string {
		if a, ok := b.alarms[name]; ok {
			return a.StateValue
		}
		if ca, ok := b.compositeAlarms[name]; ok {
			if visited[name] {
				// Circular dependency detected: treat as INSUFFICIENT_DATA.
				return alarmStateInsufficientData
			}
			visited[name] = true
			state := b.evalCompositeRuleDepth(ca.AlarmRule, visited, depth+1)
			delete(visited, name)

			return state
		}

		return alarmStateInsufficientData
	}

	return evaluateAlarmRule(rule, resolve)
}

// DescribeAlarms lists a page of alarms, optionally filtered by name, type, prefix, and/or state.
// alarmTypes can contain "MetricAlarm", "CompositeAlarm", or both (empty means both).
// MaxRecords applies to the total combined result set (metric + composite).
func (b *InMemoryBackend) DescribeAlarms(
	alarmNames []string,
	alarmTypes []string,
	alarmNamePrefix, stateValue, nextToken string,
	maxRecords int,
) (page.Page[MetricAlarm], page.Page[CompositeAlarm], error) {
	b.mu.RLock("DescribeAlarms")
	defer b.mu.RUnlock()

	nameSet := toSet(alarmNames)
	typeSet := toSet(alarmTypes)
	includeMetric := len(typeSet) == 0 || typeSet["MetricAlarm"]
	includeComposite := len(typeSet) == 0 || typeSet["CompositeAlarm"]

	metricResult := b.collectMetricAlarms(nameSet, alarmNamePrefix, stateValue, includeMetric)
	compositeResult := b.collectCompositeAlarms(nameSet, alarmNamePrefix, stateValue, includeComposite)

	// Apply a single combined page limit so MaxRecords constrains the total result set.
	limit := maxRecords
	if limit <= 0 {
		limit = cwDefaultDescribeAlarmsLimit
	}
	combinedTotal := len(metricResult) + len(compositeResult)
	start := min(page.DecodeToken(nextToken), combinedTotal)
	end := start + limit
	var next string
	if end < combinedTotal {
		next = page.EncodeToken(end)
	} else {
		end = combinedTotal
	}
	// Split the combined window back into metric and composite slices.
	var metricSlice []MetricAlarm
	var compositeSlice []CompositeAlarm
	for i := start; i < end; i++ {
		if i < len(metricResult) {
			metricSlice = append(metricSlice, metricResult[i])
		} else {
			compositeSlice = append(compositeSlice, compositeResult[i-len(metricResult)])
		}
	}
	if metricSlice == nil {
		metricSlice = []MetricAlarm{}
	}
	if compositeSlice == nil {
		compositeSlice = []CompositeAlarm{}
	}

	return page.Page[MetricAlarm]{Data: metricSlice, Next: next},
		page.Page[CompositeAlarm]{Data: compositeSlice, Next: next},
		nil
}

// toSet converts a string slice to a set (map[string]bool).
func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}

	return m
}

// collectMetricAlarms returns filtered and sorted metric alarms.
// Caller must hold b.mu (read lock).
func (b *InMemoryBackend) collectMetricAlarms(
	nameSet map[string]bool,
	alarmNamePrefix, stateValue string,
	include bool,
) []MetricAlarm {
	if !include {
		return nil
	}

	var result []MetricAlarm

	for _, alarm := range b.alarms {
		if len(nameSet) > 0 && !nameSet[alarm.AlarmName] {
			continue
		}

		if alarmNamePrefix != "" && !strings.HasPrefix(alarm.AlarmName, alarmNamePrefix) {
			continue
		}

		if stateValue != "" && alarm.StateValue != stateValue {
			continue
		}

		result = append(result, *alarm)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AlarmName < result[j].AlarmName
	})

	return result
}

// collectCompositeAlarms returns filtered and sorted composite alarms.
// Caller must hold b.mu (read lock).
func (b *InMemoryBackend) collectCompositeAlarms(
	nameSet map[string]bool,
	alarmNamePrefix, stateValue string,
	include bool,
) []CompositeAlarm {
	if !include {
		return nil
	}

	var result []CompositeAlarm

	for _, alarm := range b.compositeAlarms {
		if len(nameSet) > 0 && !nameSet[alarm.AlarmName] {
			continue
		}

		if alarmNamePrefix != "" && !strings.HasPrefix(alarm.AlarmName, alarmNamePrefix) {
			continue
		}

		if stateValue != "" && alarm.StateValue != stateValue {
			continue
		}

		result = append(result, *alarm)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AlarmName < result[j].AlarmName
	})

	return result
}

// DescribeAlarmsForMetric returns metric alarms associated with a specific metric.
func (b *InMemoryBackend) DescribeAlarmsForMetric(
	namespace, metricName string,
	dimensions []Dimension,
	alarmNames []string,
	nextToken string,
	maxRecords int,
) (page.Page[MetricAlarm], error) {
	b.mu.RLock("DescribeAlarmsForMetric")
	defer b.mu.RUnlock()

	nameSet := make(map[string]bool, len(alarmNames))
	for _, n := range alarmNames {
		nameSet[n] = true
	}

	var result []MetricAlarm
	for _, alarm := range b.alarms {
		if namespace != "" && alarm.Namespace != namespace {
			continue
		}
		if metricName != "" && alarm.MetricName != metricName {
			continue
		}
		if len(nameSet) > 0 && !nameSet[alarm.AlarmName] {
			continue
		}
		if len(dimensions) > 0 && !dimsContainAll(alarm.Dimensions, dimensions) {
			continue
		}
		result = append(result, *alarm)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AlarmName < result[j].AlarmName
	})

	return page.New(result, nextToken, maxRecords, cwDefaultDescribeForMetricLimit), nil
}

// matchesHistoryFilters returns true if the item passes all the given history filters.
func matchesHistoryFilters(
	item AlarmHistoryItem,
	alarmType, historyItemType string,
	startDate, endDate time.Time,
) bool {
	if alarmType != "" && item.AlarmType != alarmType {
		return false
	}
	if historyItemType != "" && item.HistoryItemType != historyItemType {
		return false
	}
	if !startDate.IsZero() && item.Timestamp.Before(startDate) {
		return false
	}
	if !endDate.IsZero() && item.Timestamp.After(endDate) {
		return false
	}

	return true
}

// DescribeAlarmHistory returns history items for one or all alarms, filtered by type and date range.
// alarmType filters by "MetricAlarm" or "CompositeAlarm" (stored on history items); empty means all.
func (b *InMemoryBackend) DescribeAlarmHistory(
	alarmName, alarmType, historyItemType, nextToken string,
	startDate, endDate time.Time,
	maxRecords int,
) (page.Page[AlarmHistoryItem], error) {
	b.mu.RLock("DescribeAlarmHistory")
	defer b.mu.RUnlock()

	var result []AlarmHistoryItem
	for name, items := range b.alarmHistory {
		if alarmName != "" && name != alarmName {
			continue
		}
		for _, item := range items {
			if matchesHistoryFilters(item, alarmType, historyItemType, startDate, endDate) {
				result = append(result, item)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})

	return page.New(result, nextToken, maxRecords, cwDefaultAlarmHistoryLimit), nil
}

// DeleteAlarms removes alarms by name (metric and composite).
func (b *InMemoryBackend) DeleteAlarms(alarmNames []string) error {
	b.mu.Lock("DeleteAlarms")
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		delete(b.alarms, name)
		delete(b.compositeAlarms, name)
	}

	return nil
}

// SetAlarmState manually sets the state of an alarm and fires the corresponding actions.
func (b *InMemoryBackend) SetAlarmState(
	ctx context.Context,
	alarmName, stateValue, stateReason, stateReasonData string,
) error {
	b.mu.Lock("SetAlarmState")

	metricAlarm, hasMetric := b.alarms[alarmName]
	compositeAlarm, hasComposite := b.compositeAlarms[alarmName]

	if !hasMetric && !hasComposite {
		b.mu.Unlock()

		return fmt.Errorf("%w: %s", ErrAlarmNotFound, alarmName)
	}

	var oldState string
	var alarmArn string
	var alarmDesc string
	var alarmActions, okActions, insuffActions []string
	var actionsEnabled bool

	if hasMetric {
		oldState = metricAlarm.StateValue
		alarmArn = metricAlarm.AlarmArn
		alarmDesc = metricAlarm.AlarmDescription
		alarmActions = metricAlarm.AlarmActions
		okActions = metricAlarm.OKActions
		insuffActions = metricAlarm.InsufficientDataActions
		actionsEnabled = metricAlarm.ActionsEnabled

		metricAlarm.StateValue = stateValue
		metricAlarm.StateReason = stateReason
		metricAlarm.StateReasonData = stateReasonData
		if oldState != stateValue {
			metricAlarm.StateTransitionedTimestamp = time.Now().UTC()
		}
	} else {
		oldState = compositeAlarm.StateValue
		alarmArn = compositeAlarm.AlarmArn
		alarmDesc = compositeAlarm.AlarmDescription
		alarmActions = compositeAlarm.AlarmActions
		okActions = compositeAlarm.OKActions
		insuffActions = compositeAlarm.InsufficientDataActions
		actionsEnabled = compositeAlarm.ActionsEnabled

		compositeAlarm.StateValue = stateValue
		compositeAlarm.StateReason = stateReason
		if oldState != stateValue {
			compositeAlarm.StateTransitionedTimestamp = time.Now().UTC()
		}
	}

	summary := fmt.Sprintf("Alarm %q changed from %s to %s", alarmName, oldState, stateValue)
	histData := b.stateChangeHistoryData(alarmName, oldState, stateValue, stateReason)
	histAlarmType := "MetricAlarm"
	if !hasMetric {
		histAlarmType = "CompositeAlarm"
	}
	b.appendHistory(alarmName, histAlarmType, historyTypeStateUpdate, summary, histData)

	// re-evaluate composite alarms that may reference this alarm, collecting any transitions
	compositeTransitions := b.reevaluateCompositeAlarms()

	snsPub := b.snsPublisher
	lambdaInv := b.lambdaInvoker
	b.mu.Unlock()

	if actionsEnabled && stateValue != oldState {
		var actions []string
		switch stateValue {
		case alarmStateAlarm:
			actions = alarmActions
		case alarmStateOK:
			actions = okActions
		case alarmStateInsufficientData:
			actions = insuffActions
		}

		payload := b.buildAlarmActionPayload(alarmName, alarmDesc, alarmArn, oldState, stateValue, stateReason)
		b.executeActions(ctx, actions, alarmName, payload, snsPub, lambdaInv)
	}

	// fire actions for any composite alarms that changed state
	for _, tr := range compositeTransitions {
		payload := b.buildAlarmActionPayload(
			tr.alarmName, tr.alarmDesc, tr.alarmArn,
			tr.oldState, tr.newState, tr.reason,
		)
		b.executeActions(ctx, tr.actions, tr.alarmName, payload, snsPub, lambdaInv)
	}

	return nil
}

// EnableAlarmActions enables action execution for the given alarms.
func (b *InMemoryBackend) EnableAlarmActions(alarmNames []string) error {
	b.mu.Lock("EnableAlarmActions")
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		if a, ok := b.alarms[name]; ok {
			a.ActionsEnabled = true
		}
		if ca, ok := b.compositeAlarms[name]; ok {
			ca.ActionsEnabled = true
		}
	}

	return nil
}

// DisableAlarmActions disables action execution for the given alarms.
func (b *InMemoryBackend) DisableAlarmActions(alarmNames []string) error {
	b.mu.Lock("DisableAlarmActions")
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		if a, ok := b.alarms[name]; ok {
			a.ActionsEnabled = false
		}
		if ca, ok := b.compositeAlarms[name]; ok {
			ca.ActionsEnabled = false
		}
	}

	return nil
}

// appendHistory adds a history item. Caller must hold b.mu (write lock).
// alarmTypeName should be "MetricAlarm" or "CompositeAlarm" to populate the AlarmType field.
func (b *InMemoryBackend) appendHistory(alarmName, alarmTypeName, itemType, summary, data string) {
	item := AlarmHistoryItem{
		Timestamp:       time.Now(),
		AlarmName:       alarmName,
		AlarmType:       alarmTypeName,
		HistoryItemType: itemType,
		HistorySummary:  summary,
		HistoryData:     data,
	}
	b.alarmHistory[alarmName] = append(b.alarmHistory[alarmName], item)
	// Cap history to avoid unbounded growth.
	if h := b.alarmHistory[alarmName]; len(h) > cwMaxAlarmHistory {
		b.alarmHistory[alarmName] = h[len(h)-cwMaxAlarmHistory:]
	}
}

// stateChangeHistoryData builds a JSON string for a state-change history item.
func (b *InMemoryBackend) stateChangeHistoryData(alarmName, oldState, newState, reason string) string {
	data := map[string]string{
		keyAlarmName:     alarmName,
		"OldStateValue":  oldState,
		"NewStateValue":  newState,
		"NewStateReason": reason,
	}
	// map[string]string marshaling cannot fail; error is intentionally ignored.
	bs, _ := json.Marshal(data)

	return string(bs)
}

// buildAlarmActionPayload builds the JSON payload sent to SNS/Lambda when an alarm fires.
func (b *InMemoryBackend) buildAlarmActionPayload(
	alarmName, alarmDesc, alarmArn, oldState, newState, reason string,
) []byte {
	data := map[string]string{
		keyAlarmName:        alarmName,
		keyAlarmDescription: alarmDesc,
		keyAlarmArn:         alarmArn,
		"AWSAccountId":      b.accountID,
		"Region":            b.region,
		"NewStateValue":     newState,
		"NewStateReason":    reason,
		"OldStateValue":     oldState,
		"StateChangeTime":   time.Now().UTC().Format(time.RFC3339),
	}
	// map[string]string marshaling cannot fail; error is intentionally ignored.
	bs, _ := json.Marshal(data)

	return bs
}

// executeActions delivers the alarm action notifications to SNS topics and Lambda functions.
// Delivery errors are logged as warnings but do not prevent other actions from running.
func (b *InMemoryBackend) executeActions(
	ctx context.Context,
	actions []string,
	_ string,
	payload []byte,
	snsPub SNSPublisher,
	lambdaInv LambdaInvoker,
) {
	for _, action := range actions {
		switch {
		case strings.HasPrefix(action, "arn:aws:sns:"):
			if snsPub != nil {
				if err := snsPub.PublishToTopic(action, string(payload)); err != nil {
					slog.Default().WarnContext(ctx, "cloudwatch: alarm SNS action delivery failed",
						"topic_arn", action, "error", err)
				}
			}
		case strings.HasPrefix(action, "arn:aws:lambda:"):
			if lambdaInv != nil {
				if _, _, err := lambdaInv.InvokeFunction(ctx, action, "Event", payload); err != nil {
					slog.Default().WarnContext(ctx, "cloudwatch: alarm Lambda action delivery failed",
						"function_arn", action, "error", err)
				}
			}
			// EC2 and Auto Scaling actions are stubbed (no-op).
		}
	}
}

// compositeAlarmTransition records a composite alarm state change and the actions to fire.
type compositeAlarmTransition struct {
	alarmName string
	alarmArn  string
	alarmDesc string
	oldState  string
	newState  string
	reason    string
	actions   []string
}

// reevaluateCompositeAlarms re-checks all composite alarms and updates their state.
// Returns the list of state transitions so the caller can fire actions after releasing the lock.
// Caller must hold b.mu (write lock).
func (b *InMemoryBackend) reevaluateCompositeAlarms() []compositeAlarmTransition {
	var transitions []compositeAlarmTransition

	for _, ca := range b.compositeAlarms {
		newState := b.evalCompositeRule(ca.AlarmRule)
		if newState == ca.StateValue {
			continue
		}

		oldState := ca.StateValue
		reason := "Rule evaluated to " + newState
		ca.StateValue = newState
		ca.StateReason = reason
		ca.StateTransitionedTimestamp = time.Now().UTC()
		summary := fmt.Sprintf("Composite alarm %q changed from %s to %s", ca.AlarmName, oldState, newState)
		histData := b.stateChangeHistoryData(ca.AlarmName, oldState, newState, reason)
		b.appendHistory(ca.AlarmName, "CompositeAlarm", historyTypeStateUpdate, summary, histData)

		if ca.ActionsEnabled {
			var actions []string
			switch newState {
			case alarmStateAlarm:
				actions = ca.AlarmActions
			case alarmStateOK:
				actions = ca.OKActions
			case alarmStateInsufficientData:
				actions = ca.InsufficientDataActions
			}
			if len(actions) > 0 {
				transitions = append(transitions, compositeAlarmTransition{
					alarmName: ca.AlarmName,
					alarmArn:  ca.AlarmArn,
					alarmDesc: ca.AlarmDescription,
					oldState:  oldState,
					newState:  newState,
					reason:    reason,
					actions:   actions,
				})
			}
		}
	}

	return transitions
}

// PutDashboard creates or updates a CloudWatch dashboard by name.
func (b *InMemoryBackend) PutDashboard(name, body string) error {
	if name == "" {
		return ErrDashboardNameRequired
	}

	b.mu.Lock("PutDashboard")
	defer b.mu.Unlock()

	b.dashboards[name] = &dashboardRecord{
		Name:         name,
		Body:         body,
		LastModified: time.Now().UTC(),
	}

	return nil
}

// GetDashboard returns the dashboard entry and body for the given name.
func (b *InMemoryBackend) GetDashboard(name string) (DashboardEntry, string, error) {
	b.mu.RLock("GetDashboard")
	defer b.mu.RUnlock()

	rec, ok := b.dashboards[name]
	if !ok {
		return DashboardEntry{}, "", fmt.Errorf("%w: %s", ErrDashboardNotFound, name)
	}

	return b.toDashboardEntry(rec), rec.Body, nil
}

// ListDashboards returns a page of dashboard entries optionally filtered by name prefix.
func (b *InMemoryBackend) ListDashboards(prefix, nextToken string) (page.Page[DashboardEntry], error) {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	var result []DashboardEntry

	for _, rec := range b.dashboards {
		if prefix != "" && !strings.HasPrefix(rec.Name, prefix) {
			continue
		}

		result = append(result, b.toDashboardEntry(rec))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DashboardName < result[j].DashboardName
	})

	return page.New(result, nextToken, 0, cwDefaultListDashboardsLimit), nil
}

// DeleteDashboards removes the named dashboards. Names that do not exist are silently ignored.
func (b *InMemoryBackend) DeleteDashboards(names []string) error {
	b.mu.Lock("DeleteDashboards")
	defer b.mu.Unlock()

	for _, name := range names {
		delete(b.dashboards, name)
	}

	return nil
}

// toDashboardEntry converts a dashboardRecord to a DashboardEntry.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) toDashboardEntry(rec *dashboardRecord) DashboardEntry {
	return DashboardEntry{
		DashboardName: rec.Name,
		DashboardArn:  arn.Build("cloudwatch", b.region, b.accountID, "dashboard/"+rec.Name),
		LastModified:  rec.LastModified,
		Size:          int64(len(rec.Body)),
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.metrics = make(map[string]map[string]*metricRecord)
	b.alarms = make(map[string]*MetricAlarm)
	b.compositeAlarms = make(map[string]*CompositeAlarm)
	b.alarmHistory = make(map[string][]AlarmHistoryItem)
	b.dashboards = make(map[string]*dashboardRecord)
	b.anomalyDetectors = make(map[string]*AnomalyDetector)
	b.insightRules = make(map[string]*InsightRule)
	b.metricStreams = make(map[string]*MetricStream)
	b.alarmMuteRules = make(map[string]*AlarmMuteRule)
	b.metricFilters = make(map[string]*MetricFilter)
}

// aggregateContributorPoint updates the running aggregation for a single metric record point.
func aggregateContributorPoint(
	pt MetricDatum,
	key string,
	rec *metricRecord,
	orderBy string,
	dimSums map[string]float64,
	dimKeys map[string][]string,
) {
	if _, seen := dimKeys[key]; !seen {
		keys := make([]string, len(rec.Dimensions))
		for i, d := range rec.Dimensions {
			keys[i] = d.Value
		}
		dimKeys[key] = keys
	}
	if strings.EqualFold(orderBy, "Sum") {
		dimSums[key] += pt.Sum
	} else {
		dimSums[key] += pt.Count
	}
}

// aggregateContributorRecord accumulates a metric record's in-range points into the maps.
func aggregateContributorRecord(
	rec *metricRecord,
	startTime, endTime time.Time,
	orderBy string,
	dimSums map[string]float64,
	dimKeys map[string][]string,
) {
	if len(rec.Dimensions) == 0 {
		return
	}
	key := dimensionSetKey(rec.Dimensions)
	for _, pt := range rec.Points {
		if pt.Timestamp.Before(startTime) || !pt.Timestamp.Before(endTime) {
			continue
		}
		aggregateContributorPoint(pt, key, rec, orderBy, dimSums, dimKeys)
	}
}

// topNContributors converts aggregation maps to a sorted, capped contributor list.
func topNContributors(dimSums map[string]float64, dimKeys map[string][]string, maxN int) []AlarmContributor {
	type entry struct {
		key string
		sum float64
	}
	entries := make([]entry, 0, len(dimSums))
	for k, s := range dimSums {
		entries = append(entries, entry{k, s})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].sum > entries[j].sum })
	if len(entries) > maxN {
		entries = entries[:maxN]
	}
	result := make([]AlarmContributor, 0, len(entries))
	for _, e := range entries {
		result = append(result, AlarmContributor{Keys: dimKeys[e.key], Sum: e.sum})
	}

	return result
}

// GetInsightRuleContributors returns top-N contributors for an insight rule by aggregating
// stored metric data along dimension values. This is a best-effort local approximation.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) GetInsightRuleContributors(
	ruleName string,
	startTime, endTime time.Time,
	maxContributorCount int,
	orderBy string,
) ([]AlarmContributor, error) {
	if _, ok := b.insightRules[ruleName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrInsightRuleNotFound, ruleName)
	}
	if maxContributorCount <= 0 {
		maxContributorCount = 10
	}
	dimSums := make(map[string]float64)
	dimKeys := make(map[string][]string)
	for _, nsMap := range b.metrics {
		for _, rec := range nsMap {
			aggregateContributorRecord(rec, startTime, endTime, orderBy, dimSums, dimKeys)
		}
	}

	return topNContributors(dimSums, dimKeys, maxContributorCount), nil
}

// anomalyDetectorKey returns a stable map key for an anomaly detector.
// Dimensions are included in the key so different dimension sets produce distinct detectors.
func anomalyDetectorKey(namespace, metricName, stat string) string {
	return namespace + "/" + metricName + "/" + stat
}

// PutAlarmMuteRule creates or updates an alarm mute rule by name.
func (b *InMemoryBackend) PutAlarmMuteRule(rule *AlarmMuteRule) error {
	if strings.TrimSpace(rule.MuteName) == "" {
		return fmt.Errorf("%w: MuteName parameter is required", ErrValidation)
	}

	b.PutAlarmMuteRuleInternal(rule)

	return nil
}

// DeleteAlarmMuteRule removes an alarm mute rule by name.
// Returns ErrAlarmMuteRuleNotFound if the rule does not exist.
func (b *InMemoryBackend) DeleteAlarmMuteRule(muteName string) error {
	b.mu.Lock("DeleteAlarmMuteRule")
	defer b.mu.Unlock()

	if _, ok := b.alarmMuteRules[muteName]; !ok {
		return fmt.Errorf("%w: %s", ErrAlarmMuteRuleNotFound, muteName)
	}

	delete(b.alarmMuteRules, muteName)

	return nil
}

// GetAlarmMuteRule returns an alarm mute rule by name.
// Returns ErrAlarmMuteRuleNotFound if the rule does not exist.
func (b *InMemoryBackend) GetAlarmMuteRule(muteName string) (*AlarmMuteRule, error) {
	b.mu.RLock("GetAlarmMuteRule")
	defer b.mu.RUnlock()

	rule, ok := b.alarmMuteRules[muteName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAlarmMuteRuleNotFound, muteName)
	}

	cp := *rule

	return &cp, nil
}

// PutAlarmMuteRuleInternal creates or updates an alarm mute rule (used for test seeding).
func (b *InMemoryBackend) PutAlarmMuteRuleInternal(rule *AlarmMuteRule) {
	b.mu.Lock("PutAlarmMuteRuleInternal")
	defer b.mu.Unlock()

	cp := *rule
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now().UTC()
	}

	b.alarmMuteRules[rule.MuteName] = &cp
}

// DeleteAnomalyDetector removes an anomaly detector.
// Returns ErrAnomalyDetectorNotFound if the detector does not exist.
func (b *InMemoryBackend) DeleteAnomalyDetector(namespace, metricName, stat string) error {
	b.mu.Lock("DeleteAnomalyDetector")
	defer b.mu.Unlock()

	key := anomalyDetectorKey(namespace, metricName, stat)

	if _, ok := b.anomalyDetectors[key]; !ok {
		return fmt.Errorf("%w: %s/%s/%s", ErrAnomalyDetectorNotFound, namespace, metricName, stat)
	}

	delete(b.anomalyDetectors, key)

	return nil
}

// PutAnomalyDetectorInternal creates or updates an anomaly detector (used for test seeding).
func (b *InMemoryBackend) PutAnomalyDetectorInternal(detector *AnomalyDetector) {
	b.mu.Lock("PutAnomalyDetectorInternal")
	defer b.mu.Unlock()

	key := anomalyDetectorKey(detector.Namespace, detector.MetricName, detector.Stat)
	cp := *detector
	if cp.StateValue == "" {
		// TRAINED_INSUFFICIENT_DATA is the realistic initial state for a new detector.
		cp.StateValue = statusTrainedInsufficient
	}

	b.anomalyDetectors[key] = &cp
}

// DescribeAnomalyDetectors returns a filtered, paginated list of anomaly detectors.
func (b *InMemoryBackend) DescribeAnomalyDetectors(
	namespace, metricName, nextToken string,
	maxResults int,
) (page.Page[AnomalyDetector], error) {
	b.mu.RLock("DescribeAnomalyDetectors")
	defer b.mu.RUnlock()

	type entry struct {
		key      string
		detector AnomalyDetector
	}

	var entries []entry

	for k, d := range b.anomalyDetectors {
		if namespace != "" && d.Namespace != namespace {
			continue
		}

		if metricName != "" && d.MetricName != metricName {
			continue
		}

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

// DeleteInsightRules removes insight rules by name. Non-existent rules are reported as failures.
func (b *InMemoryBackend) DeleteInsightRules(ruleNames []string) ([]InsightRuleFailure, error) {
	b.mu.Lock("DeleteInsightRules")
	defer b.mu.Unlock()

	var failures []InsightRuleFailure

	for _, name := range ruleNames {
		if _, ok := b.insightRules[name]; !ok {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		delete(b.insightRules, name)
	}

	return failures, nil
}

// PutInsightRule creates or updates an insight rule.
func (b *InMemoryBackend) PutInsightRule(rule *InsightRule) error {
	if strings.TrimSpace(rule.Name) == "" {
		return fmt.Errorf("%w: RuleName parameter is required", ErrValidation)
	}

	b.PutInsightRuleInternal(rule)

	return nil
}

// GetInsightRule returns an insight rule by name.
func (b *InMemoryBackend) GetInsightRule(name string) (*InsightRule, error) {
	b.mu.RLock("GetInsightRule")
	defer b.mu.RUnlock()

	rule, ok := b.insightRules[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInsightRuleNotFound, name)
	}

	cp := *rule

	return &cp, nil
}

// PutInsightRuleInternal creates or updates an insight rule (used for test seeding).
func (b *InMemoryBackend) PutInsightRuleInternal(rule *InsightRule) {
	b.mu.Lock("PutInsightRuleInternal")
	defer b.mu.Unlock()

	cp := *rule
	if cp.State == "" {
		cp.State = "ENABLED"
	}

	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}

	if cp.Arn == "" {
		cp.Arn = arn.Build("cloudwatch", b.region, b.accountID, "insight-rule/"+rule.Name)
	}

	b.insightRules[rule.Name] = &cp
}

// DescribeInsightRules returns a paginated list of insight rules.
func (b *InMemoryBackend) DescribeInsightRules(
	nextToken string,
	maxResults int,
) (page.Page[InsightRule], error) {
	b.mu.RLock("DescribeInsightRules")
	defer b.mu.RUnlock()

	result := make([]InsightRule, 0, len(b.insightRules))

	for _, r := range b.insightRules {
		result = append(result, *r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, nextToken, maxResults, cwDefaultDescribeInsightRulesLimit), nil
}

// DisableInsightRules disables the specified insight rules. Non-existent rules are reported as failures.
func (b *InMemoryBackend) DisableInsightRules(ruleNames []string) ([]InsightRuleFailure, error) {
	b.mu.Lock("DisableInsightRules")
	defer b.mu.Unlock()

	var failures []InsightRuleFailure

	for _, name := range ruleNames {
		rule, ok := b.insightRules[name]
		if !ok {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		rule.State = "DISABLED"
	}

	return failures, nil
}

// EnableInsightRules enables the specified insight rules. Non-existent rules are reported as failures.
func (b *InMemoryBackend) EnableInsightRules(ruleNames []string) ([]InsightRuleFailure, error) {
	b.mu.Lock("EnableInsightRules")
	defer b.mu.Unlock()

	var failures []InsightRuleFailure

	for _, name := range ruleNames {
		rule, ok := b.insightRules[name]
		if !ok {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		rule.State = "ENABLED"
	}

	return failures, nil
}

// PutMetricStream creates or updates a metric stream by name.
func (b *InMemoryBackend) PutMetricStream(stream *MetricStream) error {
	if strings.TrimSpace(stream.Name) == "" {
		return fmt.Errorf("%w: Name parameter is required for metric stream", ErrValidation)
	}

	b.PutMetricStreamInternal(stream)

	return nil
}

// GetMetricStream returns a metric stream by name.
func (b *InMemoryBackend) GetMetricStream(name string) (*MetricStream, error) {
	b.mu.RLock("GetMetricStream")
	defer b.mu.RUnlock()

	stream, ok := b.metricStreams[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMetricStreamNotFound, name)
	}

	cp := *stream

	return &cp, nil
}

// DeleteMetricStream removes a metric stream by name.
// Returns ErrMetricStreamNotFound if the stream does not exist.
func (b *InMemoryBackend) DeleteMetricStream(name string) error {
	b.mu.Lock("DeleteMetricStream")
	defer b.mu.Unlock()

	if _, ok := b.metricStreams[name]; !ok {
		return fmt.Errorf("%w: %s", ErrMetricStreamNotFound, name)
	}

	delete(b.metricStreams, name)

	return nil
}

// PutMetricStreamInternal creates or updates a metric stream (used for test seeding).
func (b *InMemoryBackend) PutMetricStreamInternal(stream *MetricStream) {
	b.mu.Lock("PutMetricStreamInternal")
	defer b.mu.Unlock()

	cp := *stream
	if cp.CreationDate.IsZero() {
		cp.CreationDate = time.Now().UTC()
	}

	cp.LastUpdateDate = time.Now().UTC()

	if cp.Arn == "" {
		cp.Arn = arn.Build("cloudwatch", b.region, b.accountID, "metric-stream/"+stream.Name)
	}

	// Preserve the existing state if not explicitly set so that Stop/Start calls are honoured.
	if existing, ok := b.metricStreams[stream.Name]; ok && cp.State == "" {
		cp.State = existing.State
	}

	if cp.State == "" {
		cp.State = metricStreamStateRunning
	}

	b.metricStreams[stream.Name] = &cp
}

// DescribeAlarmContributors returns a page of contributors for the specified alarm.
// The in-memory implementation always returns an empty list since no real metric analysis is performed.
func (b *InMemoryBackend) DescribeAlarmContributors(
	alarmName, nextToken string,
) (page.Page[AlarmContributor], error) {
	b.mu.RLock("DescribeAlarmContributors")
	defer b.mu.RUnlock()

	if _, ok := b.alarms[alarmName]; !ok {
		if _, ok2 := b.compositeAlarms[alarmName]; !ok2 {
			return page.Page[AlarmContributor]{}, fmt.Errorf("%w: %s", ErrAlarmNotFound, alarmName)
		}
	}

	return page.New([]AlarmContributor{}, nextToken, 0, cwDefaultDescribeAlarmContributorsLimit), nil
}

// GetAlarmARNs returns the ARNs for the given alarm names (metric + composite).
// Used by the HTTP handler to clean up tag entries on delete.
func (b *InMemoryBackend) GetAlarmARNs(names []string) []string {
	b.mu.RLock("GetAlarmARNs")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(names))
	for _, name := range names {
		if a, ok := b.alarms[name]; ok && a.AlarmArn != "" {
			arns = append(arns, a.AlarmArn)
		}
		if ca, ok := b.compositeAlarms[name]; ok && ca.AlarmArn != "" {
			arns = append(arns, ca.AlarmArn)
		}
	}

	return arns
}

// GetDashboardARNs returns the ARNs for the given dashboard names.
// Used by the HTTP handler to clean up tag entries on delete.
func (b *InMemoryBackend) GetDashboardARNs(names []string) []string {
	b.mu.RLock("GetDashboardARNs")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(names))
	for _, name := range names {
		if _, ok := b.dashboards[name]; ok {
			arns = append(arns, arn.Build("cloudwatch", b.region, b.accountID, "dashboard/"+name))
		}
	}

	return arns
}
func (b *InMemoryBackend) PutAnomalyDetector(detector *AnomalyDetector) error {
	if detector.Namespace == "" || detector.MetricName == "" {
		return fmt.Errorf("%w: Namespace and MetricName are required", ErrValidation)
	}

	b.PutAnomalyDetectorInternal(detector)

	return nil
}

// ListMetricStreams returns a paginated list of all metric streams.
func (b *InMemoryBackend) ListMetricStreams(nextToken string, maxResults int) (page.Page[MetricStream], error) {
	b.mu.RLock("ListMetricStreams")
	defer b.mu.RUnlock()

	result := make([]MetricStream, 0, len(b.metricStreams))
	for _, s := range b.metricStreams {
		result = append(result, *s)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, nextToken, maxResults, cwDefaultListMetricStreamsLimit), nil
}

const metricStreamStateRunning = "RUNNING"
const metricStreamStateStopped = "STOPPED"

// StartMetricStreams sets the State of the named streams to RUNNING.
// Names that do not exist are silently ignored (AWS behaviour).
func (b *InMemoryBackend) StartMetricStreams(names []string) error {
	b.mu.Lock("StartMetricStreams")
	defer b.mu.Unlock()

	for _, name := range names {
		if s, ok := b.metricStreams[name]; ok {
			s.State = metricStreamStateRunning
			s.LastUpdateDate = time.Now().UTC()
		}
	}

	return nil
}

// StopMetricStreams sets the State of the named streams to STOPPED.
// Names that do not exist are silently ignored (AWS behaviour).
func (b *InMemoryBackend) StopMetricStreams(names []string) error {
	b.mu.Lock("StopMetricStreams")
	defer b.mu.Unlock()

	for _, name := range names {
		if s, ok := b.metricStreams[name]; ok {
			s.State = metricStreamStateStopped
			s.LastUpdateDate = time.Now().UTC()
		}
	}

	return nil
}

// metricFilterKey returns a stable map key for a metric filter.
func metricFilterKey(filterName, logGroupName string) string {
	return logGroupName + "/" + filterName
}

// PutMetricFilter creates or updates a metric filter.
func (b *InMemoryBackend) PutMetricFilter(filter *MetricFilter) error {
	if strings.TrimSpace(filter.FilterName) == "" {
		return fmt.Errorf("%w: FilterName parameter is required", ErrValidation)
	}
	if strings.TrimSpace(filter.LogGroupName) == "" {
		return fmt.Errorf("%w: LogGroupName parameter is required", ErrValidation)
	}

	b.mu.Lock("PutMetricFilter")
	defer b.mu.Unlock()

	cp := *filter
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now().UTC()
	}

	b.metricFilters[metricFilterKey(filter.FilterName, filter.LogGroupName)] = &cp

	return nil
}

// DescribeMetricFilters returns a paginated list of metric filters with optional filters.
func (b *InMemoryBackend) DescribeMetricFilters(
	filterNamePrefix, logGroupName, nextToken string,
	maxResults int,
) (page.Page[MetricFilter], error) {
	b.mu.RLock("DescribeMetricFilters")
	defer b.mu.RUnlock()

	var result []MetricFilter
	for _, f := range b.metricFilters {
		if logGroupName != "" && f.LogGroupName != logGroupName {
			continue
		}
		if filterNamePrefix != "" && !strings.HasPrefix(f.FilterName, filterNamePrefix) {
			continue
		}
		result = append(result, *f)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].LogGroupName != result[j].LogGroupName {
			return result[i].LogGroupName < result[j].LogGroupName
		}

		return result[i].FilterName < result[j].FilterName
	})

	return page.New(result, nextToken, maxResults, cwDefaultDescribeMetricFiltersLimit), nil
}

// DeleteMetricFilter removes a metric filter by name and log group.
// Returns ErrMetricFilterNotFound if the filter does not exist.
func (b *InMemoryBackend) DeleteMetricFilter(filterName, logGroupName string) error {
	b.mu.Lock("DeleteMetricFilter")
	defer b.mu.Unlock()

	key := metricFilterKey(filterName, logGroupName)
	if _, ok := b.metricFilters[key]; !ok {
		return fmt.Errorf("%w: %s/%s", ErrMetricFilterNotFound, logGroupName, filterName)
	}

	delete(b.metricFilters, key)

	return nil
}
