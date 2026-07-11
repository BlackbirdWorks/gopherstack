package cloudwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusTrainedInsufficient = "TRAINED_INSUFFICIENT_DATA"
	metricDataStatusComplete  = "Complete"
	statSum                   = "Sum"
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
	cwDefaultListAlarmMuteRulesLimit        = 100
	cwDefaultListManagedInsightRulesLimit   = 100
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

	insightRuleStateEnabled = "ENABLED"

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
	InvokeFunction(
		ctx context.Context,
		name string,
		invocationType string,
		payload []byte,
	) ([]byte, int, error)
}

// EC2InstanceActioner executes EC2 alarm actions (arn:aws:automate:<region>:ec2:*)
// against the instances named by the alarm's InstanceId dimension.
type EC2InstanceActioner interface {
	StopInstances(instanceIDs []string) error
	TerminateInstances(instanceIDs []string) error
	RebootInstances(instanceIDs []string) error
}

// AutoScalingPolicyExecutor executes an Auto Scaling scaling-policy alarm action
// (arn:aws:autoscaling:...:scalingPolicy:...).
type AutoScalingPolicyExecutor interface {
	ExecuteScalingPolicy(autoScalingGroupName, policyName string) error
}

// StorageBackend is the interface for the CloudWatch in-memory store.
type StorageBackend interface {
	PutMetricData(namespace string, data []MetricDatum) error
	GetMetricStatistics(
		namespace, metricName string,
		dimensions []Dimension,
		startTime, endTime time.Time,
		period int32,
		statistics []string,
		extendedStatistics []string,
	) ([]Datapoint, error)
	GetMetricData(
		queries []MetricDataQuery,
		startTime, endTime time.Time,
	) ([]MetricDataResult, error)
	ListMetrics(
		namespace, metricName string,
		dimensions []Dimension,
		recentlyActive, nextToken string,
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
	SetAlarmState(
		ctx context.Context,
		alarmName, stateValue, stateReason, stateReasonData string,
	) error
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
	DeleteAnomalyDetector(namespace, metricName, stat string, dims []Dimension) error
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
	ListAlarmMuteRules(nextToken string, maxResults int) (page.Page[AlarmMuteRule], error)
	ListManagedInsightRules(
		resourceARN, nextToken string,
		maxResults int,
	) (page.Page[InsightRule], error)
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
// metrics and alarmHistory are deliberately left as plain maps rather than
// store.Table -- see the comment block at the top of store_setup.go for why.
// Every other resource field is registered on registry -- see
// registerAllTables in store_setup.go.
type InMemoryBackend struct {
	metrics          map[string]map[string]*metricRecord
	alarmHistory     map[string][]AlarmHistoryItem
	alarms           *store.Table[MetricAlarm]
	compositeAlarms  *store.Table[CompositeAlarm]
	dashboards       *store.Table[dashboardRecord]
	anomalyDetectors *store.Table[AnomalyDetector]
	insightRules     *store.Table[InsightRule]
	metricStreams    *store.Table[MetricStream]
	alarmMuteRules   *store.Table[AlarmMuteRule]
	metricFilters    *store.Table[MetricFilter]
	registry         *store.Registry
	snsPublisher     SNSPublisher
	lambdaInvoker    LambdaInvoker
	ec2Actioner      EC2InstanceActioner
	asgExecutor      AutoScalingPolicyExecutor
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
	b := &InMemoryBackend{
		accountID:    accountID,
		region:       region,
		metrics:      make(map[string]map[string]*metricRecord),
		alarmHistory: make(map[string][]AlarmHistoryItem),
		registry:     store.NewRegistry(),
		mu:           lockmetrics.New("cloudwatch"),
	}

	registerAllTables(b)

	return b
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

// SetEC2Actioner registers an EC2 actioner used to execute arn:aws:automate EC2
// alarm actions (stop/terminate/reboot/recover).
func (b *InMemoryBackend) SetEC2Actioner(a EC2InstanceActioner) {
	b.mu.Lock("SetEC2Actioner")
	defer b.mu.Unlock()
	b.ec2Actioner = a
}

// SetAutoScalingExecutor registers an Auto Scaling executor used to run
// scaling-policy alarm actions.
func (b *InMemoryBackend) SetAutoScalingExecutor(e AutoScalingPolicyExecutor) {
	b.mu.Lock("SetAutoScalingExecutor")
	defer b.mu.Unlock()
	b.asgExecutor = e
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

	for _, d := range data {
		if err := validateMetricDatum(d); err != nil {
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

	b.mu.Lock("PutMetricData")

	if b.metrics[namespace] == nil {
		b.metrics[namespace] = make(map[string]*metricRecord)
	}

	if err := b.validatePutMetricDataBatch(namespace, data); err != nil {
		b.mu.Unlock()

		return err
	}

	for _, d := range data {
		d.Namespace = namespace
		b.storeDatum(namespace, d)
	}

	// Collect matching running stream names while holding the write lock; the
	// actual timestamp update happens in a second, shorter lock acquisition so
	// the main metrics write lock is not held during filter iteration.
	matchingStreams := b.matchingRunningStreamNames(namespace, data)

	b.mu.Unlock()

	// Update LastUpdateDate for matched streams outside the metrics write lock.
	if len(matchingStreams) > 0 {
		now := time.Now().UTC()
		b.mu.Lock("PutMetricData.streamDelivery")
		for _, name := range matchingStreams {
			if s, ok := b.metricStreams.Get(name); ok && s.State == metricStreamStateRunning {
				s.LastUpdateDate = now
			}
		}
		b.mu.Unlock()
	}

	return nil
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

// matchingRunningStreamNames returns the names of running metric streams that
// allow at least one datum in data. Caller must hold b.mu (any lock).
// The caller updates LastUpdateDate in a separate, shorter lock acquisition to
// avoid holding the metrics write lock during the full stream-filter scan.
func (b *InMemoryBackend) matchingRunningStreamNames(
	namespace string,
	data []MetricDatum,
) []string {
	var names []string

	for _, s := range b.metricStreams.All() {
		if s.State != metricStreamStateRunning {
			continue
		}

		for _, d := range data {
			if streamAllowsMetric(s, namespace, d.MetricName) {
				names = append(names, s.Name)

				break
			}
		}
	}

	return names
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

	return b.resolveMetricDataQueries(queries, startTime, endTime, scanBy), nil
}

// resolveMetricDataQueries resolves every query into a MetricDataResult, preserving
// query order and honouring ReturnData / ScanBy. Per-result diagnostic Messages and
// the PartialData status are attached where metric math produced NaN/Inf.
// Caller must hold b.mu (at least a read lock).
func (b *InMemoryBackend) resolveMetricDataQueries(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
	scanBy string,
) []MetricDataResult {
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
		r := evalExpression(q, resolved)
		annotateArithmeticMessages(&r)
		resolved[id] = r
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

	return results
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
func (b *InMemoryBackend) resolveMetricStat(
	q MetricDataQuery,
	startTime, endTime time.Time,
) MetricDataResult {
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

// PutMetricAlarm creates or updates an alarm.
func (b *InMemoryBackend) PutMetricAlarm(alarm *MetricAlarm) error {
	if alarm.AlarmName == "" {
		return ErrAlarmNameRequired
	}

	// AWS validation: Statistic and ExtendedStatistic are mutually exclusive.
	if alarm.Statistic != "" && alarm.ExtendedStatistic != "" {
		return fmt.Errorf(
			"%w: Statistic and ExtendedStatistic are mutually exclusive",
			ErrValidation,
		)
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

	isNew := !b.alarms.Has(alarm.AlarmName)

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
	if existing, ok := b.alarms.Get(alarm.AlarmName); ok {
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
	b.alarms.Put(&cp)

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

	isNew := !b.compositeAlarms.Has(alarm.AlarmName)

	if alarm.AlarmArn == "" {
		alarm.AlarmArn = arn.Build("cloudwatch", b.region, b.accountID, "alarm:"+alarm.AlarmName)
	}
	if alarm.CreatedAt.IsZero() {
		alarm.CreatedAt = time.Now()
	}

	// Evaluate state based on AlarmRule and current child alarm states.
	newState := b.evalCompositeRule(alarm.AlarmRule)
	if existing, ok := b.compositeAlarms.Get(alarm.AlarmName); ok {
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
	b.compositeAlarms.Put(&cp)

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
func (b *InMemoryBackend) evalCompositeRuleDepth(
	rule string,
	visited map[string]bool,
	depth int,
) string {
	if depth > cwMaxCompositeEvalDepth {
		return alarmStateInsufficientData
	}

	resolve := func(name string) string {
		if a, ok := b.alarms.Get(name); ok {
			return a.StateValue
		}
		if ca, ok := b.compositeAlarms.Get(name); ok {
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
	compositeResult := b.collectCompositeAlarms(
		nameSet,
		alarmNamePrefix,
		stateValue,
		includeComposite,
	)

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

	for _, alarm := range b.alarms.All() {
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

	for _, alarm := range b.compositeAlarms.All() {
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
	for _, alarm := range b.alarms.All() {
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
		b.alarms.Delete(name)
		b.compositeAlarms.Delete(name)
		// Release the per-alarm history so it cannot accumulate across the
		// lifetime of the backend once the alarm itself is gone.
		delete(b.alarmHistory, name)
	}

	return nil
}

// SetAlarmState manually sets the state of an alarm and fires the corresponding actions.
func (b *InMemoryBackend) SetAlarmState(
	ctx context.Context,
	alarmName, stateValue, stateReason, stateReasonData string,
) error {
	b.mu.Lock("SetAlarmState")

	metricAlarm, hasMetric := b.alarms.Get(alarmName)
	compositeAlarm, hasComposite := b.compositeAlarms.Get(alarmName)

	if !hasMetric && !hasComposite {
		b.mu.Unlock()

		return fmt.Errorf("%w: %s", ErrAlarmNotFound, alarmName)
	}

	var oldState string
	var alarmArn string
	var alarmDesc string
	var alarmActions, okActions, insuffActions []string
	var actionsEnabled bool
	var instanceIDs []string

	if hasMetric {
		oldState = metricAlarm.StateValue
		alarmArn = metricAlarm.AlarmArn
		alarmDesc = metricAlarm.AlarmDescription
		alarmActions = metricAlarm.AlarmActions
		okActions = metricAlarm.OKActions
		insuffActions = metricAlarm.InsufficientDataActions
		actionsEnabled = metricAlarm.ActionsEnabled
		instanceIDs = instanceIDsFromDimensions(metricAlarm.Dimensions)

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

	deps := alarmActionDeps{
		snsPub:      b.snsPublisher,
		lambdaInv:   b.lambdaInvoker,
		ec2:         b.ec2Actioner,
		asg:         b.asgExecutor,
		instanceIDs: instanceIDs,
	}
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

		payload := b.buildAlarmActionPayload(
			alarmName,
			alarmDesc,
			alarmArn,
			oldState,
			stateValue,
			stateReason,
		)
		b.executeActions(ctx, actions, alarmName, histAlarmType, payload, deps)
	}

	b.fireCompositeTransitions(ctx, compositeTransitions, deps)

	return nil
}

// fireCompositeTransitions fires the actions for composite alarms that changed
// state. Composite alarms have no metric dimensions, so EC2 instance IDs are not
// carried over — only the SNS/Lambda collaborators are reused.
func (b *InMemoryBackend) fireCompositeTransitions(
	ctx context.Context,
	transitions []compositeAlarmTransition,
	deps alarmActionDeps,
) {
	compositeDeps := alarmActionDeps{snsPub: deps.snsPub, lambdaInv: deps.lambdaInv}
	for _, tr := range transitions {
		payload := b.buildAlarmActionPayload(
			tr.alarmName, tr.alarmDesc, tr.alarmArn,
			tr.oldState, tr.newState, tr.reason,
		)
		b.executeActions(ctx, tr.actions, tr.alarmName, "CompositeAlarm", payload, compositeDeps)
	}
}

// alarmActionDeps carries the collaborators needed to fire alarm actions plus the
// instance IDs (from the alarm's InstanceId dimension) that EC2 automate actions
// target.
type alarmActionDeps struct {
	snsPub      SNSPublisher
	lambdaInv   LambdaInvoker
	ec2         EC2InstanceActioner
	asg         AutoScalingPolicyExecutor
	instanceIDs []string
}

// instanceIDsFromDimensions extracts EC2 instance IDs from an alarm's dimensions.
// CloudWatch fires arn:aws:automate EC2 actions against the instance named by the
// alarm's "InstanceId" dimension.
func instanceIDsFromDimensions(dims []Dimension) []string {
	var ids []string
	for _, d := range dims {
		if d.Name == "InstanceId" && d.Value != "" {
			ids = append(ids, d.Value)
		}
	}

	return ids
}

// EnableAlarmActions enables action execution for the given alarms.
func (b *InMemoryBackend) EnableAlarmActions(alarmNames []string) error {
	b.mu.Lock("EnableAlarmActions")
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		if a, ok := b.alarms.Get(name); ok {
			a.ActionsEnabled = true
		}
		if ca, ok := b.compositeAlarms.Get(name); ok {
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
		if a, ok := b.alarms.Get(name); ok {
			a.ActionsEnabled = false
		}
		if ca, ok := b.compositeAlarms.Get(name); ok {
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
func (b *InMemoryBackend) stateChangeHistoryData(
	alarmName, oldState, newState, reason string,
) string {
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

// executeActions delivers the alarm action notifications to SNS topics, Lambda
// functions, EC2 instances (arn:aws:automate), and Auto Scaling scaling policies.
// Delivery errors are logged as warnings but do not prevent other actions from
// running. Each fired action is recorded as an Action history entry on the alarm,
// tagged with alarmTypeName ("MetricAlarm" or "CompositeAlarm") so
// DescribeAlarmHistory's AlarmType filter matches composite-alarm action entries
// too (previously hardcoded to "MetricAlarm" even when firing for a composite alarm).
func (b *InMemoryBackend) executeActions(
	ctx context.Context,
	actions []string,
	alarmName, alarmTypeName string,
	payload []byte,
	deps alarmActionDeps,
) {
	log := logger.Load(ctx)

	for _, action := range actions {
		var actionResult string

		switch {
		case strings.HasPrefix(action, "arn:aws:sns:"):
			actionResult = "SNS"
			if deps.snsPub != nil {
				if err := deps.snsPub.PublishToTopic(action, string(payload)); err != nil {
					log.WarnContext(ctx, "cloudwatch: alarm SNS action delivery failed",
						"topic_arn", action, "error", err)
					actionResult = "SNS (failed)"
				}
			}
		case strings.HasPrefix(action, "arn:aws:lambda:"):
			actionResult = "Lambda"
			if deps.lambdaInv != nil {
				if _, _, err := deps.lambdaInv.InvokeFunction(ctx, action, "Event", payload); err != nil {
					log.WarnContext(ctx, "cloudwatch: alarm Lambda action delivery failed",
						"function_arn", action, "error", err)
					actionResult = "Lambda (failed)"
				}
			}
		case strings.HasPrefix(action, "arn:aws:automate:"):
			actionResult = b.executeEC2Action(ctx, action, deps)
		case strings.HasPrefix(action, "arn:aws:autoscaling:"):
			actionResult = b.executeAutoScalingAction(ctx, action, deps)
		default:
			log.WarnContext(ctx, "cloudwatch: unrecognised alarm action skipped",
				"action", action)
			actionResult = "unknown (skipped)"
		}

		if alarmName != "" {
			summary := fmt.Sprintf("Alarm %q action executed: %s → %s", alarmName, action, actionResult)
			b.mu.Lock("executeActions-history")
			b.appendHistory(alarmName, alarmTypeName, historyTypeAction, summary, "")
			b.mu.Unlock()
		}
	}
}

// executeEC2Action performs an arn:aws:automate:<region>:ec2:<verb> alarm action
// against the instances named by the alarm's InstanceId dimension. It returns a
// short human-readable result string recorded in alarm history.
func (b *InMemoryBackend) executeEC2Action(
	ctx context.Context,
	action string,
	deps alarmActionDeps,
) string {
	log := logger.Load(ctx)

	verb, ok := parseEC2AutomateVerb(action)
	if !ok {
		log.WarnContext(ctx, "cloudwatch: unrecognised EC2 automate action", "action", action)

		return "EC2 automate (invalid ARN)"
	}

	if deps.ec2 == nil {
		return "EC2 " + verb + " (no EC2 backend wired)"
	}

	if len(deps.instanceIDs) == 0 {
		return "EC2 " + verb + " (no InstanceId dimension)"
	}

	var err error
	switch verb {
	case "stop":
		err = deps.ec2.StopInstances(deps.instanceIDs)
	case "terminate":
		err = deps.ec2.TerminateInstances(deps.instanceIDs)
	case "reboot":
		err = deps.ec2.RebootInstances(deps.instanceIDs)
	case "recover":
		// Recover keeps the instance running on new underlying hardware; there is
		// no observable state change to emulate, so it is a validated no-op.
		return "EC2 recover"
	default:
		return "EC2 automate (unsupported verb)"
	}

	if err != nil {
		log.WarnContext(ctx, "cloudwatch: EC2 alarm action failed",
			"action", action, "error", err)

		return "EC2 " + verb + " (failed)"
	}

	return "EC2 " + verb
}

// executeAutoScalingAction performs an Auto Scaling scaling-policy alarm action.
// The scaling-policy ARN encodes both the Auto Scaling group name and the policy
// name, which are parsed and passed to the executor.
func (b *InMemoryBackend) executeAutoScalingAction(
	ctx context.Context,
	action string,
	deps alarmActionDeps,
) string {
	log := logger.Load(ctx)

	asgName, policyName, ok := parseScalingPolicyARN(action)
	if !ok {
		log.WarnContext(ctx, "cloudwatch: unrecognised scaling-policy ARN", "action", action)

		return "AutoScaling (invalid ARN)"
	}

	if deps.asg == nil {
		return "AutoScaling (no AutoScaling backend wired)"
	}

	if err := deps.asg.ExecuteScalingPolicy(asgName, policyName); err != nil {
		log.WarnContext(ctx, "cloudwatch: AutoScaling alarm action failed",
			"action", action, "error", err)

		return "AutoScaling (failed)"
	}

	return "AutoScaling policy executed"
}

// parseEC2AutomateVerb extracts the action verb from an
// arn:aws:automate:<region>:ec2:<verb> ARN.
func parseEC2AutomateVerb(action string) (string, bool) {
	parts := strings.Split(action, ":")
	// arn:aws:automate:<region>:ec2:<verb> → 6 colon-separated parts.
	const automateParts = 6
	if len(parts) != automateParts || parts[4] != "ec2" {
		return "", false
	}

	switch parts[5] {
	case "stop", "terminate", "reboot", "recover":
		return parts[5], true
	}

	return "", false
}

// parseScalingPolicyARN extracts the Auto Scaling group name and policy name from a
// scaling-policy ARN of the form
// arn:aws:autoscaling:<region>:<account>:scalingPolicy:<uuid>:autoScalingGroupName/<asg>:policyName/<name>.
func parseScalingPolicyARN(action string) (string, string, bool) {
	var asgName, policyName string

	for seg := range strings.SplitSeq(action, ":") {
		switch {
		case strings.HasPrefix(seg, "autoScalingGroupName/"):
			asgName = strings.TrimPrefix(seg, "autoScalingGroupName/")
		case strings.HasPrefix(seg, "policyName/"):
			policyName = strings.TrimPrefix(seg, "policyName/")
		}
	}

	if asgName == "" || policyName == "" {
		return "", "", false
	}

	return asgName, policyName, true
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

	for _, ca := range b.compositeAlarms.All() {
		newState := b.evalCompositeRule(ca.AlarmRule)
		if newState == ca.StateValue {
			continue
		}

		oldState := ca.StateValue
		reason := "Rule evaluated to " + newState
		ca.StateValue = newState
		ca.StateReason = reason
		ca.StateTransitionedTimestamp = time.Now().UTC()
		summary := fmt.Sprintf(
			"Composite alarm %q changed from %s to %s",
			ca.AlarmName,
			oldState,
			newState,
		)
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

	b.dashboards.Put(&dashboardRecord{
		Name:         name,
		Body:         body,
		LastModified: time.Now().UTC(),
	})

	return nil
}

// GetDashboard returns the dashboard entry and body for the given name.
func (b *InMemoryBackend) GetDashboard(name string) (DashboardEntry, string, error) {
	b.mu.RLock("GetDashboard")
	defer b.mu.RUnlock()

	rec, ok := b.dashboards.Get(name)
	if !ok {
		return DashboardEntry{}, "", fmt.Errorf("%w: %s", ErrDashboardNotFound, name)
	}

	return b.toDashboardEntry(rec), rec.Body, nil
}

// ListDashboards returns a page of dashboard entries optionally filtered by name prefix.
func (b *InMemoryBackend) ListDashboards(
	prefix, nextToken string,
) (page.Page[DashboardEntry], error) {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	var result []DashboardEntry

	for _, rec := range b.dashboards.All() {
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
		b.dashboards.Delete(name)
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
	b.alarmHistory = make(map[string][]AlarmHistoryItem)
	b.registry.ResetAll()
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
	if strings.EqualFold(orderBy, statSum) {
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
func topNContributors(
	dimSums map[string]float64,
	dimKeys map[string][]string,
	maxN int,
) []AlarmContributor {
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
	if !b.insightRules.Has(ruleName) {
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
// Dimensions are included so different dimension sets produce distinct detectors.
func anomalyDetectorKey(namespace, metricName, stat string, dims []Dimension) string {
	return namespace + "/" + metricName + "/" + stat + "/" + dimensionSetKey(dims)
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

	if !b.alarmMuteRules.Has(muteName) {
		return fmt.Errorf("%w: %s", ErrAlarmMuteRuleNotFound, muteName)
	}

	b.alarmMuteRules.Delete(muteName)

	return nil
}

// GetAlarmMuteRule returns an alarm mute rule by name.
// Returns ErrAlarmMuteRuleNotFound if the rule does not exist.
func (b *InMemoryBackend) GetAlarmMuteRule(muteName string) (*AlarmMuteRule, error) {
	b.mu.RLock("GetAlarmMuteRule")
	defer b.mu.RUnlock()

	rule, ok := b.alarmMuteRules.Get(muteName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAlarmMuteRuleNotFound, muteName)
	}

	cp := *rule

	return &cp, nil
}

// ListAlarmMuteRules returns a paginated list of all alarm mute rules.
func (b *InMemoryBackend) ListAlarmMuteRules(
	nextToken string,
	maxResults int,
) (page.Page[AlarmMuteRule], error) {
	b.mu.RLock("ListAlarmMuteRules")
	defer b.mu.RUnlock()

	result := make([]AlarmMuteRule, 0, b.alarmMuteRules.Len())
	for _, rule := range b.alarmMuteRules.All() {
		result = append(result, *rule)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].MuteName < result[j].MuteName })

	return page.New(result, nextToken, maxResults, cwDefaultListAlarmMuteRulesLimit), nil
}

// ListManagedInsightRules returns a paginated list of managed (service-linked) insight rules.
// If resourceARN is non-empty only rules whose Arn matches are included; in the emulator the
// ManagedRule flag is used as the primary discriminator.
func (b *InMemoryBackend) ListManagedInsightRules(
	resourceARN, nextToken string,
	maxResults int,
) (page.Page[InsightRule], error) {
	b.mu.RLock("ListManagedInsightRules")
	defer b.mu.RUnlock()

	result := make([]InsightRule, 0)
	for _, rule := range b.insightRules.All() {
		if !rule.ManagedRule {
			continue
		}
		if resourceARN != "" && rule.Arn != resourceARN {
			continue
		}
		result = append(result, *rule)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return page.New(result, nextToken, maxResults, cwDefaultListManagedInsightRulesLimit), nil
}

// PutAlarmMuteRuleInternal creates or updates an alarm mute rule (used for test seeding).
func (b *InMemoryBackend) PutAlarmMuteRuleInternal(rule *AlarmMuteRule) {
	b.mu.Lock("PutAlarmMuteRuleInternal")
	defer b.mu.Unlock()

	cp := *rule
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now().UTC()
	}

	b.alarmMuteRules.Put(&cp)
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
func (b *InMemoryBackend) PutAnomalyDetectorInternal(detector *AnomalyDetector) {
	b.mu.Lock("PutAnomalyDetectorInternal")
	defer b.mu.Unlock()

	cp := *detector
	if cp.StateValue == "" {
		// TRAINED_INSUFFICIENT_DATA is the realistic initial state for a new detector.
		cp.StateValue = statusTrainedInsufficient
	}

	b.anomalyDetectors.Put(&cp)
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

	for _, d := range b.anomalyDetectors.All() {
		if namespace != "" && d.Namespace != namespace {
			continue
		}

		if metricName != "" && d.MetricName != metricName {
			continue
		}

		k := anomalyDetectorKey(d.Namespace, d.MetricName, d.Stat, d.Dimensions)
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
		if !b.insightRules.Has(name) {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		b.insightRules.Delete(name)
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

	rule, ok := b.insightRules.Get(name)
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
		cp.State = insightRuleStateEnabled
	}

	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}

	if cp.Arn == "" {
		cp.Arn = arn.Build("cloudwatch", b.region, b.accountID, "insight-rule/"+rule.Name)
	}

	b.insightRules.Put(&cp)
}

// DescribeInsightRules returns a paginated list of insight rules.
func (b *InMemoryBackend) DescribeInsightRules(
	nextToken string,
	maxResults int,
) (page.Page[InsightRule], error) {
	b.mu.RLock("DescribeInsightRules")
	defer b.mu.RUnlock()

	result := make([]InsightRule, 0, b.insightRules.Len())

	for _, r := range b.insightRules.All() {
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
		rule, ok := b.insightRules.Get(name)
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
		rule, ok := b.insightRules.Get(name)
		if !ok {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		rule.State = insightRuleStateEnabled
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

	stream, ok := b.metricStreams.Get(name)
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

	if !b.metricStreams.Has(name) {
		return fmt.Errorf("%w: %s", ErrMetricStreamNotFound, name)
	}

	b.metricStreams.Delete(name)

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
	if existing, ok := b.metricStreams.Get(stream.Name); ok && cp.State == "" {
		cp.State = existing.State
	}

	if cp.State == "" {
		cp.State = metricStreamStateRunning
	}

	b.metricStreams.Put(&cp)
}

// DescribeAlarmContributors returns a page of contributors for the specified alarm.
//
// For a composite alarm, the contributors are the referenced child alarms whose
// state currently satisfies their state-function condition (e.g. a child named in
// ALARM(...) that is itself in ALARM) — i.e. the alarms actually driving the
// composite alarm's state. Each contributor is keyed by [childAlarmName, state]
// with Sum=1. For a metric alarm, AWS returns no contributors, so the result is an
// empty (but successful) page.
func (b *InMemoryBackend) DescribeAlarmContributors(
	alarmName, nextToken string,
) (page.Page[AlarmContributor], error) {
	b.mu.RLock("DescribeAlarmContributors")
	defer b.mu.RUnlock()

	if b.alarms.Has(alarmName) {
		// Metric alarms have no contributors in AWS.
		return page.New(
			[]AlarmContributor{},
			nextToken,
			0,
			cwDefaultDescribeAlarmContributorsLimit,
		), nil
	}

	ca, ok := b.compositeAlarms.Get(alarmName)
	if !ok {
		return page.Page[AlarmContributor]{}, fmt.Errorf("%w: %s", ErrAlarmNotFound, alarmName)
	}

	contributors := b.compositeContributorsLocked(ca)

	return page.New(
		contributors,
		nextToken,
		0,
		cwDefaultDescribeAlarmContributorsLimit,
	), nil
}

// compositeContributorsLocked computes the child alarms currently satisfying their
// state-function condition in a composite alarm's rule. Caller must hold b.mu.
func (b *InMemoryBackend) compositeContributorsLocked(ca *CompositeAlarm) []AlarmContributor {
	refs := extractAlarmRuleRefs(ca.AlarmRule)

	resolve := func(name string) string {
		if a, ok := b.alarms.Get(name); ok {
			return a.StateValue
		}
		if child, ok := b.compositeAlarms.Get(name); ok {
			return child.StateValue
		}

		return alarmStateInsufficientData
	}

	seen := make(map[string]bool, len(refs))
	contributors := make([]AlarmContributor, 0, len(refs))

	for _, ref := range refs {
		if seen[ref.Name] {
			continue
		}

		state := resolve(ref.Name)
		// A child alarm contributes only when its actual state matches the state
		// its state-function tests for (that is the condition currently firing).
		if state != ref.Func {
			continue
		}

		seen[ref.Name] = true
		contributors = append(contributors, AlarmContributor{
			Keys: []string{ref.Name, state},
			Sum:  1,
		})
	}

	return contributors
}

// GetAlarmARNs returns the ARNs for the given alarm names (metric + composite).
// Used by the HTTP handler to clean up tag entries on delete.
func (b *InMemoryBackend) GetAlarmARNs(names []string) []string {
	b.mu.RLock("GetAlarmARNs")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(names))
	for _, name := range names {
		if a, ok := b.alarms.Get(name); ok && a.AlarmArn != "" {
			arns = append(arns, a.AlarmArn)
		}
		if ca, ok := b.compositeAlarms.Get(name); ok && ca.AlarmArn != "" {
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
		if b.dashboards.Has(name) {
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
func (b *InMemoryBackend) ListMetricStreams(
	nextToken string,
	maxResults int,
) (page.Page[MetricStream], error) {
	b.mu.RLock("ListMetricStreams")
	defer b.mu.RUnlock()

	result := make([]MetricStream, 0, b.metricStreams.Len())
	for _, s := range b.metricStreams.All() {
		result = append(result, *s)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, nextToken, maxResults, cwDefaultListMetricStreamsLimit), nil
}

const (
	metricStreamStateRunning = "RUNNING"
	metricStreamStateStopped = "STOPPED"
)

// StartMetricStreams sets the State of the named streams to RUNNING.
// Names that do not exist are silently ignored (AWS behaviour).
func (b *InMemoryBackend) StartMetricStreams(names []string) error {
	b.mu.Lock("StartMetricStreams")
	defer b.mu.Unlock()

	for _, name := range names {
		if s, ok := b.metricStreams.Get(name); ok {
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
		if s, ok := b.metricStreams.Get(name); ok {
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

	b.metricFilters.Put(&cp)

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
	for _, f := range b.metricFilters.All() {
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
	if !b.metricFilters.Has(key) {
		return fmt.Errorf("%w: %s/%s", ErrMetricFilterNotFound, logGroupName, filterName)
	}

	b.metricFilters.Delete(key)

	return nil
}

// EvaluateAlarms evaluates all metric alarms against recent metric data and
// transitions state (OK ↔ ALARM ↔ INSUFFICIENT_DATA) as appropriate. SNS and
// Lambda alarm actions are fired on state change. Intended to be called
// periodically by the Janitor.
func (b *InMemoryBackend) EvaluateAlarms(ctx context.Context, now time.Time) {
	// Snapshot alarms under a read-lock to avoid holding the lock during evaluation.
	b.mu.RLock("EvaluateAlarms.snapshot")

	type alarmSnap struct {
		alarm MetricAlarm
	}

	var snaps []alarmSnap

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

	b.mu.RUnlock()

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
