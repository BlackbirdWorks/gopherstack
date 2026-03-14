package cloudwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// ErrAlarmNotFound is returned when a requested alarm does not exist.
var ErrAlarmNotFound = errors.New("ResourceNotFoundException")

// ErrAlarmNameRequired is returned when an alarm name is missing.
var ErrAlarmNameRequired = errors.New("AlarmName is required")

// ErrAlarmRuleRequired is returned when a composite alarm rule is missing.
var ErrAlarmRuleRequired = errors.New("AlarmRule is required")

// ErrDashboardNotFound is returned when a requested dashboard does not exist.
var ErrDashboardNotFound = errors.New("ResourceNotFoundException")

// ErrDashboardNameRequired is returned when a dashboard name is missing.
var ErrDashboardNameRequired = errors.New("DashboardName is required")

const (
	cwDefaultListMetricsLimit       = 500
	cwDefaultDescribeAlarmsLimit    = 100
	cwDefaultAlarmHistoryLimit      = 100
	cwDefaultDescribeForMetricLimit = 100
	cwDefaultListDashboardsLimit    = 300
	cwMaxMetricDataPoints           = 1000 // maximum data points retained per metric
	cwMaxAlarmHistory               = 100  // maximum alarm history entries per alarm

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
	PutMetricData(namespace string, data []MetricDatum) error
	GetMetricStatistics(
		namespace, metricName string,
		startTime, endTime time.Time,
		period int32,
		statistics []string,
	) ([]Datapoint, error)
	GetMetricData(queries []MetricDataQuery, startTime, endTime time.Time) ([]MetricDataResult, error)
	ListMetrics(namespace, metricName, nextToken string, maxResults int) (page.Page[Metric], error)
	PutMetricAlarm(alarm *MetricAlarm) error
	PutCompositeAlarm(alarm *CompositeAlarm) error
	DescribeAlarms(
		alarmNames []string,
		alarmTypes []string,
		stateValue, nextToken string,
		maxRecords int,
	) (page.Page[MetricAlarm], page.Page[CompositeAlarm], error)
	DescribeAlarmsForMetric(
		namespace, metricName string,
		alarmNames []string,
		nextToken string,
		maxRecords int,
	) (page.Page[MetricAlarm], error)
	DescribeAlarmHistory(
		alarmName, historyItemType, nextToken string,
		startDate, endDate time.Time,
		maxRecords int,
	) (page.Page[AlarmHistoryItem], error)
	DeleteAlarms(alarmNames []string) error
	SetAlarmState(alarmName, stateValue, stateReason string) error
	EnableAlarmActions(alarmNames []string) error
	DisableAlarmActions(alarmNames []string) error
	PutDashboard(name, body string) error
	GetDashboard(name string) (DashboardEntry, string, error)
	ListDashboards(prefix, nextToken string) (page.Page[DashboardEntry], error)
	DeleteDashboards(names []string) error
}

// InMemoryBackend implements StorageBackend using in-memory maps.
// metrics is a two-level map: namespace -> metricName -> []MetricDatum.
type InMemoryBackend struct {
	metrics         map[string]map[string][]MetricDatum
	alarms          map[string]*MetricAlarm
	compositeAlarms map[string]*CompositeAlarm
	alarmHistory    map[string][]AlarmHistoryItem
	dashboards      map[string]*dashboardRecord
	snsPublisher    SNSPublisher
	lambdaInvoker   LambdaInvoker
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// dashboardRecord holds dashboard body and metadata.
type dashboardRecord struct {
	lastModified time.Time
	name         string
	body         string
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:       accountID,
		region:          region,
		metrics:         make(map[string]map[string][]MetricDatum),
		alarms:          make(map[string]*MetricAlarm),
		compositeAlarms: make(map[string]*CompositeAlarm),
		alarmHistory:    make(map[string][]AlarmHistoryItem),
		dashboards:      make(map[string]*dashboardRecord),
		mu:              lockmetrics.New("cloudwatch"),
	}
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
func (b *InMemoryBackend) PutMetricData(namespace string, data []MetricDatum) error {
	b.mu.Lock("PutMetricData")
	defer b.mu.Unlock()

	if b.metrics[namespace] == nil {
		b.metrics[namespace] = make(map[string][]MetricDatum)
	}
	for _, d := range data {
		d.Namespace = namespace
		b.metrics[namespace][d.MetricName] = append(b.metrics[namespace][d.MetricName], d)
		// Cap data points to prevent unbounded memory growth.
		if pts := b.metrics[namespace][d.MetricName]; len(pts) > cwMaxMetricDataPoints {
			b.metrics[namespace][d.MetricName] = pts[len(pts)-cwMaxMetricDataPoints:]
		}
	}

	return nil
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
func (b *InMemoryBackend) GetMetricStatistics(
	namespace, metricName string,
	startTime, endTime time.Time,
	period int32,
	statistics []string,
) ([]Datapoint, error) {
	b.mu.RLock("GetMetricStatistics")
	defer b.mu.RUnlock()

	var all []MetricDatum
	if nsMap, ok := b.metrics[namespace]; ok {
		all = nsMap[metricName]
	}

	buckets := populateBuckets(all, startTime, endTime, period)

	statSet := make(map[string]bool, len(statistics))
	for _, s := range statistics {
		statSet[s] = true
	}

	datapoints := make([]Datapoint, 0, len(buckets))
	for _, bk := range buckets {
		if bk.count == 0 {
			continue
		}

		datapoints = append(datapoints, buildDatapoint(bk, statSet))
	}

	sort.Slice(datapoints, func(i, j int) bool {
		return datapoints[i].Timestamp.Before(datapoints[j].Timestamp)
	})

	return datapoints, nil
}

// GetMetricData executes multiple metric queries and returns results.
// Each query specifies a namespace, metric name, statistic, and period.
func (b *InMemoryBackend) GetMetricData(
	queries []MetricDataQuery,
	startTime, endTime time.Time,
) ([]MetricDataResult, error) {
	b.mu.RLock("GetMetricData")
	defer b.mu.RUnlock()

	results := make([]MetricDataResult, 0, len(queries))

	for _, q := range queries {
		ns := q.MetricStat.Namespace
		metricName := q.MetricStat.MetricName
		period := q.MetricStat.Period
		stat := q.MetricStat.Stat

		var all []MetricDatum
		if nsMap, ok := b.metrics[ns]; ok {
			all = nsMap[metricName]
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

		label := q.Label
		if label == "" {
			label = metricName
		}

		results = append(results, MetricDataResult{
			ID:         q.ID,
			Label:      label,
			Timestamps: timestamps,
			Values:     values,
			StatusCode: "Complete",
		})
	}

	return results, nil
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

// ListMetrics returns a page of unique metrics matching optional namespace and metricName filters.
func (b *InMemoryBackend) ListMetrics(
	namespace, metricName, nextToken string,
	maxResults int,
) (page.Page[Metric], error) {
	b.mu.RLock("ListMetrics")
	defer b.mu.RUnlock()

	var result []Metric
	for ns, nsMap := range b.metrics {
		if namespace != "" && ns != namespace {
			continue
		}
		for mn := range nsMap {
			if metricName != "" && mn != metricName {
				continue
			}
			result = append(result, Metric{Namespace: ns, MetricName: mn})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}

		return result[i].MetricName < result[j].MetricName
	})

	return page.New(result, nextToken, maxResults, cwDefaultListMetricsLimit), nil
}

// PutMetricAlarm creates or updates an alarm.
func (b *InMemoryBackend) PutMetricAlarm(alarm *MetricAlarm) error {
	if alarm.AlarmName == "" {
		return ErrAlarmNameRequired
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
	if alarm.CreatedAt.IsZero() {
		alarm.CreatedAt = time.Now()
	}

	cp := *alarm
	b.alarms[alarm.AlarmName] = &cp

	histType := historyTypeConfigurationUpdate
	historySummary := fmt.Sprintf("Alarm %q updated", alarm.AlarmName)
	if isNew {
		historySummary = fmt.Sprintf("Alarm %q created", alarm.AlarmName)
	}
	b.appendHistory(alarm.AlarmName, histType, historySummary, "")

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
	alarm.StateValue = newState
	if alarm.StateReason == "" {
		alarm.StateReason = fmt.Sprintf("Rule evaluated to %s", newState)
	}

	cp := *alarm
	b.compositeAlarms[alarm.AlarmName] = &cp

	histType := historyTypeConfigurationUpdate
	historySummary := fmt.Sprintf("Composite alarm %q updated", alarm.AlarmName)
	if isNew {
		historySummary = fmt.Sprintf("Composite alarm %q created", alarm.AlarmName)
	}
	b.appendHistory(alarm.AlarmName, histType, historySummary, "")

	return nil
}

// evalCompositeRule evaluates the composite alarm rule using current alarm states.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) evalCompositeRule(rule string) string {
	resolve := func(name string) string {
		if a, ok := b.alarms[name]; ok {
			return a.StateValue
		}
		if ca, ok := b.compositeAlarms[name]; ok {
			return ca.StateValue
		}

		return alarmStateInsufficientData
	}

	return evaluateAlarmRule(rule, resolve)
}

// DescribeAlarms lists a page of alarms, optionally filtered by name, type, and/or state.
// alarmTypes can contain "MetricAlarm", "CompositeAlarm", or both (empty means both).
// MaxRecords applies to the total combined result set (metric + composite).
func (b *InMemoryBackend) DescribeAlarms(
	alarmNames []string,
	alarmTypes []string,
	stateValue, nextToken string,
	maxRecords int,
) (page.Page[MetricAlarm], page.Page[CompositeAlarm], error) {
	b.mu.RLock("DescribeAlarms")
	defer b.mu.RUnlock()

	nameSet := toSet(alarmNames)
	typeSet := toSet(alarmTypes)
	includeMetric := len(typeSet) == 0 || typeSet["MetricAlarm"]
	includeComposite := len(typeSet) == 0 || typeSet["CompositeAlarm"]

	metricResult := b.collectMetricAlarms(nameSet, stateValue, includeMetric)
	compositeResult := b.collectCompositeAlarms(nameSet, stateValue, includeComposite)

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
func (b *InMemoryBackend) collectMetricAlarms(nameSet map[string]bool, stateValue string, include bool) []MetricAlarm {
	if !include {
		return nil
	}

	var result []MetricAlarm

	for _, alarm := range b.alarms {
		if len(nameSet) > 0 && !nameSet[alarm.AlarmName] {
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
	stateValue string,
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
		result = append(result, *alarm)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AlarmName < result[j].AlarmName
	})

	return page.New(result, nextToken, maxRecords, cwDefaultDescribeForMetricLimit), nil
}

// DescribeAlarmHistory returns history items for one or all alarms, filtered by type and date range.
func (b *InMemoryBackend) DescribeAlarmHistory(
	alarmName, historyItemType, nextToken string,
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
			if historyItemType != "" && item.HistoryItemType != historyItemType {
				continue
			}
			if !startDate.IsZero() && item.Timestamp.Before(startDate) {
				continue
			}
			if !endDate.IsZero() && item.Timestamp.After(endDate) {
				continue
			}
			result = append(result, item)
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
func (b *InMemoryBackend) SetAlarmState(alarmName, stateValue, stateReason string) error {
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
	}

	summary := fmt.Sprintf("Alarm %q changed from %s to %s", alarmName, oldState, stateValue)
	histData := b.stateChangeHistoryData(alarmName, oldState, stateValue, stateReason)
	b.appendHistory(alarmName, historyTypeStateUpdate, summary, histData)

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
		b.executeActions(actions, alarmName, payload, snsPub, lambdaInv)
	}

	// fire actions for any composite alarms that changed state
	for _, tr := range compositeTransitions {
		payload := b.buildAlarmActionPayload(
			tr.alarmName, tr.alarmDesc, tr.alarmArn,
			tr.oldState, tr.newState, tr.reason,
		)
		b.executeActions(tr.actions, tr.alarmName, payload, snsPub, lambdaInv)
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
func (b *InMemoryBackend) appendHistory(alarmName, itemType, summary, data string) {
	item := AlarmHistoryItem{
		Timestamp:       time.Now(),
		AlarmName:       alarmName,
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
		"AlarmName":      alarmName,
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
		"AlarmName":        alarmName,
		"AlarmDescription": alarmDesc,
		"AlarmArn":         alarmArn,
		"AWSAccountId":     b.accountID,
		"Region":           b.region,
		"NewStateValue":    newState,
		"NewStateReason":   reason,
		"OldStateValue":    oldState,
		"StateChangeTime":  time.Now().UTC().Format(time.RFC3339),
	}
	// map[string]string marshaling cannot fail; error is intentionally ignored.
	bs, _ := json.Marshal(data)

	return bs
}

// executeActions delivers the alarm action notifications to SNS topics and Lambda functions.
// Delivery errors are logged as warnings but do not prevent other actions from running.
func (b *InMemoryBackend) executeActions(
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
					slog.Default().Warn("cloudwatch: alarm SNS action delivery failed",
						"topic_arn", action, "error", err)
				}
			}
		case strings.HasPrefix(action, "arn:aws:lambda:"):
			if lambdaInv != nil {
				if _, _, err := lambdaInv.InvokeFunction(context.Background(), action, "Event", payload); err != nil {
					slog.Default().Warn("cloudwatch: alarm Lambda action delivery failed",
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
		reason := fmt.Sprintf("Rule evaluated to %s", newState)
		ca.StateValue = newState
		ca.StateReason = reason
		summary := fmt.Sprintf("Composite alarm %q changed from %s to %s", ca.AlarmName, oldState, newState)
		histData := b.stateChangeHistoryData(ca.AlarmName, oldState, newState, reason)
		b.appendHistory(ca.AlarmName, historyTypeStateUpdate, summary, histData)

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
		name:         name,
		body:         body,
		lastModified: time.Now().UTC(),
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

	return b.toDashboardEntry(rec), rec.body, nil
}

// ListDashboards returns a page of dashboard entries optionally filtered by name prefix.
func (b *InMemoryBackend) ListDashboards(prefix, nextToken string) (page.Page[DashboardEntry], error) {
	b.mu.RLock("ListDashboards")
	defer b.mu.RUnlock()

	var result []DashboardEntry

	for _, rec := range b.dashboards {
		if prefix != "" && !strings.HasPrefix(rec.name, prefix) {
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
		DashboardName: rec.name,
		DashboardArn:  arn.Build("cloudwatch", b.region, b.accountID, "dashboard/"+rec.name),
		LastModified:  rec.lastModified,
		Size:          int64(len(rec.body)),
	}
}
