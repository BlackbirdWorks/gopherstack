package cloudwatch

import (
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// ErrAlarmNotFound is returned when a requested alarm does not exist.
var ErrAlarmNotFound = errors.New("ResourceNotFoundException")

// ErrAlarmNameRequired is returned when an alarm name is missing.
var ErrAlarmNameRequired = errors.New("AlarmName is required")

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
	ListMetrics(namespace, metricName string) ([]Metric, error)
	PutMetricAlarm(alarm *MetricAlarm) error
	DescribeAlarms(alarmNames []string, stateValue string) ([]MetricAlarm, error)
	DeleteAlarms(alarmNames []string) error
}

// InMemoryBackend implements StorageBackend using in-memory maps.
// metrics is a two-level map: namespace -> metricName -> []MetricDatum.
type InMemoryBackend struct {
	metrics   map[string]map[string][]MetricDatum
	alarms    map[string]*MetricAlarm
	accountID string
	region    string
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend with default configuration.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with given account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID: accountID,
		region:    region,
		metrics:   make(map[string]map[string][]MetricDatum),
		alarms:    make(map[string]*MetricAlarm),
	}
}

// PutMetricData stores metric data points for the given namespace.
func (b *InMemoryBackend) PutMetricData(namespace string, data []MetricDatum) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.metrics[namespace] == nil {
		b.metrics[namespace] = make(map[string][]MetricDatum)
	}
	for _, d := range data {
		d.Namespace = namespace
		b.metrics[namespace][d.MetricName] = append(b.metrics[namespace][d.MetricName], d)
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
	b.mu.RLock()
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
	b.mu.RLock()
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

// ListMetrics returns unique metrics matching optional namespace and metricName filters.
func (b *InMemoryBackend) ListMetrics(namespace, metricName string) ([]Metric, error) {
	b.mu.RLock()
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

	return result, nil
}

// PutMetricAlarm creates or updates an alarm.
func (b *InMemoryBackend) PutMetricAlarm(alarm *MetricAlarm) error {
	if alarm.AlarmName == "" {
		return ErrAlarmNameRequired
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if alarm.AlarmArn == "" {
		alarm.AlarmArn = arn.Build("cloudwatch", b.region, b.accountID, "alarm:"+alarm.AlarmName)
	}
	if alarm.StateValue == "" {
		alarm.StateValue = "INSUFFICIENT_DATA"
	}
	if alarm.CreatedAt.IsZero() {
		alarm.CreatedAt = time.Now()
	}

	cp := *alarm
	b.alarms[alarm.AlarmName] = &cp

	return nil
}

// DescribeAlarms lists alarms, optionally filtered by name and/or state.
func (b *InMemoryBackend) DescribeAlarms(alarmNames []string, stateValue string) ([]MetricAlarm, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	nameSet := make(map[string]bool, len(alarmNames))
	for _, n := range alarmNames {
		nameSet[n] = true
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

	return result, nil
}

// DeleteAlarms removes alarms by name.
func (b *InMemoryBackend) DeleteAlarms(alarmNames []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		delete(b.alarms, name)
	}

	return nil
}
