package gcpmonitoring

import (
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
)

const (
	userMetricPrefix = "logging.googleapis.com/user/"
	// logHistory is how many one-minute marks before now alert evaluation sees the accumulated total at.
	logHistory = 60
)

// LogMetricSeries is the accumulated value of one label combination of a log-based metric.
type LogMetricSeries struct {
	Labels        map[string]string
	BucketOptions *distribution.Distribution_BucketOptions
	BucketCounts  []int64
	Count         int64
	Sum           float64
	SumSqDev      float64
}

// LogMetric is a Cloud Logging log-based metric with its accumulated series.
type LogMetric struct {
	CreateTime time.Time
	Descriptor *metricpb.MetricDescriptor
	Name       string
	Series     []LogMetricSeries
}

// LogMetricSource lists the log-based metrics of a project; it is the seam to Cloud Logging.
type LogMetricSource interface {
	LogMetrics(project string) []LogMetric
}

func mayMatchLogMetrics(filter string) bool {
	for _, hint := range []string{"logging", opStartsWith, opHasSubstring, "regex", opOneOf, ":"} {
		if strings.Contains(filter, hint) {
			return true
		}
	}

	return false
}

// logMetrics fetches the project's log-based metrics from the seam without holding the backend lock.
func (b *InMemoryBackend) logMetrics(project string) []LogMetric {
	b.mu.RLock("logMetrics")
	src := b.logs
	b.mu.RUnlock()

	if src == nil {
		return nil
	}

	return slices.DeleteFunc(src.LogMetrics(project), func(m LogMetric) bool { return m.Descriptor == nil })
}

func logDescriptor(project string, lm LogMetric) *metricpb.MetricDescriptor {
	d := cloneDescriptor(lm.Descriptor)
	d.Type = userMetricPrefix + lm.Name
	d.Name = descriptorName(project, d.GetType())

	return d
}

// logSeries materialises the accumulated series of log-based metrics as DELTA points ending now.
func (b *InMemoryBackend) logSeries(project string, metrics []LogMetric, now time.Time, history int) []*seriesRec {
	var out []*seriesRec

	for _, lm := range metrics {
		for _, s := range lm.Series {
			out = append(out, logSeriesRec(project, lm, s, now, history))
		}
	}

	return out
}

func logSeriesRec(project string, lm LogMetric, s LogMetricSeries, now time.Time, history int) *seriesRec {
	start := lm.CreateTime
	if !start.Before(now) {
		start = now.Add(-time.Nanosecond)
	}

	vt := vtInt64
	val := intValue(s.Count)

	if lm.Descriptor.GetValueType() == vtDist {
		vt, val = vtDist, distributionOf(s)
	}

	rec := &seriesRec{
		project:  project,
		metric:   &metricpb.Metric{Type: userMetricPrefix + lm.Name, Labels: s.Labels},
		resource: &monitoredres.MonitoredResource{Type: typeGlobal, Labels: map[string]string{labelProject: project}},
		kind:     kindDelta,
		vtype:    vt,
		unit:     lm.Descriptor.GetUnit(),
	}

	for i := history; i >= 0; i-- {
		end := now.Add(-time.Duration(i) * time.Minute)
		if end.After(start) {
			rec.points = append(rec.points, pointAt(start, end, val))
		}
	}

	return rec
}

func distributionOf(s LogMetricSeries) *monitoringpb.TypedValue {
	d := &distribution.Distribution{
		Count:                 s.Count,
		SumOfSquaredDeviation: s.SumSqDev,
		BucketCounts:          s.BucketCounts,
		BucketOptions:         s.BucketOptions,
	}
	if s.Count > 0 {
		d.Mean = s.Sum / float64(s.Count)
	}

	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: d}}
}
