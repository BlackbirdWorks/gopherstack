package gcplogging

import (
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	userMetricPrefix = "logging.googleapis.com/user/"
	maxMetricLabels  = 10
	maxFiniteBuckets = 1000
)

var metricNameRe = regexp.MustCompile(`^[A-Za-z0-9_.,+!*'()%/\\-]{1,100}$`)

func cloneMetric(m *loggingpb.LogMetric) *loggingpb.LogMetric {
	out, _ := proto.Clone(m).(*loggingpb.LogMetric)

	return out
}

func metricKey(name string) (string, string, error) {
	parent, id, ok := strings.Cut(name, "/metrics/")
	if !ok || checkParent(parent) != nil || id == "" || strings.Count(name, "/metrics/") != 1 {
		return "", "", errInvalid("Invalid metric name [%s]: expected [PARENT]/metrics/[METRIC_ID].", name)
	}

	return parent, id, nil
}

// normalizeMetric validates a metric and fills the descriptor defaults the API reports.
func normalizeMetric(parent string, in *loggingpb.LogMetric) (*loggingpb.LogMetric, *metricCompiled, error) {
	if !metricNameRe.MatchString(in.GetName()) {
		return nil, nil, errInvalid("Invalid metric name [%s].", in.GetName())
	}

	if in.GetFilter() == "" {
		return nil, nil, errInvalid("Metric filter must be set.")
	}

	if _, err := compileFilter(in.GetFilter()); err != nil {
		return nil, nil, err
	}

	m := cloneMetric(in)
	distribution := m.GetValueExtractor() != ""

	d := m.GetMetricDescriptor()
	if d == nil {
		d = &metric.MetricDescriptor{}
	}

	if err := fillDescriptor(parent, m.GetName(), d, distribution); err != nil {
		return nil, nil, err
	}

	m.MetricDescriptor = d

	cm, err := compileMetric(m)
	if err != nil {
		return nil, nil, err
	}

	return m, cm, nil
}

func fillDescriptor(parent, name string, d *metric.MetricDescriptor, distribution bool) error {
	switch d.GetMetricKind() {
	case metric.MetricDescriptor_METRIC_KIND_UNSPECIFIED:
		d.MetricKind = metric.MetricDescriptor_DELTA
	case metric.MetricDescriptor_DELTA:
	default:
		return errInvalid("Log-based metrics must have metric_kind DELTA.")
	}

	want := metric.MetricDescriptor_INT64
	if distribution {
		want = metric.MetricDescriptor_DISTRIBUTION
	}

	if d.GetValueType() != metric.MetricDescriptor_VALUE_TYPE_UNSPECIFIED && d.GetValueType() != want {
		return errInvalid("Metric value_type must be %s for this metric.", want)
	}

	d.ValueType = want

	if len(d.GetLabels()) > maxMetricLabels {
		return errInvalid("A metric can have at most %d labels.", maxMetricLabels)
	}

	if d.GetUnit() == "" {
		d.Unit = "1"
	}

	d.Type = userMetricPrefix + name
	d.Name = parent + "/metricDescriptors/" + userMetricPrefix + name

	return nil
}

// CreateLogMetric stores a log-based metric.
func (b *InMemoryBackend) CreateLogMetric(req *loggingpb.CreateLogMetricRequest) (*loggingpb.LogMetric, error) {
	parent := req.GetParent()
	if err := checkParent(parent); err != nil {
		return nil, err
	}

	if req.GetMetric() == nil {
		return nil, errInvalid("metric is required.")
	}

	m, cm, err := normalizeMetric(parent, req.GetMetric())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateLogMetric")
	defer b.mu.Unlock()

	key := parent + "/metrics/" + m.GetName()
	if _, ok := b.metrics[key]; ok {
		return nil, errExists("Metric %s already exists.", m.GetName())
	}

	now := timestamppb.New(b.now())
	m.CreateTime, m.UpdateTime = now, now
	b.metrics[key] = &metricRec{pb: m, compiled: cm, series: map[string]*seriesRec{}}

	return cloneMetric(m), nil
}

// GetLogMetric returns one metric.
func (b *InMemoryBackend) GetLogMetric(name string) (*loggingpb.LogMetric, error) {
	if _, _, err := metricKey(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetLogMetric")
	defer b.mu.RUnlock()

	rec, ok := b.metrics[name]
	if !ok {
		return nil, errNotFound("Metric %s does not exist.", name)
	}

	return cloneMetric(rec.pb), nil
}

// ListLogMetrics returns the parent's metrics ordered by name.
func (b *InMemoryBackend) ListLogMetrics(req *loggingpb.ListLogMetricsRequest) ([]*loggingpb.LogMetric, string, error) {
	parent := req.GetParent()
	if err := checkParent(parent); err != nil {
		return nil, "", err
	}

	if err := pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListLogMetrics")

	var all []*loggingpb.LogMetric

	for key, rec := range b.metrics {
		if strings.HasPrefix(key, parent+"/metrics/") {
			all = append(all, cloneMetric(rec.pb))
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c *loggingpb.LogMetric) int { return strings.Compare(a.GetName(), c.GetName()) })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// UpdateLogMetric replaces a metric, creating it when it does not exist.
func (b *InMemoryBackend) UpdateLogMetric(req *loggingpb.UpdateLogMetricRequest) (*loggingpb.LogMetric, error) {
	parent, id, err := metricKey(req.GetMetricName())
	if err != nil {
		return nil, err
	}

	in := req.GetMetric()
	if in == nil {
		return nil, errInvalid("metric is required.")
	}

	if in.GetName() != id {
		return nil, errInvalid("The metric name [%s] must match the name in the request path [%s].", in.GetName(), id)
	}

	m, cm, err := normalizeMetric(parent, in)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateLogMetric")
	defer b.mu.Unlock()

	now := timestamppb.New(b.now())
	m.CreateTime, m.UpdateTime = now, now

	rec := &metricRec{pb: m, compiled: cm, series: map[string]*seriesRec{}}

	if cur, ok := b.metrics[req.GetMetricName()]; ok {
		m.CreateTime = cur.pb.GetCreateTime()
		rec.series = cur.series

		if !proto.Equal(cur.pb.GetBucketOptions(), m.GetBucketOptions()) ||
			cur.pb.GetValueExtractor() != m.GetValueExtractor() {
			rec.series = map[string]*seriesRec{}
		}
	}

	b.metrics[req.GetMetricName()] = rec

	return cloneMetric(m), nil
}

// DeleteLogMetric removes a metric.
func (b *InMemoryBackend) DeleteLogMetric(name string) error {
	if _, _, err := metricKey(name); err != nil {
		return err
	}

	b.mu.Lock("DeleteLogMetric")
	defer b.mu.Unlock()

	if _, ok := b.metrics[name]; !ok {
		return errNotFound("Metric %s does not exist.", name)
	}

	delete(b.metrics, name)

	return nil
}
