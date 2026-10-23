package gcpmonitoring_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
)

const (
	testProject = "test-project"
	proj        = "projects/" + testProject
	queueDepth  = "custom.googleapis.com/test/queue_depth"
	zoneA       = "us-central1-a"
)

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time  { return time.Unix(0, c.ns.Load()).UTC() }
func (c *clock) advanceMinute()  { c.ns.Add(int64(time.Minute)) }
func (c *clock) set(t time.Time) { c.ns.Store(t.UnixNano()) }

func epoch() time.Time { return time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC) }

type published struct {
	topic string
	data  string
}

type fakePub struct {
	msgs []published
	mu   sync.Mutex
}

func (f *fakePub) Publish(topic string, data []byte, _ map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.msgs = append(f.msgs, published{topic: topic, data: string(data)})

	return nil
}

func (f *fakePub) all() []published {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]published(nil), f.msgs...)
}

type fakeLogs struct {
	metrics []gcpmonitoring.LogMetric
}

func (f *fakeLogs) LogMetrics(string) []gcpmonitoring.LogMetric { return f.metrics }

type env struct {
	handler *gcpmonitoring.Handler
	clk     *clock
	pub     *fakePub
	opts    []option.ClientOption
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpmonitoring.NewHandler(gcpmonitoring.NewInMemoryBackend())
	h.Port = 0
	h.EvalInterval = 0

	e := &env{handler: h, clk: &clock{}, pub: &fakePub{}}
	e.clk.set(epoch())
	h.Backend.SetClock(e.clk.now)
	h.Backend.SetPublisher(e.pub)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e.opts = []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	return e
}

func (e *env) metric(t *testing.T) *monitoring.MetricClient {
	t.Helper()

	c, err := monitoring.NewMetricClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) alerts(t *testing.T) *monitoring.AlertPolicyClient {
	t.Helper()

	c, err := monitoring.NewAlertPolicyClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) channels(t *testing.T) *monitoring.NotificationChannelClient {
	t.Helper()

	c, err := monitoring.NewNotificationChannelClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) uptime(t *testing.T) *monitoring.UptimeCheckClient {
	t.Helper()

	c, err := monitoring.NewUptimeCheckClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) groups(t *testing.T) *monitoring.GroupClient {
	t.Helper()

	c, err := monitoring.NewGroupClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func instance(id string) *monitoredres.MonitoredResource {
	return &monitoredres.MonitoredResource{
		Type: "gce_instance", Labels: map[string]string{"instance_id": id, "zone": zoneA},
	}
}

func gaugeDouble(
	typ string,
	labels map[string]string,
	res *monitoredres.MonitoredResource,
	at time.Time,
	v float64,
) *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{Type: typ, Labels: labels}, Resource: res,
		Points: []*monitoringpb.Point{{
			Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(at)},
			Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: v}},
		}},
	}
}

func (e *env) write(t *testing.T, series ...*monitoringpb.TimeSeries) {
	t.Helper()

	require.NoError(t, e.metric(t).CreateTimeSeries(t.Context(),
		&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: series}))
}

// writeAt moves the clock to at and writes one gauge point per entry of values keyed by instance id.
func (e *env) writeAt(t *testing.T, at time.Time, tier string, values map[string]float64) {
	t.Helper()

	e.clk.set(at)

	series := make([]*monitoringpb.TimeSeries, 0, len(values))

	for id, v := range values {
		series = append(series, gaugeDouble(queueDepth, map[string]string{"tier": tier}, instance(id), at, v))
	}

	e.write(t, series...)
}

func interval(start, end time.Time) *monitoringpb.TimeInterval {
	return &monitoringpb.TimeInterval{StartTime: timestamppb.New(start), EndTime: timestamppb.New(end)}
}

func (e *env) list(t *testing.T, req *monitoringpb.ListTimeSeriesRequest) []*monitoringpb.TimeSeries {
	t.Helper()

	req.Name = proj

	var out []*monitoringpb.TimeSeries

	it := e.metric(t).ListTimeSeries(t.Context(), req)

	for {
		ts, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, ts)
	}
}

func values(ts *monitoringpb.TimeSeries) []float64 {
	out := make([]float64, 0, len(ts.GetPoints()))

	for _, p := range ts.GetPoints() {
		switch v := p.GetValue().GetValue().(type) {
		case *monitoringpb.TypedValue_DoubleValue:
			out = append(out, v.DoubleValue)
		case *monitoringpb.TypedValue_Int64Value:
			out = append(out, float64(v.Int64Value))
		}
	}

	return out
}

func codeOf(err error) codes.Code { return status.Code(err) }

func intSeries(
	typ string,
	kind metricpb.MetricDescriptor_MetricKind,
	res *monitoredres.MonitoredResource,
	start, end time.Time,
	v int64,
) *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{Type: typ}, Resource: res, MetricKind: kind,
		Points: []*monitoringpb.Point{{
			Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(start), EndTime: timestamppb.New(end)},
			Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: v}},
		}},
	}
}

func mustLabels(t *testing.T, msg published) map[string]string {
	t.Helper()

	var p struct {
		Incident struct {
			Metric struct {
				Labels map[string]string `json:"labels"`
			} `json:"metric"`
		} `json:"incident"`
	}
	require.NoError(t, json.Unmarshal([]byte(msg.data), &p))

	return p.Incident.Metric.Labels
}
