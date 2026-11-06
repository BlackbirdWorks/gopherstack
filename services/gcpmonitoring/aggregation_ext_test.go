package gcpmonitoring_test

import (
	"math"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	distpb "google.golang.org/genproto/googleapis/api/distribution"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const latencyMetric = "custom.googleapis.com/test/latency"

func (e *env) listAgg(
	t *testing.T, filter string, from, to time.Time, a *monitoringpb.Aggregation,
) []*monitoringpb.TimeSeries {
	t.Helper()

	return e.list(t, &monitoringpb.ListTimeSeriesRequest{
		Filter: filter, Interval: interval(from, to), Aggregation: a,
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	})
}

type offsetValue struct {
	off time.Duration
	v   float64
}

func TestAlignInterpolateAndPercentChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		points []offsetValue
		want   []float64
		period time.Duration
		from   time.Duration
		align  monitoringpb.Aggregation_Aligner
	}{
		{
			name: "interpolate", align: monitoringpb.Aggregation_ALIGN_INTERPOLATE, period: time.Minute,
			from:   -3 * time.Minute,
			points: []offsetValue{{-4 * time.Minute, 10}, {0, 50}},
			want:   []float64{50, 40, 30},
		},
		{
			name: "interpolate skips bucket without later point", align: monitoringpb.Aggregation_ALIGN_INTERPOLATE,
			period: time.Minute, from: -2 * time.Minute,
			points: []offsetValue{{-time.Minute, 10}},
			want:   []float64{10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			t0 := epoch()

			for _, p := range tt.points {
				e.writeAt(t, t0.Add(p.off), "gold", map[string]float64{"inst-1": p.v})
			}

			e.clk.set(t0)

			got := e.listAgg(t, metricFilter, t0.Add(tt.from), t0, agg(tt.period, tt.align, 0))
			require.Len(t, got, 1)
			assert.Equal(t, tt.want, values(got[0]))
			assert.Equal(t, metricpb.MetricDescriptor_GAUGE, got[0].GetMetricKind())
		})
	}
}

func TestAlignPercentChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prev, cur  float64
		wantChange float64
	}{
		{"doubling", 10, 20, 100},
		{"halving", 20, 10, -50},
		{"unchanged zero", 0, 0, 0},
		{"unchanged", 7, 7, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			t0 := epoch()

			for m := 19; m >= 0; m-- {
				v := tt.cur
				if m >= 10 {
					v = tt.prev
				}

				e.writeAt(t, t0.Add(-time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": v})
			}

			a := agg(10*time.Minute, monitoringpb.Aggregation_ALIGN_PERCENT_CHANGE, 0)
			got := e.listAgg(t, metricFilter, t0.Add(-10*time.Minute), t0, a)
			require.Len(t, got, 1)
			assert.Equal(t, []float64{tt.wantChange}, values(got[0]))
			assert.Equal(t, metricpb.MetricDescriptor_DOUBLE, got[0].GetValueType())
		})
	}
}

func TestReducePercentileNumeric(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reducer monitoringpb.Aggregation_Reducer
		want    float64
	}{
		{monitoringpb.Aggregation_REDUCE_PERCENTILE_05, 1.2},
		{monitoringpb.Aggregation_REDUCE_PERCENTILE_50, 3},
		{monitoringpb.Aggregation_REDUCE_PERCENTILE_95, 4.8},
		{monitoringpb.Aggregation_REDUCE_PERCENTILE_99, 4.96},
	}

	for _, tt := range tests {
		t.Run(tt.reducer.String(), func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			t0 := epoch()
			vals := map[string]float64{}

			for i := 1; i <= 5; i++ {
				vals[fmt.Sprintf("inst-%d", i)] = float64(i)
			}

			e.writeAt(t, t0, "gold", vals)

			a := agg(time.Minute, monitoringpb.Aggregation_ALIGN_MEAN, tt.reducer)
			got := e.listAgg(t, metricFilter, t0.Add(-time.Minute), t0, a)
			require.Len(t, got, 1)
			require.Len(t, got[0].GetPoints(), 1)
			assert.InDelta(t, tt.want, values(got[0])[0], 1e-9)
			assert.Equal(t, metricpb.MetricDescriptor_DOUBLE, got[0].GetValueType())
		})
	}
}

func latencySeries(
	id string, kind metricpb.MetricDescriptor_MetricKind, start, end time.Time, counts []int64, mean float64,
) *monitoringpb.TimeSeries {
	var total int64
	for _, c := range counts {
		total += c
	}

	d := &distpb.Distribution{
		Count: total, Mean: mean, BucketCounts: counts,
		BucketOptions: &distpb.Distribution_BucketOptions{Options: &distpb.Distribution_BucketOptions_ExplicitBuckets{
			ExplicitBuckets: &distpb.Distribution_BucketOptions_Explicit{Bounds: []float64{10, 20, 30}},
		}},
	}

	return &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{Type: latencyMetric}, Resource: &monitoredres.MonitoredResource{
			Type: "gce_instance", Labels: map[string]string{"instance_id": id, "zone": zoneA},
		},
		MetricKind: kind, ValueType: metricpb.MetricDescriptor_DISTRIBUTION,
		Points: []*monitoringpb.Point{{
			Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(start), EndTime: timestamppb.New(end)},
			Value: &monitoringpb.TypedValue{
				Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: d},
			},
		}},
	}
}

func distCounts(ts *monitoringpb.TimeSeries) []int64 {
	return ts.GetPoints()[0].GetValue().GetDistributionValue().GetBucketCounts()
}

func TestDistributionAggregation(t *testing.T) {
	t.Parallel()

	const latencyFilter = `metric.type = "` + latencyMetric + `"`

	tests := []struct {
		agg   *monitoringpb.Aggregation
		check func(t *testing.T, got []*monitoringpb.TimeSeries)
		name  string
	}{
		{
			name: "align sum merges buckets",
			agg:  agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, 0),
			check: func(t *testing.T, got []*monitoringpb.TimeSeries) {
				t.Helper()
				require.Len(t, got, 2)
				assert.Equal(t, metricpb.MetricDescriptor_DISTRIBUTION, got[0].GetValueType())
			},
		},
		{
			name: "reduce sum merges series",
			agg:  agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, monitoringpb.Aggregation_REDUCE_SUM),
			check: func(t *testing.T, got []*monitoringpb.TimeSeries) {
				t.Helper()
				require.Len(t, got, 1)
				assert.Equal(t, []int64{0, 6, 4, 2}, distCounts(got[0]))
			},
		},
		{
			name: "reduce percentile 50",
			agg:  agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, monitoringpb.Aggregation_REDUCE_PERCENTILE_50),
			check: func(t *testing.T, got []*monitoringpb.TimeSeries) {
				t.Helper()
				require.Len(t, got, 1)
				assert.InDelta(t, 10+10*6.0/6.0, values(got[0])[0], 1e-9)
			},
		},
		{
			name: "reduce mean weights by count",
			agg:  agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, monitoringpb.Aggregation_REDUCE_MEAN),
			check: func(t *testing.T, got []*monitoringpb.TimeSeries) {
				t.Helper()
				require.Len(t, got, 1)
				assert.InDelta(t, (10.0*4+30.0*8)/12, values(got[0])[0], 1e-9)
			},
		},
		{
			name: "reduce stddev is the sample deviation of the merged distribution",
			agg:  agg(time.Minute, monitoringpb.Aggregation_ALIGN_SUM, monitoringpb.Aggregation_REDUCE_STDDEV),
			check: func(t *testing.T, got []*monitoringpb.TimeSeries) {
				t.Helper()
				require.Len(t, got, 1)
				assert.Equal(t, metricpb.MetricDescriptor_DOUBLE, got[0].GetValueType())
				assert.InDelta(t, math.Sqrt(400.0*4*8/12/11), values(got[0])[0], 1e-9)
			},
		},
		{
			name: "align delta of delta series sums",
			agg:  agg(time.Minute, monitoringpb.Aggregation_ALIGN_DELTA, 0),
			check: func(t *testing.T, got []*monitoringpb.TimeSeries) {
				t.Helper()
				require.Len(t, got, 2)
				assert.Equal(t, metricpb.MetricDescriptor_DELTA, got[0].GetMetricKind())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			t0 := epoch()
			e.clk.set(t0)
			e.write(t,
				latencySeries("inst-1", metricpb.MetricDescriptor_DELTA, t0.Add(-30*time.Second), t0,
					[]int64{0, 4, 0, 0}, 10),
				latencySeries("inst-2", metricpb.MetricDescriptor_DELTA, t0.Add(-30*time.Second), t0,
					[]int64{0, 2, 4, 2}, 30),
			)

			tt.check(t, e.listAgg(t, latencyFilter, t0.Add(-time.Minute), t0, tt.agg))
		})
	}
}

func TestCumulativeDistributionAlignDelta(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	t0 := epoch()
	start := t0.Add(-10 * time.Minute)

	e.clk.set(t0.Add(-2 * time.Minute))
	e.write(t, latencySeries("inst-1", metricpb.MetricDescriptor_CUMULATIVE, start, t0.Add(-2*time.Minute),
		[]int64{1, 2, 3, 0}, 15))
	e.clk.set(t0)
	e.write(t, latencySeries("inst-1", metricpb.MetricDescriptor_CUMULATIVE, start, t0, []int64{1, 5, 4, 2}, 18))

	a := agg(time.Minute, monitoringpb.Aggregation_ALIGN_DELTA, 0)
	got := e.listAgg(t, `metric.type = "`+latencyMetric+`"`, t0.Add(-time.Minute), t0, a)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{0, 3, 1, 2}, distCounts(got[0]))
	assert.Equal(t, metricpb.MetricDescriptor_DELTA, got[0].GetMetricKind())
}
