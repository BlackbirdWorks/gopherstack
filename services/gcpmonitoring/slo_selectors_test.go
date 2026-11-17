package gcpmonitoring_test

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSLOSelectorsOverListTimeSeries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     map[string]float64
		selector string
	}{
		{selector: "select_slo_health", want: map[string]float64{"": 0.9}},
		{selector: "select_slo_compliance", want: map[string]float64{"": 0.9}},
		{selector: "select_slo_budget_fraction", want: map[string]float64{"": -19}},
		{selector: "select_slo_budget_total", want: map[string]float64{"": 5}},
		{selector: "select_slo_budget", want: map[string]float64{"": -95}},
		{selector: "select_slo_counts", want: map[string]float64{"good": 900, "bad": 100}},
	}

	for _, tt := range tests {
		t.Run(tt.selector, func(t *testing.T) {
			t.Parallel()

			e, name := sloEnv(t, requestSLO(0.995))
			t0 := epoch()
			e.writeCount(t, goodMetric, t0, 900)
			e.writeCount(t, totalMetric, t0, 1000)
			e.clk.set(t0)

			got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
				Filter:      tt.selector + `("` + name + `")`,
				Interval:    interval(t0, t0),
				Aggregation: &monitoringpb.Aggregation{AlignmentPeriod: durationpb.New(time.Minute)},
			})

			have := map[string]float64{}

			for _, ts := range got {
				require.Len(t, ts.GetPoints(), 1)
				have[ts.GetMetric().GetLabels()["status"]] = values(ts)[0]
			}

			assert.Len(t, have, len(tt.want))

			for k, v := range tt.want {
				assert.InDelta(t, v, have[k], 1e-9, k)
			}
		})
	}
}

func TestSLOSelectorListErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		period time.Duration
		code   codes.Code
	}{
		{
			name:   "unknown slo",
			filter: `select_slo_compliance("projects/test-project/services/shop/serviceLevelObjectives/x")`,
			code:   codes.NotFound,
		},
		{name: "extra argument", filter: `select_slo_compliance("%s", "1h")`, code: codes.InvalidArgument},
		{name: "too many periods", filter: `select_slo_health("%s")`, period: time.Second, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, name := sloEnv(t, requestSLO(0.99))
			t0 := epoch()
			filter := tt.filter

			if tt.code != codes.NotFound {
				filter = sprintf(tt.filter, name)
			}

			req := &monitoringpb.ListTimeSeriesRequest{
				Name:     proj,
				Filter:   filter,
				Interval: interval(t0.Add(-48*time.Hour), t0),
			}
			if tt.period > 0 {
				req.Aggregation = &monitoringpb.Aggregation{AlignmentPeriod: durationpb.New(tt.period)}
			}

			it := e.metric(t).ListTimeSeries(t.Context(), req)
			_, err := it.Next()
			require.Error(t, err)
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}
