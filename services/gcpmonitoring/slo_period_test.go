package gcpmonitoring_test

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/calendarperiod"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func periodSLO(rolling time.Duration, cal calendarperiod.CalendarPeriod) *monitoringpb.ServiceLevelObjective {
	s := requestSLO(0.9)

	if rolling > 0 {
		s.Period = &monitoringpb.ServiceLevelObjective_RollingPeriod{RollingPeriod: durationpb.New(rolling)}
	} else {
		s.Period = &monitoringpb.ServiceLevelObjective_CalendarPeriod{CalendarPeriod: cal}
	}

	return s
}

// periodNow is 2030-01-09 is a Wednesday in ISO week 2; its week starts Jan 7, its fortnight Dec 31, its month Jan 1.
func periodNow() time.Time { return time.Date(2030, 1, 9, 12, 0, 0, 0, time.UTC) }

func TestSLOCompliancePeriods(t *testing.T) {
	t.Parallel()

	now := periodNow()
	day := 24 * time.Hour

	tests := []struct {
		oldAt   time.Time
		name    string
		rolling time.Duration
		cal     calendarperiod.CalendarPeriod
		want    float64
	}{
		{name: "rolling 1d excludes 3d old", rolling: day, oldAt: now.Add(-3 * day), want: 0.5},
		{name: "rolling 1d excludes 30h old", rolling: day, oldAt: now.Add(-30 * time.Hour), want: 0.5},
		{name: "rolling 2d includes 30h old", rolling: 2 * day, oldAt: now.Add(-30 * time.Hour), want: 0.75},
		{name: "rolling 2d excludes 60h old", rolling: 2 * day, oldAt: now.Add(-60 * time.Hour), want: 0.5},
		{name: "rolling 4d includes 3d old", rolling: 4 * day, oldAt: now.Add(-3 * day), want: 0.75},
		{name: "day excludes yesterday", cal: calendarperiod.CalendarPeriod_DAY, oldAt: now.Add(-day), want: 0.5},
		{
			name: "week excludes sunday", cal: calendarperiod.CalendarPeriod_WEEK,
			oldAt: time.Date(2030, 1, 6, 12, 0, 0, 0, time.UTC), want: 0.5,
		},
		{
			name: "week includes monday", cal: calendarperiod.CalendarPeriod_WEEK,
			oldAt: time.Date(2030, 1, 7, 12, 0, 0, 0, time.UTC), want: 0.75,
		},
		{
			name: "fortnight includes previous week", cal: calendarperiod.CalendarPeriod_FORTNIGHT,
			oldAt: time.Date(2030, 1, 6, 12, 0, 0, 0, time.UTC), want: 0.75,
		},
		{
			name: "fortnight excludes before its start", cal: calendarperiod.CalendarPeriod_FORTNIGHT,
			oldAt: time.Date(2030, 1, 5, 12, 0, 0, 0, time.UTC).Add(-6 * day), want: 0.5,
		},
		{
			name: "month excludes december", cal: calendarperiod.CalendarPeriod_MONTH,
			oldAt: time.Date(2029, 12, 31, 12, 0, 0, 0, time.UTC), want: 0.5,
		},
		{
			name: "month includes january", cal: calendarperiod.CalendarPeriod_MONTH,
			oldAt: time.Date(2030, 1, 2, 12, 0, 0, 0, time.UTC), want: 0.75,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, name := sloEnv(t, periodSLO(tt.rolling, tt.cal))

			e.writeCount(t, goodMetric, tt.oldAt, 10)
			e.writeCount(t, totalMetric, tt.oldAt, 10)
			e.writeCount(t, goodMetric, now.Add(-time.Hour), 5)
			e.writeCount(t, totalMetric, now.Add(-time.Hour), 10)
			e.clk.set(now)

			got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
				Filter:   `select_slo_compliance("` + name + `")`,
				Interval: interval(now, now),
			})
			require.Len(t, got, 1)
			require.Len(t, got[0].GetPoints(), 1)
			assert.InDelta(t, tt.want, values(got[0])[0], 1e-9)
		})
	}
}

func TestSLOSelectorsAcrossAlignmentPeriods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     map[string][]float64
		selector string
	}{
		{selector: "select_slo_health", want: map[string][]float64{"": {0, 1}}},
		{selector: "select_slo_compliance", want: map[string][]float64{"": {0.5, 1}}},
		{selector: "select_slo_counts", want: map[string][]float64{"good": {0, 10}, "bad": {10, 0}}},
		{selector: "select_slo_budget_fraction", want: map[string][]float64{"": {-4, 1}}},
	}

	for _, tt := range tests {
		t.Run(tt.selector, func(t *testing.T) {
			t.Parallel()

			now := periodNow()
			e, name := sloEnv(t, periodSLO(7*24*time.Hour, 0))

			e.writeCount(t, goodMetric, now.Add(-90*time.Minute), 10)
			e.writeCount(t, totalMetric, now.Add(-90*time.Minute), 10)
			e.writeCount(t, goodMetric, now.Add(-30*time.Minute), 0)
			e.writeCount(t, totalMetric, now.Add(-30*time.Minute), 10)
			e.clk.set(now)

			got := e.list(t, &monitoringpb.ListTimeSeriesRequest{
				Filter:      tt.selector + `("` + name + `")`,
				Interval:    interval(now.Add(-2*time.Hour), now),
				Aggregation: &monitoringpb.Aggregation{AlignmentPeriod: durationpb.New(time.Hour)},
			})

			// ListTimeSeries returns the newest point first.
			have := map[string][]float64{}

			for _, ts := range got {
				have[ts.GetMetric().GetLabels()["status"]] = values(ts)
			}

			require.Len(t, have, len(tt.want))

			for k, w := range tt.want {
				require.Len(t, have[k], len(w), k)

				for i := range w {
					assert.InDelta(t, w[i], have[k][i], 1e-9, k)
				}
			}
		})
	}
}

func TestSLOSelectorArgumentsAndCost(t *testing.T) {
	t.Parallel()

	heavy := periodSLO(30*24*time.Hour, 0)
	heavy.ServiceLevelIndicator = &monitoringpb.ServiceLevelIndicator{
		Type: &monitoringpb.ServiceLevelIndicator_WindowsBased{WindowsBased: &monitoringpb.WindowsBasedSli{
			WindowPeriod: durationpb.New(time.Minute),
			WindowCriterion: &monitoringpb.WindowsBasedSli_GoodBadMetricFilter{
				GoodBadMetricFilter: metricQuery(boolMetric),
			},
		}},
	}

	tests := []struct {
		slo    *monitoringpb.ServiceLevelObjective
		name   string
		filter string
		span   time.Duration
		ok     bool
	}{
		{
			name:   "health with lookback",
			slo:    requestSLO(0.9),
			filter: `select_slo_health("%s", "1h")`,
			span:   time.Minute,
		},
		{name: "burn rate with lookback", slo: requestSLO(0.9), filter: `select_slo_burn_rate("%s", "1h")`, ok: true},
		{name: "burn rate without lookback", slo: requestSLO(0.9), filter: `select_slo_burn_rate("%s")`},
		{name: "cheap windows", slo: heavy, filter: `select_slo_compliance("%s")`, ok: true},
		{name: "expensive windows", slo: heavy, filter: `select_slo_compliance("%s")`, span: time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, name := sloEnv(t, tt.slo)
			now := periodNow()
			e.clk.set(now)

			req := &monitoringpb.ListTimeSeriesRequest{
				Name: proj, Filter: sprintf(tt.filter, name), Interval: interval(now.Add(-tt.span), now),
				Aggregation: &monitoringpb.Aggregation{AlignmentPeriod: durationpb.New(time.Minute)},
			}

			_, err := e.metric(t).ListTimeSeries(t.Context(), req).Next()
			if tt.ok {
				assert.NotEqual(t, codes.InvalidArgument, status.Code(err))

				return
			}

			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}
