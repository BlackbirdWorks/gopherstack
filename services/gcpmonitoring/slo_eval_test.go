package gcpmonitoring_test

import (
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	goodMetric  = "custom.googleapis.com/test/good"
	totalMetric = "custom.googleapis.com/test/total"
	boolMetric  = "custom.googleapis.com/test/window_ok"
)

func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }

func metricQuery(typ string) string { return `metric.type = "` + typ + `"` }

func requestSLO(goal float64) *monitoringpb.ServiceLevelObjective {
	s := availabilitySLO("requests", goal)
	s.ServiceLevelIndicator = &monitoringpb.ServiceLevelIndicator{
		Type: &monitoringpb.ServiceLevelIndicator_RequestBased{RequestBased: &monitoringpb.RequestBasedSli{
			Method: &monitoringpb.RequestBasedSli_GoodTotalRatio{GoodTotalRatio: &monitoringpb.TimeSeriesRatio{
				GoodServiceFilter: metricQuery(goodMetric), TotalServiceFilter: metricQuery(totalMetric),
			}},
		}},
	}

	return s
}

func sloEnv(t *testing.T, slo *monitoringpb.ServiceLevelObjective) (*env, string) {
	t.Helper()

	e := newEnv(t)
	svc := e.createService(t, "shop", customService("Shop"))

	return e, e.createSLO(t, svc.GetName(), "avail", slo).GetName()
}

func sloPolicy(filter string, cmp monitoringpb.ComparisonType, threshold float64) *monitoringpb.AlertPolicy {
	return &monitoringpb.AlertPolicy{
		DisplayName: "slo", Combiner: monitoringpb.AlertPolicy_OR,
		Conditions: []*monitoringpb.AlertPolicy_Condition{
			thresholdCondition("slo selector", filter, cmp, threshold, 0),
		},
	}
}

func TestSLOBurnRateAndHealthConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		selector   string
		threshold  float64
		good       int64
		tot        int64
		cmp        monitoringpb.ComparisonType
		wantFiring bool
	}{
		{
			name: "burn rate above", selector: `select_slo_burn_rate("%s", "3600s")`,
			cmp: monitoringpb.ComparisonType_COMPARISON_GT, threshold: 10, good: 80, tot: 100, wantFiring: true,
		},
		{
			name: "burn rate below", selector: `select_slo_burn_rate("%s", "3600s")`,
			cmp: monitoringpb.ComparisonType_COMPARISON_GT, threshold: 10, good: 95, tot: 100,
		},
		{
			name: "health below", selector: `select_slo_health("%s")`,
			cmp: monitoringpb.ComparisonType_COMPARISON_LT, threshold: 0.9, good: 80, tot: 100, wantFiring: true,
		},
		{
			name: "health fine", selector: `select_slo_health("%s")`,
			cmp: monitoringpb.ComparisonType_COMPARISON_LT, threshold: 0.9, good: 99, tot: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, name := sloEnv(t, requestSLO(0.99))
			filter := sprintf(tt.selector, name)
			e.createPolicy(t, sloPolicy(filter, tt.cmp, tt.threshold))

			t0 := epoch()
			e.writeCount(t, goodMetric, t0, tt.good)
			e.writeCount(t, totalMetric, t0, tt.tot)
			e.handler.Backend.Tick(t.Context())

			assert.Equal(t, tt.wantFiring, len(e.handler.Backend.Incidents(testProject)) == 1)
		})
	}
}

func TestSLOSelectorValidation(t *testing.T) {
	t.Parallel()

	e, name := sloEnv(t, requestSLO(0.99))

	tests := []struct {
		name, filter string
		ok           bool
	}{
		{"missing lookback", sprintf(`select_slo_burn_rate("%s")`, name), false},
		{"bad lookback", sprintf(`select_slo_burn_rate("%s", "soon")`, name), false},
		{"unknown slo", `select_slo_health("projects/test-project/services/shop/serviceLevelObjectives/nope")`, false},
		{"bad name", `select_slo_health("nonsense")`, false},
		{"valid", sprintf(`select_slo_burn_rate("%s", "1h")`, name), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.alerts(t).CreateAlertPolicy(t.Context(), &monitoringpb.CreateAlertPolicyRequest{
				Name: proj, AlertPolicy: sloPolicy(tt.filter, monitoringpb.ComparisonType_COMPARISON_GT, 1),
			})
			assert.Equal(t, tt.ok, err == nil)
		})
	}
}

func boolSeries(typ string, at time.Time, v bool) *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{Type: typ}, Resource: instance("inst-1"),
		Points: []*monitoringpb.Point{{
			Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(at)},
			Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: v}},
		}},
	}
}

func TestSLOStatus(t *testing.T) {
	t.Parallel()

	t0 := epoch()

	windows := availabilitySLO("windows", 0.5)
	windows.Period = &monitoringpb.ServiceLevelObjective_RollingPeriod{RollingPeriod: durationpb.New(24 * time.Hour)}
	windows.ServiceLevelIndicator = &monitoringpb.ServiceLevelIndicator{
		Type: &monitoringpb.ServiceLevelIndicator_WindowsBased{WindowsBased: &monitoringpb.WindowsBasedSli{
			WindowPeriod: durationpb.New(6 * time.Hour),
			WindowCriterion: &monitoringpb.WindowsBasedSli_GoodBadMetricFilter{
				GoodBadMetricFilter: metricQuery(boolMetric),
			},
		}},
	}

	cut := availabilitySLO("cut", 0.5)
	cut.ServiceLevelIndicator = &monitoringpb.ServiceLevelIndicator{
		Type: &monitoringpb.ServiceLevelIndicator_RequestBased{RequestBased: &monitoringpb.RequestBasedSli{
			Method: &monitoringpb.RequestBasedSli_DistributionCut{DistributionCut: &monitoringpb.DistributionCut{
				DistributionFilter: metricQuery(latencyMetric), Range: &monitoringpb.Range{Min: 0, Max: 20},
			}},
		}},
	}

	tests := []struct {
		slo        *monitoringpb.ServiceLevelObjective
		seed       func(t *testing.T, e *env)
		name       string
		compliance float64
		burn       float64
		total      float64
	}{
		{
			name: "request based", slo: requestSLO(0.99), compliance: 0.99, burn: 1, total: 1000,
			seed: func(t *testing.T, e *env) {
				t.Helper()
				e.writeCount(t, goodMetric, t0.Add(-time.Hour), 990)
				e.writeCount(t, totalMetric, t0.Add(-time.Hour), 1000)
			},
		},
		{
			name: "distribution cut", slo: cut, compliance: 0.5, burn: 1, total: 8,
			seed: func(t *testing.T, e *env) {
				t.Helper()
				e.clk.set(t0)
				e.write(t, latencySeries("inst-1", metricpb.MetricDescriptor_DELTA, t0.Add(-time.Hour),
					t0.Add(-59*time.Minute), []int64{0, 4, 4, 0}, 20))
			},
		},
		{
			name: "windows based", slo: windows, compliance: 0.75, burn: 0.5, total: 4,
			seed: func(t *testing.T, e *env) {
				t.Helper()
				e.clk.set(t0)

				for _, p := range []struct {
					off time.Duration
					ok  bool
				}{{-23 * time.Hour, true}, {-17 * time.Hour, false}, {-11 * time.Hour, true}, {-5 * time.Hour, true}} {
					e.write(t, boolSeries(boolMetric, t0.Add(p.off), p.ok))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, name := sloEnv(t, tt.slo)
			tt.seed(t, e)
			e.clk.set(t0)

			st, err := e.handler.Backend.SLOStatus(name)
			require.NoError(t, err)
			assert.InDelta(t, tt.compliance, st.Compliance, 1e-9)
			assert.InDelta(t, tt.burn, st.BurnRate, 1e-9)
			assert.InDelta(t, tt.total, st.Total, 1e-9)
			assert.InDelta(t, 1-tt.burn, st.BudgetFraction, 1e-9)
		})
	}
}

func TestSLOExplicitViewExpandsBasicSli(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	svc := e.createService(t, "run", &monitoringpb.Service{
		DisplayName: "Run",
		Identifier: &monitoringpb.Service_CloudRun_{CloudRun: &monitoringpb.Service_CloudRun{
			ServiceName: "api", Location: "us-central1",
		}},
	})

	avail := e.createSLO(t, svc.GetName(), "avail", availabilitySLO("avail", 0.9))
	latency := availabilitySLO("latency", 0.9)
	latency.ServiceLevelIndicator.GetBasicSli().SliCriteria = &monitoringpb.BasicSli_Latency{
		Latency: &monitoringpb.BasicSli_LatencyCriteria{Threshold: durationpb.New(500 * time.Millisecond)},
	}
	lat := e.createSLO(t, svc.GetName(), "lat", latency)

	c := e.svcs(t)

	full, err := c.GetServiceLevelObjective(t.Context(), &monitoringpb.GetServiceLevelObjectiveRequest{
		Name: avail.GetName(), View: monitoringpb.ServiceLevelObjective_FULL,
	})
	require.NoError(t, err)
	assert.NotNil(t, full.GetServiceLevelIndicator().GetBasicSli())

	explicit, err := c.GetServiceLevelObjective(t.Context(), &monitoringpb.GetServiceLevelObjectiveRequest{
		Name: avail.GetName(), View: monitoringpb.ServiceLevelObjective_EXPLICIT,
	})
	require.NoError(t, err)
	assert.Nil(t, explicit.GetServiceLevelIndicator().GetBasicSli())

	ratio := explicit.GetServiceLevelIndicator().GetRequestBased().GetGoodTotalRatio()
	assert.Contains(t, ratio.GetTotalServiceFilter(), "run.googleapis.com/request_count")
	assert.Contains(t, ratio.GetTotalServiceFilter(), `"api"`)
	assert.Contains(t, ratio.GetGoodServiceFilter(), "5xx")

	explicitLat, err := c.GetServiceLevelObjective(t.Context(), &monitoringpb.GetServiceLevelObjectiveRequest{
		Name: lat.GetName(), View: monitoringpb.ServiceLevelObjective_EXPLICIT,
	})
	require.NoError(t, err)

	cut := explicitLat.GetServiceLevelIndicator().GetRequestBased().GetDistributionCut()
	assert.Contains(t, cut.GetDistributionFilter(), "run.googleapis.com/request_latencies")
	assert.InDelta(t, 500, cut.GetRange().GetMax(), 1e-9)

	listed := c.ListServiceLevelObjectives(t.Context(), &monitoringpb.ListServiceLevelObjectivesRequest{
		Parent: svc.GetName(), View: monitoringpb.ServiceLevelObjective_EXPLICIT,
	})

	for {
		s, lerr := listed.Next()
		if lerr != nil {
			break
		}

		assert.NotNil(t, s.GetServiceLevelIndicator().GetRequestBased())
	}
}

func TestBasicSliAvailabilityOnCloudRun(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	svc := e.createService(t, "run", &monitoringpb.Service{
		DisplayName: "Run",
		Identifier: &monitoringpb.Service_CloudRun_{CloudRun: &monitoringpb.Service_CloudRun{
			ServiceName: "api", Location: "us-central1",
		}},
	})
	slo := e.createSLO(t, svc.GetName(), "avail", availabilitySLO("avail", 0.9))
	t0 := epoch()
	e.clk.set(t0)

	counts := map[string]int64{"2xx": 90, "5xx": 10}
	for class, n := range counts {
		ts := intSeries(
			"run.googleapis.com/request_count",
			metricpb.MetricDescriptor_DELTA,
			&monitoredres.MonitoredResource{
				Type: "cloud_run_revision", Labels: map[string]string{
					"service_name": "api", "revision_name": "r", "location": "us-central1", "configuration_name": "c",
				},
			},
			t0.Add(-time.Minute),
			t0,
			n,
		)
		ts.Metric.Labels = map[string]string{"response_code": "200", "response_code_class": class}
		require.NoError(t, e.metric(t).CreateServiceTimeSeries(t.Context(),
			&monitoringpb.CreateTimeSeriesRequest{Name: proj, TimeSeries: []*monitoringpb.TimeSeries{ts}}))
	}

	st, err := e.handler.Backend.SLOStatus(slo.GetName())
	require.NoError(t, err)
	assert.InDelta(t, 0.9, st.Compliance, 1e-9)
	assert.InDelta(t, 100, st.Total, 1e-9)
}
