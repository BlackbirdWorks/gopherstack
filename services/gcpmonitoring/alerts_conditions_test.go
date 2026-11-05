package gcpmonitoring_test

import (
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
)

type fakeEntries struct {
	filters []string
	entries []gcpmonitoring.LogEntry
	mu      sync.Mutex
}

func (f *fakeEntries) LogEntries(_, filter string, notBefore time.Time) []gcpmonitoring.LogEntry {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.filters = append(f.filters, filter)

	var out []gcpmonitoring.LogEntry

	for _, e := range f.entries {
		if !e.Timestamp.Before(notBefore) {
			out = append(out, e)
		}
	}

	return out
}

func logEntry(at time.Time, id, service string) gcpmonitoring.LogEntry {
	return gcpmonitoring.LogEntry{
		Timestamp: at, InsertID: id,
		Fields: map[string]any{
			"severity":    "ERROR",
			"jsonPayload": map[string]any{"service": service, "message": "boom 42"},
		},
	}
}

func logPolicy(channel string) *monitoringpb.AlertPolicy {
	return &monitoringpb.AlertPolicy{
		DisplayName: "errors in logs", Combiner: monitoringpb.AlertPolicy_OR, NotificationChannels: []string{channel},
		AlertStrategy: &monitoringpb.AlertPolicy_AlertStrategy{
			NotificationRateLimit: &monitoringpb.AlertPolicy_AlertStrategy_NotificationRateLimit{
				Period: durationpb.New(5 * time.Minute),
			},
			NotificationPrompts: []monitoringpb.AlertPolicy_AlertStrategy_NotificationPrompt{
				monitoringpb.AlertPolicy_AlertStrategy_OPENED,
			},
			AutoClose: durationpb.New(30 * time.Minute),
		},
		Conditions: []*monitoringpb.AlertPolicy_Condition{{
			DisplayName: "error entries",
			Condition: &monitoringpb.AlertPolicy_Condition_ConditionMatchedLog{
				ConditionMatchedLog: &monitoringpb.AlertPolicy_Condition_LogMatch{
					Filter: `severity>=ERROR`,
					LabelExtractors: map[string]string{
						"service": "EXTRACT(jsonPayload.service)",
						"code":    `REGEXP_EXTRACT(jsonPayload.message, "boom ([0-9]+)")`,
					},
				},
			},
		}},
	}
}

func TestMatchedLogConditionOpensIncidentsAndAutoCloses(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	src := &fakeEntries{}
	e.handler.Backend.SetLogEntrySource(src)

	b := e.handler.Backend
	t0 := epoch()
	e.clk.set(t0)
	e.createPolicy(t, logPolicy(e.pubsubChannel(t)))

	b.Tick(t.Context())
	assert.Empty(t, e.pub.all(), "nothing matched yet")

	src.entries = []gcpmonitoring.LogEntry{logEntry(t0.Add(time.Minute), "a", "checkout")}
	e.clk.set(t0.Add(time.Minute))
	b.Tick(t.Context())
	b.Tick(t.Context())

	msgs := e.pub.all()
	require.Len(t, msgs, 1, "the same entry never opens twice")
	assert.Equal(t, "open", decode(t, msgs[0]).Incident.State)
	assert.Equal(t, map[string]string{"service": "checkout", "code": "42"}, mustLabels(t, msgs[0]))
	assert.Equal(t, []string{`severity>=ERROR`}, src.filters[:1])

	src.entries = append(src.entries, logEntry(t0.Add(2*time.Minute), "b", "billing"),
		logEntry(t0.Add(2*time.Minute), "c", "checkout"))
	e.clk.set(t0.Add(2 * time.Minute))
	b.Tick(t.Context())

	incs := b.Incidents(testProject)
	require.Len(t, incs, 2, "one incident per label combination")
	assert.Len(t, e.pub.all(), 1, "second notification is held back by the rate limit")

	e.clk.set(t0.Add(40 * time.Minute))
	b.Tick(t.Context())

	for _, inc := range b.Incidents(testProject) {
		assert.Equal(t, "closed", inc.State)
	}

	assert.Len(t, e.pub.all(), 1, "log policies never notify on close")
}

func TestMatchedLogConditionRejectsBadExtractor(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	p := logPolicy(e.pubsubChannel(t))
	p.Conditions[0].GetConditionMatchedLog().LabelExtractors = map[string]string{"x": "BOGUS(a)"}

	_, err := e.alerts(t).
		CreateAlertPolicy(t.Context(), &monitoringpb.CreateAlertPolicyRequest{Name: proj, AlertPolicy: p})
	assert.Equal(t, codes.InvalidArgument, codeOf(err))
}

const (
	errorsMetric   = "custom.googleapis.com/test/errors"
	requestsMetric = "custom.googleapis.com/test/requests"
)

func (e *env) writeCount(t *testing.T, metric string, at time.Time, v int64) {
	t.Helper()

	e.clk.set(at)
	e.write(t, intSeries(metric, metricpb.MetricDescriptor_DELTA, instance("inst-1"), at.Add(-time.Minute), at, v))
}

func ratioCondition(denomPeriod time.Duration, threshold float64) *monitoringpb.AlertPolicy_Condition {
	sum := func(p time.Duration) []*monitoringpb.Aggregation {
		return []*monitoringpb.Aggregation{
			agg(p, monitoringpb.Aggregation_ALIGN_SUM, monitoringpb.Aggregation_REDUCE_SUM),
		}
	}

	return &monitoringpb.AlertPolicy_Condition{
		DisplayName: "error ratio",
		Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{
			ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
				Filter:                  `metric.type = "` + errorsMetric + `"`,
				Aggregations:            sum(time.Minute),
				DenominatorFilter:       `metric.type = "` + requestsMetric + `"`,
				DenominatorAggregations: sum(denomPeriod),
				Comparison:              monitoringpb.ComparisonType_COMPARISON_GT,
				ThresholdValue:          threshold,
				Duration:                durationpb.New(0),
			},
		},
	}
}

func TestRatioCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		errs, reqs   int64
		denomPeriod  time.Duration
		wantIncident bool
		wantCode     codes.Code
	}{
		{"ratio above threshold", 6, 10, time.Minute, true, codes.OK},
		{"ratio below threshold", 2, 10, time.Minute, false, codes.OK},
		{"mismatched alignment period", 6, 10, 2 * time.Minute, false, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			p := &monitoringpb.AlertPolicy{
				DisplayName: "ratio", Combiner: monitoringpb.AlertPolicy_OR,
				Conditions: []*monitoringpb.AlertPolicy_Condition{ratioCondition(tt.denomPeriod, 0.5)},
			}

			_, err := e.alerts(t).CreateAlertPolicy(t.Context(),
				&monitoringpb.CreateAlertPolicyRequest{Name: proj, AlertPolicy: p})
			require.Equal(t, tt.wantCode, codeOf(err))

			if tt.wantCode != codes.OK {
				return
			}

			t0 := epoch()
			e.writeCount(t, errorsMetric, t0, tt.errs)
			e.writeCount(t, requestsMetric, t0, tt.reqs)
			e.handler.Backend.Tick(t.Context())

			assert.Equal(t, tt.wantIncident, len(e.handler.Backend.Incidents(testProject)) == 1)
		})
	}
}

func TestForecastCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		horizon   time.Duration
		threshold float64
		wantFire  bool
		wantCode  codes.Code
	}{
		{"forecast crosses threshold", time.Hour, 50, true, codes.OK},
		{"forecast stays below", time.Hour, 100, false, codes.OK},
		{"horizon below one hour", 30 * time.Minute, 50, false, codes.InvalidArgument},
		{"horizon above sixty hours", 61 * time.Hour, 50, false, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			cond := thresholdCondition("forecast", metricFilter, monitoringpb.ComparisonType_COMPARISON_GT,
				tt.threshold, 0)
			cond.GetConditionThreshold().ForecastOptions = &monitoringpb.AlertPolicy_Condition_MetricThreshold_ForecastOptions{
				ForecastHorizon: durationpb.New(tt.horizon),
			}

			_, err := e.alerts(t).CreateAlertPolicy(t.Context(), &monitoringpb.CreateAlertPolicyRequest{
				Name: proj, AlertPolicy: &monitoringpb.AlertPolicy{
					DisplayName: "forecast", Combiner: monitoringpb.AlertPolicy_OR,
					Conditions: []*monitoringpb.AlertPolicy_Condition{cond},
				},
			})
			require.Equal(t, tt.wantCode, codeOf(err))

			if tt.wantCode != codes.OK {
				return
			}

			t0 := epoch()
			for m := 1; m <= 5; m++ {
				e.writeAt(t, t0.Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": float64(m)})
			}

			e.handler.Backend.Tick(t.Context())
			assert.Equal(t, tt.wantFire, len(e.handler.Backend.Incidents(testProject)) == 1)
		})
	}
}
