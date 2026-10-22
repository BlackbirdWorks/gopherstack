package gcpmonitoring_test

import (
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const alertTopic = "projects/" + testProject + "/topics/alerts"

type payload struct {
	Incident struct {
		Resource struct {
			Labels map[string]string `json:"labels"`
		} `json:"resource"`
		State    string `json:"state"`
		Policy   string `json:"policy_name"`
		Observed string `json:"observed_value"`
		Metric   struct {
			Type string `json:"type"`
		} `json:"metric"`
	} `json:"incident"`
	Version string `json:"version"`
}

func (e *env) pubsubChannel(t *testing.T) string {
	t.Helper()

	ch, err := e.channels(t).CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
		Name: proj,
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type: "pubsub", DisplayName: "alerts", Labels: map[string]string{"topic": alertTopic},
		},
	})
	require.NoError(t, err)

	return ch.GetName()
}

func thresholdCondition(
	name, filter string, cmp monitoringpb.ComparisonType, threshold float64, dur time.Duration,
) *monitoringpb.AlertPolicy_Condition {
	return &monitoringpb.AlertPolicy_Condition{
		DisplayName: name,
		Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{
			ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
				Filter:         filter,
				Aggregations:   []*monitoringpb.Aggregation{agg(time.Minute, monitoringpb.Aggregation_ALIGN_MEAN, 0)},
				Comparison:     cmp,
				ThresholdValue: threshold,
				Duration:       durationpb.New(dur),
			},
		},
	}
}

func queuePolicy(channels []string, cmp monitoringpb.ComparisonType, threshold float64) *monitoringpb.AlertPolicy {
	return &monitoringpb.AlertPolicy{
		DisplayName: "queue too deep", Combiner: monitoringpb.AlertPolicy_OR, NotificationChannels: channels,
		Conditions: []*monitoringpb.AlertPolicy_Condition{
			thresholdCondition(
				"queue",
				metricFilter+` AND resource.type = "gce_instance"`,
				cmp,
				threshold,
				2*time.Minute,
			),
		},
	}
}

func (e *env) createPolicy(t *testing.T, p *monitoringpb.AlertPolicy) *monitoringpb.AlertPolicy {
	t.Helper()

	got, err := e.alerts(t).
		CreateAlertPolicy(t.Context(), &monitoringpb.CreateAlertPolicyRequest{Name: proj, AlertPolicy: p})
	require.NoError(t, err)

	return got
}

func decode(t *testing.T, msg published) payload {
	t.Helper()

	var p payload
	require.NoError(t, json.Unmarshal([]byte(msg.data), &p))

	return p
}

func TestAlertPolicyFiresAndRecovers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cmp       monitoringpb.ComparisonType
		threshold float64
		bad, good float64
	}{
		{"greater than", monitoringpb.ComparisonType_COMPARISON_GT, 3, 5, 1},
		{"greater or equal", monitoringpb.ComparisonType_COMPARISON_GE, 5, 5, 4},
		{"less than", monitoringpb.ComparisonType_COMPARISON_LT, 3, 1, 5},
		{"less or equal", monitoringpb.ComparisonType_COMPARISON_LE, 1, 1, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ch := e.pubsubChannel(t)
			e.createPolicy(t, queuePolicy([]string{ch}, tt.cmp, tt.threshold))
			b := e.handler.Backend
			t0 := epoch()

			e.writeAt(t, t0.Add(time.Minute), "gold", map[string]float64{"inst-1": tt.bad})
			b.Tick(t.Context())
			assert.Empty(t, e.pub.all(), "duration not yet elapsed")

			e.writeAt(t, t0.Add(2*time.Minute), "gold", map[string]float64{"inst-1": tt.bad})
			b.Tick(t.Context())
			b.Tick(t.Context())

			msgs := e.pub.all()
			require.Len(t, msgs, 1)
			assert.Equal(t, alertTopic, msgs[0].topic)

			opened := decode(t, msgs[0])
			assert.Equal(t, "open", opened.Incident.State)
			assert.Equal(t, "queue too deep", opened.Incident.Policy)
			assert.Equal(t, queueDepth, opened.Incident.Metric.Type)
			assert.Equal(t, "inst-1", opened.Incident.Resource.Labels["instance_id"])
			assert.Equal(t, "1.2", opened.Version)

			incs := b.Incidents(testProject)
			require.Len(t, incs, 1)
			assert.Equal(t, "open", incs[0].State)
			assert.Equal(t, t0.Add(2*time.Minute), incs[0].StartedAt)

			e.writeAt(t, t0.Add(3*time.Minute), "gold", map[string]float64{"inst-1": tt.good})
			b.Tick(t.Context())

			msgs = e.pub.all()
			require.Len(t, msgs, 2)
			assert.Equal(t, "closed", decode(t, msgs[1]).Incident.State)

			incs = b.Incidents(testProject)
			require.Len(t, incs, 1)
			assert.Equal(t, "closed", incs[0].State)
			assert.Equal(t, t0.Add(3*time.Minute), incs[0].EndedAt)
		})
	}
}

func TestAlertPolicyPerSeriesIncidents(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.pubsubChannel(t)
	e.createPolicy(t, queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3))
	t0 := epoch()

	for m := 1; m <= 2; m++ {
		e.writeAt(t, t0.Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9, "inst-2": 1})
	}

	e.handler.Backend.Tick(t.Context())

	msgs := e.pub.all()
	require.Len(t, msgs, 1)
	assert.Equal(t, "inst-1", decode(t, msgs[0]).Incident.Resource.Labels["instance_id"])
}

func TestAlertPolicyNotificationPrompts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prompts []monitoringpb.AlertPolicy_AlertStrategy_NotificationPrompt
		want    []string
	}{
		{"default", nil, []string{"open", "closed"}},
		{
			"opened only",
			[]monitoringpb.AlertPolicy_AlertStrategy_NotificationPrompt{monitoringpb.AlertPolicy_AlertStrategy_OPENED},
			[]string{"open"},
		},
		{
			"closed only",
			[]monitoringpb.AlertPolicy_AlertStrategy_NotificationPrompt{monitoringpb.AlertPolicy_AlertStrategy_CLOSED},
			[]string{"closed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			p := queuePolicy([]string{e.pubsubChannel(t)}, monitoringpb.ComparisonType_COMPARISON_GT, 3)
			p.AlertStrategy = &monitoringpb.AlertPolicy_AlertStrategy{NotificationPrompts: tt.prompts}
			e.createPolicy(t, p)
			t0 := epoch()

			e.writeAt(t, t0.Add(time.Minute), "gold", map[string]float64{"inst-1": 9})
			e.writeAt(t, t0.Add(2*time.Minute), "gold", map[string]float64{"inst-1": 9})
			e.handler.Backend.Tick(t.Context())
			e.writeAt(t, t0.Add(3*time.Minute), "gold", map[string]float64{"inst-1": 0})
			e.handler.Backend.Tick(t.Context())

			msgs := e.pub.all()
			got := make([]string, 0, len(msgs))

			for _, m := range msgs {
				got = append(got, decode(t, m).Incident.State)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAlertPolicyCombiners(t *testing.T) {
	t.Parallel()

	other := "custom.googleapis.com/test/errors"
	tests := []struct {
		name     string
		combiner monitoringpb.AlertPolicy_ConditionCombinerType
		errValue float64
		want     int
	}{
		{"or one condition", monitoringpb.AlertPolicy_OR, 0, 1},
		{"and one condition", monitoringpb.AlertPolicy_AND, 0, 0},
		{"and both", monitoringpb.AlertPolicy_AND, 9, 1},
		{"and matching resource both", monitoringpb.AlertPolicy_AND_WITH_MATCHING_RESOURCE, 9, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			p := queuePolicy([]string{e.pubsubChannel(t)}, monitoringpb.ComparisonType_COMPARISON_GT, 3)
			p.Combiner = tt.combiner
			p.Conditions = append(p.Conditions, thresholdCondition("errors", `metric.type = "`+other+`"`,
				monitoringpb.ComparisonType_COMPARISON_GT, 3, 2*time.Minute))
			e.createPolicy(t, p)

			for m := 1; m <= 2; m++ {
				at := epoch().Add(time.Duration(m) * time.Minute)
				e.writeAt(t, at, "gold", map[string]float64{"inst-1": 9})
				e.write(t, gaugeDouble(other, nil, instance("inst-1"), at, tt.errValue))
			}

			e.handler.Backend.Tick(t.Context())
			assert.Len(t, e.pub.all(), tt.want)
		})
	}
}

func TestAlertPolicyTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		trigger *monitoringpb.AlertPolicy_Condition_Trigger
		name    string
		want    int
	}{
		{nil, "any one", 1},
		{
			&monitoringpb.AlertPolicy_Condition_Trigger{
				Type: &monitoringpb.AlertPolicy_Condition_Trigger_Count{Count: 2},
			},
			"count two",
			0,
		},
		{
			&monitoringpb.AlertPolicy_Condition_Trigger{
				Type: &monitoringpb.AlertPolicy_Condition_Trigger_Percent{Percent: 50},
			},
			"half",
			1,
		},
		{
			&monitoringpb.AlertPolicy_Condition_Trigger{
				Type: &monitoringpb.AlertPolicy_Condition_Trigger_Percent{Percent: 100},
			},
			"all",
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			p := queuePolicy([]string{e.pubsubChannel(t)}, monitoringpb.ComparisonType_COMPARISON_GT, 3)
			p.Conditions[0].GetConditionThreshold().Trigger = tt.trigger
			e.createPolicy(t, p)

			for m := 1; m <= 2; m++ {
				e.writeAt(
					t,
					epoch().Add(time.Duration(m)*time.Minute),
					"gold",
					map[string]float64{"inst-1": 9, "inst-2": 1},
				)
			}

			e.handler.Backend.Tick(t.Context())
			assert.Len(t, e.pub.all(), tt.want)
		})
	}
}

func TestAlertPolicyAbsentCondition(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	p := &monitoringpb.AlertPolicy{
		DisplayName: "gone quiet", Combiner: monitoringpb.AlertPolicy_OR,
		NotificationChannels: []string{e.pubsubChannel(t)},
		Conditions: []*monitoringpb.AlertPolicy_Condition{{
			DisplayName: "absent",
			Condition: &monitoringpb.AlertPolicy_Condition_ConditionAbsent{
				ConditionAbsent: &monitoringpb.AlertPolicy_Condition_MetricAbsence{
					Filter: metricFilter, Duration: durationpb.New(2 * time.Minute),
				},
			},
		}},
	}
	e.createPolicy(t, p)

	t0 := epoch()
	e.writeAt(t, t0, "gold", map[string]float64{"inst-1": 1})
	e.clk.set(t0.Add(time.Minute))
	e.handler.Backend.Tick(t.Context())
	assert.Empty(t, e.pub.all())

	e.clk.set(t0.Add(3 * time.Minute))
	e.handler.Backend.Tick(t.Context())
	require.Len(t, e.pub.all(), 1)

	e.writeAt(t, t0.Add(4*time.Minute), "gold", map[string]float64{"inst-1": 1})
	e.handler.Backend.Tick(t.Context())
	require.Len(t, e.pub.all(), 2)
	assert.Equal(t, "closed", decode(t, e.pub.all()[1]).Incident.State)
}

func TestAlertPolicyDisabledClosesIncident(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	created := e.createPolicy(
		t,
		queuePolicy([]string{e.pubsubChannel(t)}, monitoringpb.ComparisonType_COMPARISON_GT, 3),
	)

	for m := 1; m <= 2; m++ {
		e.writeAt(t, epoch().Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
	}

	e.handler.Backend.Tick(t.Context())
	require.Len(t, e.pub.all(), 1)

	_, err := e.alerts(t).UpdateAlertPolicy(t.Context(), &monitoringpb.UpdateAlertPolicyRequest{
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"enabled"}},
		AlertPolicy: &monitoringpb.AlertPolicy{Name: created.GetName(), Enabled: wrapperspb.Bool(false)},
	})
	require.NoError(t, err)

	e.handler.Backend.Tick(t.Context())

	incs := e.handler.Backend.Incidents(testProject)
	require.Len(t, incs, 1)
	assert.Equal(t, "closed", incs[0].State)
	assert.Len(t, e.pub.all(), 1, "closing for a disabled policy sends nothing")
}
