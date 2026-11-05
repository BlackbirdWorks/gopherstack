package gcpmonitoring_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

func promPolicy(channel string, pc *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition,
) *monitoringpb.AlertPolicy {
	return &monitoringpb.AlertPolicy{
		DisplayName: "promql queue", Combiner: monitoringpb.AlertPolicy_OR, NotificationChannels: []string{channel},
		AlertStrategy: &monitoringpb.AlertPolicy_AlertStrategy{
			NotificationPrompts: []monitoringpb.AlertPolicy_AlertStrategy_NotificationPrompt{
				monitoringpb.AlertPolicy_AlertStrategy_OPENED, monitoringpb.AlertPolicy_AlertStrategy_CLOSED,
			},
		},
		Conditions: []*monitoringpb.AlertPolicy_Condition{{
			DisplayName: "deep queue",
			Condition: &monitoringpb.AlertPolicy_Condition_ConditionPrometheusQueryLanguage{
				ConditionPrometheusQueryLanguage: pc,
			},
		}},
	}
}

type rulesReply struct {
	Data struct {
		Groups []struct {
			Name  string `json:"name"`
			Rules []struct {
				Labels map[string]string `json:"labels"`
				Name   string            `json:"name"`
				Query  string            `json:"query"`
				State  string            `json:"state"`
				Type   string            `json:"type"`
				Alerts []struct {
					Labels map[string]string `json:"labels"`
					State  string            `json:"state"`
					Value  string            `json:"value"`
				} `json:"alerts"`
				Duration float64 `json:"duration"`
			} `json:"rules"`
			Interval float64 `json:"interval"`
		} `json:"groups"`
	} `json:"data"`
}

type alertsReply struct {
	Data struct {
		Alerts []struct {
			Labels map[string]string `json:"labels"`
			State  string            `json:"state"`
		} `json:"alerts"`
	} `json:"data"`
}

func TestPromQLAlertFiresAfterDurationAndNotifiesWebhook(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	hs, hookURL := newHookServer(t, http.StatusOK)

	ch, err := e.channels(t).CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
		Name: proj,
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type: "webhook_tokenauth", DisplayName: "hook",
			Labels: map[string]string{"url": hookURL, "auth_token": "tok"},
		},
	})
	require.NoError(t, err)

	e.createPolicy(t, promPolicy(ch.GetName(), &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{
		Query:    promQueueDepth + ` > 3`,
		Duration: durationpb.New(2 * time.Minute), EvaluationInterval: durationpb.New(30 * time.Second),
		Labels:    map[string]string{"severity": "page", "inst": "{{ $labels.instance_id }}"},
		RuleGroup: "queues", AlertRule: "QueueTooDeep",
	}))

	b := e.handler.Backend
	t0 := epoch()
	e.writeAt(t, t0, "gold", map[string]float64{"inst-1": 9, "inst-2": 1})

	b.Tick(t.Context())
	assert.Empty(t, hs.requests(), "pending until the duration has elapsed")

	code, body := e.promGet(t, promBase+"alerts", url.Values{})
	require.Equal(t, http.StatusOK, code)

	var alerts alertsReply
	require.NoError(t, json.Unmarshal(body, &alerts))
	require.Len(t, alerts.Data.Alerts, 1)
	assert.Equal(t, "pending", alerts.Data.Alerts[0].State)

	e.clk.set(t0.Add(time.Minute))
	b.Tick(t.Context())
	assert.Empty(t, hs.requests())

	e.clk.set(t0.Add(2 * time.Minute))
	b.Tick(t.Context())

	reqs := hs.requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "tok", reqs[0].query)
	assert.Contains(t, reqs[0].body, `"state":"open"`)
	assert.Contains(t, reqs[0].body, "promql queue")

	incs := b.Incidents(testProject)
	require.Len(t, incs, 1)
	assert.Equal(t, t0.Add(2*time.Minute), incs[0].StartedAt)
	assert.Equal(t, "inst-1", incs[0].MetricLabels["instance_id"])
	assert.Equal(t, "page", incs[0].MetricLabels["severity"])
	assert.Equal(t, "inst-1", incs[0].MetricLabels["inst"])

	code, body = e.promGet(t, promBase+"rules", url.Values{})
	require.Equal(t, http.StatusOK, code)

	var rules rulesReply
	require.NoError(t, json.Unmarshal(body, &rules))
	require.Len(t, rules.Data.Groups, 1)
	assert.Equal(t, "queues", rules.Data.Groups[0].Name)
	assert.InDelta(t, 30, rules.Data.Groups[0].Interval, 0)
	require.Len(t, rules.Data.Groups[0].Rules, 1)

	rule := rules.Data.Groups[0].Rules[0]
	assert.Equal(t, "QueueTooDeep", rule.Name)
	assert.Equal(t, "firing", rule.State)
	assert.Equal(t, "alerting", rule.Type)
	assert.InDelta(t, 120, rule.Duration, 0)
	require.Len(t, rule.Alerts, 1)
	assert.Equal(t, "QueueTooDeep", rule.Alerts[0].Labels["alertname"])
	assert.Equal(t, "firing", rule.Alerts[0].State)
	assert.Equal(t, "9", rule.Alerts[0].Value)

	e.writeAt(t, t0.Add(3*time.Minute), "gold", map[string]float64{"inst-1": 1})
	b.Tick(t.Context())

	reqs = hs.requests()
	require.Len(t, reqs, 2)
	assert.Contains(t, reqs[1].body, `"state":"closed"`)
	assert.Equal(t, "closed", b.Incidents(testProject)[0].State)
}

func TestPromQLAlertHonoursEvaluationInterval(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.pubsubChannel(t)
	e.createPolicy(t, promPolicy(ch, &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{
		Query: promQueueDepth + ` > 3`, EvaluationInterval: durationpb.New(time.Minute),
	}))

	b := e.handler.Backend
	t0 := epoch()
	e.writeAt(t, t0, "gold", map[string]float64{"inst-1": 9})
	b.Tick(t.Context())
	require.Len(t, b.Incidents(testProject), 1)

	e.writeAt(t, t0.Add(10*time.Second), "gold", map[string]float64{"inst-1": 1})
	b.Tick(t.Context())
	assert.Equal(t, "open", b.Incidents(testProject)[0].State, "not due before the 60s interval")

	e.clk.set(t0.Add(time.Minute))
	b.Tick(t.Context())
	assert.Equal(t, "closed", b.Incidents(testProject)[0].State)
}

func TestPromQLConditionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pc   *monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition
		name string
		want codes.Code
	}{
		{name: "valid", pc: &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{
			Query: `up > 0`, AlertRule: "Down", RuleGroup: "g",
		}, want: codes.OK},
		{
			name: "empty query",
			pc:   &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{},
			want: codes.InvalidArgument,
		},
		{name: "syntax error", pc: &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{
			Query: `sum(`,
		}, want: codes.InvalidArgument},
		{
			name: "interval not a multiple of 30s",
			pc: &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{
				Query: `up`, EvaluationInterval: durationpb.New(45 * time.Second),
			},
			want: codes.InvalidArgument,
		},
		{name: "bad label name", pc: &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{
			Query: `up`, Labels: map[string]string{"bad-name": "x"},
		}, want: codes.InvalidArgument},
		{name: "bad alert rule", pc: &monitoringpb.AlertPolicy_Condition_PrometheusQueryLanguageCondition{
			Query: `up`, AlertRule: "has space",
		}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.alerts(t).CreateAlertPolicy(t.Context(), &monitoringpb.CreateAlertPolicyRequest{
				Name: proj, AlertPolicy: promPolicy(e.pubsubChannel(t), tt.pc),
			})
			assert.Equal(t, tt.want, codeOf(err))
		})
	}
}
