package gcpmonitoring_test

import (
	"testing"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestEvaluationMissingData(t *testing.T) {
	t.Parallel()

	const (
		unspec = monitoringpb.AlertPolicy_Condition_EVALUATION_MISSING_DATA_UNSPECIFIED
		inact  = monitoringpb.AlertPolicy_Condition_EVALUATION_MISSING_DATA_INACTIVE
		active = monitoringpb.AlertPolicy_Condition_EVALUATION_MISSING_DATA_ACTIVE
		noop   = monitoringpb.AlertPolicy_Condition_EVALUATION_MISSING_DATA_NO_OP
	)

	tests := []struct {
		name     string
		value    float64
		mode     monitoringpb.AlertPolicy_Condition_EvaluationMissingData
		wantOpen bool
	}{
		{"unspecified keeps open incident", 9, unspec, true},
		{"no-op keeps open incident", 9, noop, true},
		{"inactive closes open incident", 9, inact, false},
		{"active keeps open incident", 9, active, true},
		{"unspecified does not open", 0, unspec, false},
		{"no-op does not open", 0, noop, false},
		{"inactive does not open", 0, inact, false},
		{"active opens without data", 0, active, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			p := queuePolicy(nil, monitoringpb.ComparisonType_COMPARISON_GT, 3)
			p.Conditions[0].GetConditionThreshold().EvaluationMissingData = tt.mode
			e.createPolicy(t, p)
			b := e.handler.Backend
			t0 := epoch()

			e.writeAt(t, t0.Add(time.Minute), "gold", map[string]float64{"inst-1": tt.value})
			e.writeAt(t, t0.Add(2*time.Minute), "gold", map[string]float64{"inst-1": tt.value})
			b.Tick(t.Context())

			e.clk.set(t0.Add(30 * time.Minute))
			b.Tick(t.Context())

			open := 0

			for _, inc := range b.Incidents(testProject) {
				if inc.State == "open" {
					open++
				}
			}

			assert.Equal(t, map[bool]int{true: 1, false: 0}[tt.wantOpen], open)
		})
	}
}

func TestRenotifyIntervalPerChannel(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ps := e.pubsubChannel(t)
	mail, err := e.channels(t).CreateNotificationChannel(t.Context(), &monitoringpb.CreateNotificationChannelRequest{
		Name: proj,
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type: "email", DisplayName: "ops", Labels: map[string]string{"email_address": "ops@example.com"},
		},
	})
	require.NoError(t, err)

	p := queuePolicy([]string{ps, mail.GetName()}, monitoringpb.ComparisonType_COMPARISON_GT, 3)
	p.AlertStrategy = &monitoringpb.AlertPolicy_AlertStrategy{
		NotificationChannelStrategy: []*monitoringpb.AlertPolicy_AlertStrategy_NotificationChannelStrategy{{
			NotificationChannelNames: []string{ps}, RenotifyInterval: durationpb.New(30 * time.Minute),
		}},
	}
	e.createPolicy(t, p)

	b := e.handler.Backend
	t0 := epoch()

	for m := 1; m <= 65; m++ {
		e.writeAt(t, t0.Add(time.Duration(m)*time.Minute), "gold", map[string]float64{"inst-1": 9})
		b.Tick(t.Context())
	}

	msgs := e.pub.all()
	require.Len(t, msgs, 3, "open, then a reminder every 30 minutes")

	for _, m := range msgs {
		assert.Equal(t, "open", decode(t, m).Incident.State)
	}

	box := b.Outbox()
	require.Len(t, box, 1, "the email channel has no renotify interval")
	assert.Equal(t, "ops@example.com", box[0].Recipient)
	assert.Equal(t, "email", box[0].Type)
	assert.Contains(t, box[0].Payload, `"version":"1.2"`)
}

func TestChannelStrategyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		names func(ch string) []string
		name  string
		every time.Duration
	}{
		{name: "interval too short", names: func(ch string) []string { return []string{ch} }, every: time.Minute},
		{name: "interval too long", names: func(ch string) []string { return []string{ch} }, every: 25 * time.Hour},
		{
			name:  "unknown channel",
			names: func(string) []string { return []string{proj + "/notificationChannels/nope"} },
			every: time.Hour,
		},
		{name: "no channels", names: func(string) []string { return nil }, every: time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ch := e.pubsubChannel(t)
			p := queuePolicy([]string{ch}, monitoringpb.ComparisonType_COMPARISON_GT, 3)
			p.AlertStrategy = &monitoringpb.AlertPolicy_AlertStrategy{
				NotificationChannelStrategy: []*monitoringpb.AlertPolicy_AlertStrategy_NotificationChannelStrategy{{
					NotificationChannelNames: tt.names(ch), RenotifyInterval: durationpb.New(tt.every),
				}},
			}

			_, err := e.alerts(t).CreateAlertPolicy(t.Context(),
				&monitoringpb.CreateAlertPolicyRequest{Name: proj, AlertPolicy: p})
			assert.Equal(t, codes.InvalidArgument, codeOf(err))
		})
	}
}

func TestMetricIncidentAutoClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		autoClose time.Duration
		idle      time.Duration
		wantOpen  bool
	}{
		{"before auto close", time.Hour, 30 * time.Minute, true},
		{"after auto close", time.Hour, 61 * time.Minute, false},
		{"default is seven days", 0, 6 * 24 * time.Hour, true},
		{"default elapsed", 0, 8 * 24 * time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			p := queuePolicy([]string{e.pubsubChannel(t)}, monitoringpb.ComparisonType_COMPARISON_GT, 3)
			p.AlertStrategy = &monitoringpb.AlertPolicy_AlertStrategy{}
			if tt.autoClose > 0 {
				p.AlertStrategy.AutoClose = durationpb.New(tt.autoClose)
			}

			e.createPolicy(t, p)
			b := e.handler.Backend
			t0 := epoch()

			e.writeAt(t, t0.Add(time.Minute), "gold", map[string]float64{"inst-1": 9})
			e.writeAt(t, t0.Add(2*time.Minute), "gold", map[string]float64{"inst-1": 9})
			b.Tick(t.Context())
			require.Len(t, e.pub.all(), 1)

			e.clk.set(t0.Add(2*time.Minute + tt.idle))
			b.Tick(t.Context())

			incs := b.Incidents(testProject)
			require.Len(t, incs, 1)
			assert.Equal(t, tt.wantOpen, incs[0].State == "open")
			assert.Len(t, e.pub.all(), 1, "auto close sends no notification")
		})
	}
}
