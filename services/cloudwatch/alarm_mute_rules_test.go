package cloudwatch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func TestBackend_AlarmMuteRule_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutAlarmMuteRule(&cloudwatch.AlarmMuteRule{
		Name:       "mute1",
		AlarmNames: []string{"alarm1", "alarm2"},
		Schedule: cloudwatch.AlarmMuteRuleSchedule{
			Expression: "cron(0 2 * * *)",
			Duration:   "PT1H",
		},
	}))

	rule, err := b.GetAlarmMuteRule("mute1")
	require.NoError(t, err)
	assert.Equal(t, "mute1", rule.Name)
	assert.Len(t, rule.AlarmNames, 2)
	assert.Equal(t, "RECURRING", rule.MuteType())

	require.NoError(t, b.DeleteAlarmMuteRule("mute1"))
	_, err = b.GetAlarmMuteRule("mute1")
	assert.Error(t, err)
}

func TestBackend_PutAlarmMuteRule_Validation(t *testing.T) {
	t.Parallel()

	validSchedule := cloudwatch.AlarmMuteRuleSchedule{Expression: "cron(0 2 * * *)", Duration: "PT1H"}

	tests := []struct {
		name string
		rule cloudwatch.AlarmMuteRule
	}{
		{name: "missing name", rule: cloudwatch.AlarmMuteRule{Schedule: validSchedule}},
		{name: "missing expression", rule: cloudwatch.AlarmMuteRule{
			Name:     "r",
			Schedule: cloudwatch.AlarmMuteRuleSchedule{Duration: "PT1H"},
		}},
		{name: "missing duration", rule: cloudwatch.AlarmMuteRule{
			Name:     "r",
			Schedule: cloudwatch.AlarmMuteRuleSchedule{Expression: "cron(0 2 * * *)"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := cloudwatch.NewInMemoryBackend()
			err := b.PutAlarmMuteRule(&tc.rule)
			require.ErrorIs(t, err, cloudwatch.ErrValidation)
		})
	}
}

func TestBackend_DeleteAlarmMuteRule_Idempotent(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.DeleteAlarmMuteRule("does-not-exist"))
}

// TestBackend_ListAlarmMuteRules verifies pagination, ordering, and filtering.
func TestBackend_ListAlarmMuteRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantFirst  string
		seed       []cloudwatch.AlarmMuteRule
		alarmName  string
		statuses   []string
		maxResults int
		wantLen    int
	}{
		{
			name:    "empty",
			seed:    nil,
			wantLen: 0,
		},
		{
			name: "alphabetical order",
			seed: []cloudwatch.AlarmMuteRule{
				{Name: "z-rule"},
				{Name: "a-rule"},
				{Name: "m-rule"},
			},
			wantLen:   3,
			wantFirst: "a-rule",
		},
		{
			name: "maxResults limits page",
			seed: []cloudwatch.AlarmMuteRule{
				{Name: "r1"},
				{Name: "r2"},
				{Name: "r3"},
			},
			maxResults: 2,
			wantLen:    2,
		},
		{
			name: "filter by alarm name",
			seed: []cloudwatch.AlarmMuteRule{
				{Name: "r1", AlarmNames: []string{"alarm-a"}},
				{Name: "r2", AlarmNames: []string{"alarm-b"}},
			},
			alarmName: "alarm-a",
			wantLen:   1,
			wantFirst: "r1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := cloudwatch.NewInMemoryBackend()
			for i := range tc.seed {
				b.PutAlarmMuteRuleInternal(&tc.seed[i])
			}

			p, err := b.ListAlarmMuteRules("", tc.maxResults, tc.alarmName, tc.statuses)
			require.NoError(t, err)
			assert.Len(t, p.Data, tc.wantLen)
			if tc.wantFirst != "" && len(p.Data) > 0 {
				assert.Equal(t, tc.wantFirst, p.Data[0].Name)
			}
		})
	}
}
