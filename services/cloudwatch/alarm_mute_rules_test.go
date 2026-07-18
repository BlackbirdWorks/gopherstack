package cloudwatch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// AlarmMuteRule: CRUD
// ---------------------------------------------------------------------------

func TestBackend_AlarmMuteRule_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutAlarmMuteRule(&cloudwatch.AlarmMuteRule{
		MuteName:     "mute1",
		AlarmNames:   []string{"alarm1", "alarm2"},
		MuteDuration: 3600,
	}))

	rule, err := b.GetAlarmMuteRule("mute1")
	require.NoError(t, err)
	assert.Equal(t, "mute1", rule.MuteName)
	assert.Len(t, rule.AlarmNames, 2)

	require.NoError(t, b.DeleteAlarmMuteRule("mute1"))
	_, err = b.GetAlarmMuteRule("mute1")
	assert.Error(t, err)
}

// TestBackend_ListAlarmMuteRules verifies pagination and ordering.
func TestBackend_ListAlarmMuteRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantFirst  string
		seed       []cloudwatch.AlarmMuteRule
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
				{MuteName: "z-rule", MuteDuration: 60},
				{MuteName: "a-rule", MuteDuration: 60},
				{MuteName: "m-rule", MuteDuration: 60},
			},
			wantLen:   3,
			wantFirst: "a-rule",
		},
		{
			name: "maxResults limits page",
			seed: []cloudwatch.AlarmMuteRule{
				{MuteName: "r1", MuteDuration: 60},
				{MuteName: "r2", MuteDuration: 60},
				{MuteName: "r3", MuteDuration: 60},
			},
			maxResults: 2,
			wantLen:    2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := cloudwatch.NewInMemoryBackend()
			for i := range tc.seed {
				b.PutAlarmMuteRuleInternal(&tc.seed[i])
			}

			p, err := b.ListAlarmMuteRules("", tc.maxResults)
			require.NoError(t, err)
			assert.Len(t, p.Data, tc.wantLen)
			if tc.wantFirst != "" && len(p.Data) > 0 {
				assert.Equal(t, tc.wantFirst, p.Data[0].MuteName)
			}
		})
	}
}
