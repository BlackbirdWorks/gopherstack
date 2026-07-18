package cloudwatch_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestListAlarmMuteRules_ReturnsStoredRules verifies that ListAlarmMuteRules
// returns rules previously created via PutAlarmMuteRule.
func TestListAlarmMuteRules_ReturnsStoredRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		seed         []string
		wantNames    []string
		wantNotNames []string
	}{
		{
			name:      "empty store",
			seed:      nil,
			wantNames: nil,
		},
		{
			name:      "single rule",
			seed:      []string{"mute-prod"},
			wantNames: []string{"mute-prod"},
		},
		{
			name:      "multiple rules",
			seed:      []string{"mute-alpha", "mute-beta", "mute-gamma"},
			wantNames: []string{"mute-alpha", "mute-beta", "mute-gamma"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()

			for _, name := range tc.seed {
				rec := postForm(t, h, "Action=PutAlarmMuteRule&MuteName="+name+"&MuteDuration=3600")
				require.Equal(t, http.StatusOK, rec.Code, "PutAlarmMuteRule %s", name)
			}

			rec := postForm(t, h, "Action=ListAlarmMuteRules")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "ListAlarmMuteRulesResponse")

			type muteRule struct {
				MuteName string `xml:"MuteName"`
			}
			type listResp struct {
				XMLName xml.Name `xml:"ListAlarmMuteRulesResponse"`
				Result  struct {
					Rules []muteRule `xml:"MuteRules>member"`
				} `xml:"ListAlarmMuteRulesResult"`
			}
			var r listResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &r))

			got := make([]string, 0, len(r.Result.Rules))
			for _, rule := range r.Result.Rules {
				got = append(got, rule.MuteName)
			}

			for _, want := range tc.wantNames {
				assert.Contains(t, got, want)
			}
			if tc.wantNames == nil {
				assert.Empty(t, r.Result.Rules)
			}
		})
	}
}

func TestCloudWatchHandler_AlarmMuteRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *cloudwatch.Handler, b *cloudwatch.InMemoryBackend)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "PutAlarmMuteRule/success",
			body: "Action=PutAlarmMuteRule&MuteName=my-mute-rule" +
				"&Description=suppress+noisy+alerts" +
				"&AlarmNames.member.1=alarm-a" +
				"&MuteDuration=3600",
			wantCode:     http.StatusOK,
			wantContains: []string{"PutAlarmMuteRuleResponse"},
		},
		{
			name:     "PutAlarmMuteRule/missing name",
			body:     "Action=PutAlarmMuteRule",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "UpdateAlarmMuteRule/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{MuteName: "update-mute"})
			},
			body:         "Action=UpdateAlarmMuteRule&MuteName=update-mute&Description=updated",
			wantCode:     http.StatusOK,
			wantContains: []string{"UpdateAlarmMuteRuleResponse"},
		},
		{
			name:     "UpdateAlarmMuteRule/not found",
			body:     "Action=UpdateAlarmMuteRule&MuteName=missing-mute",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "GetAlarmMuteRule/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{
					MuteName:    "my-mute-rule",
					Description: "suppress noisy alerts",
				})
			},
			body:         "Action=GetAlarmMuteRule&MuteName=my-mute-rule",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetAlarmMuteRuleResponse", "my-mute-rule", "suppress noisy alerts"},
		},
		{
			name:     "GetAlarmMuteRule/not found",
			body:     "Action=GetAlarmMuteRule&MuteName=ghost-rule",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GetAlarmMuteRule/missing name",
			body:     "Action=GetAlarmMuteRule",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "DeleteAlarmMuteRule/success",
			setup: func(t *testing.T, _ *cloudwatch.Handler, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{MuteName: "delete-me"})
			},
			body:         "Action=DeleteAlarmMuteRule&MuteName=delete-me",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteAlarmMuteRuleResponse"},
		},
		{
			name:     "DeleteAlarmMuteRule/not found",
			body:     "Action=DeleteAlarmMuteRule&MuteName=ghost-rule",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DeleteAlarmMuteRule/missing name",
			body:     "Action=DeleteAlarmMuteRule",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newCWHandlerWithBackend()
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_AlarmMuteRule_AlarmNames(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()
	b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{
		MuteName:   "mute-with-alarms",
		AlarmNames: []string{"alarm-1", "alarm-2", "alarm-3"},
	})

	rec := postForm(t, h, "Action=GetAlarmMuteRule&MuteName=mute-with-alarms")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "alarm-1")
	assert.Contains(t, body, "alarm-2")
	assert.Contains(t, body, "alarm-3")
	assert.Contains(t, body, "AlarmNames")
}
