package cloudwatch_test

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestListAlarmMuteRules_ReturnsStoredRules verifies that ListAlarmMuteRules
// (query protocol) returns rules previously created via PutAlarmMuteRule.
// Real query-protocol summaries carry only AlarmMuteRuleArn (no Name), per
// botocore cloudwatch 2010-08-01 service-2.json's AlarmMuteRuleSummary shape.
func TestListAlarmMuteRules_ReturnsStoredRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    []string
		wantLen int
	}{
		{name: "empty store", seed: nil, wantLen: 0},
		{name: "single rule", seed: []string{"mute-prod"}, wantLen: 1},
		{name: "multiple rules", seed: []string{"mute-alpha", "mute-beta", "mute-gamma"}, wantLen: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()

			for _, name := range tc.seed {
				rec := postForm(t, h, url.Values{
					"Action":                   []string{"PutAlarmMuteRule"},
					"Name":                     []string{name},
					"Rule.Schedule.Expression": []string{"cron(0 2 * * *)"},
					"Rule.Schedule.Duration":   []string{"PT1H"},
				}.Encode())
				require.Equal(t, http.StatusOK, rec.Code, "PutAlarmMuteRule %s: %s", name, rec.Body.String())
			}

			rec := postForm(t, h, url.Values{"Action": []string{"ListAlarmMuteRules"}}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "ListAlarmMuteRulesResponse")

			type summary struct {
				AlarmMuteRuleArn string `xml:"AlarmMuteRuleArn"`
			}
			type listResp struct {
				XMLName xml.Name `xml:"ListAlarmMuteRulesResponse"`
				Result  struct {
					Summaries []summary `xml:"AlarmMuteRuleSummaries>member"`
				} `xml:"ListAlarmMuteRulesResult"`
			}
			var r listResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &r))
			assert.Len(t, r.Result.Summaries, tc.wantLen)
		})
	}
}

func TestCloudWatchHandler_AlarmMuteRule(t *testing.T) {
	t.Parallel()

	validSchedule := url.Values{
		"Rule.Schedule.Expression": []string{"cron(0 2 * * *)"},
		"Rule.Schedule.Duration":   []string{"PT1H"},
	}

	tests := []struct {
		setup           func(t *testing.T, b *cloudwatch.InMemoryBackend)
		body            func() url.Values
		name            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "put success",
			body: func() url.Values {
				v := url.Values{
					"Action":                          []string{"PutAlarmMuteRule"},
					"Name":                            []string{"my-mute-rule"},
					"Description":                     []string{"suppress noisy alerts"},
					"MuteTargets.AlarmNames.member.1": []string{"alarm-a"},
				}
				maps.Copy(v, validSchedule)

				return v
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"PutAlarmMuteRuleResponse"},
		},
		{
			name: "put missing name",
			body: func() url.Values {
				return url.Values{"Action": []string{"PutAlarmMuteRule"}}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "put missing schedule",
			body: func() url.Values {
				return url.Values{"Action": []string{"PutAlarmMuteRule"}, "Name": []string{"no-schedule"}}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "put updates existing",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{Name: "update-mute"})
			},
			body: func() url.Values {
				v := url.Values{
					"Action":      []string{"PutAlarmMuteRule"},
					"Name":        []string{"update-mute"},
					"Description": []string{"updated"},
				}
				maps.Copy(v, validSchedule)

				return v
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"PutAlarmMuteRuleResponse"},
		},
		{
			name: "get success",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{
					Name:        "my-mute-rule",
					Description: "suppress noisy alerts",
					Schedule:    cloudwatch.AlarmMuteRuleSchedule{Expression: "cron(0 2 * * *)", Duration: "PT1H"},
				})
			},
			body: func() url.Values {
				return url.Values{
					"Action":            []string{"GetAlarmMuteRule"},
					"AlarmMuteRuleName": []string{"my-mute-rule"},
				}
			},
			wantCode: http.StatusOK,
			wantContains: []string{
				"GetAlarmMuteRuleResponse",
				"my-mute-rule",
				"suppress noisy alerts",
				"cron(0 2 * * *)",
			},
		},
		{
			name: "get not found",
			body: func() url.Values {
				return url.Values{
					"Action":            []string{"GetAlarmMuteRule"},
					"AlarmMuteRuleName": []string{"ghost-rule"},
				}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "get missing name",
			body: func() url.Values {
				return url.Values{"Action": []string{"GetAlarmMuteRule"}}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "delete success",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{Name: "delete-me"})
			},
			body: func() url.Values {
				return url.Values{
					"Action":            []string{"DeleteAlarmMuteRule"},
					"AlarmMuteRuleName": []string{"delete-me"},
				}
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteAlarmMuteRuleResponse"},
		},
		{
			// DeleteAlarmMuteRule is idempotent (aws-sdk-go-v2 cloudwatch@v1.66.3
			// api_op_DeleteAlarmMuteRule.go:19): deleting a rule that doesn't
			// exist succeeds, it does not 400.
			name: "delete not found is idempotent",
			body: func() url.Values {
				return url.Values{
					"Action":            []string{"DeleteAlarmMuteRule"},
					"AlarmMuteRuleName": []string{"ghost-rule"},
				}
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteAlarmMuteRuleResponse"},
		},
		{
			name: "delete missing name",
			body: func() url.Values {
				return url.Values{"Action": []string{"DeleteAlarmMuteRule"}}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newCWHandlerWithBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postForm(t, h, tt.body().Encode())

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestCloudWatchHandler_AlarmMuteRule_MuteTargets(t *testing.T) {
	t.Parallel()

	h, b := newCWHandlerWithBackend()
	b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{
		Name:       "mute-with-alarms",
		AlarmNames: []string{"alarm-1", "alarm-2", "alarm-3"},
	})

	rec := postForm(t, h, url.Values{
		"Action":            []string{"GetAlarmMuteRule"},
		"AlarmMuteRuleName": []string{"mute-with-alarms"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "alarm-1")
	assert.Contains(t, body, "alarm-2")
	assert.Contains(t, body, "alarm-3")
	assert.Contains(t, body, "MuteTargets")
}
