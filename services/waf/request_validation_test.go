package waf_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_RequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode string
		wantMsg  string
		want     int
	}{
		{
			name:     "rule_metric_hyphen",
			op:       "CreateRule",
			body:     map[string]any{"Name": "r", "MetricName": "my-metric"},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "MetricName",
		},
		{
			name:     "rule_metric_space",
			op:       "CreateRule",
			body:     map[string]any{"Name": "r", "MetricName": "my metric"},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "MetricName",
		},
		{
			name:     "rule_metric_reserved",
			op:       "CreateRule",
			body:     map[string]any{"Name": "r", "MetricName": "Default_Action"},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "MetricName",
		},
		{
			name:     "rule_metric_all",
			op:       "CreateRule",
			body:     map[string]any{"Name": "r", "MetricName": "All"},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "reserved",
		},
		{
			name:     "rule_name_long",
			op:       "CreateRule",
			body:     map[string]any{"Name": strings.Repeat("a", 129), "MetricName": "m"},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "Name",
		},
		{name: "rule_ok", op: "CreateRule", body: map[string]any{"Name": "r", "MetricName": "metric1"}, want: 200},
		{
			name:     "ipset_name_long",
			op:       "CreateIPSet",
			body:     map[string]any{"Name": strings.Repeat("a", 129)},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "Name",
		},
		{
			name: "webacl_metric_bad",
			op:   "CreateWebACL",
			body: map[string]any{
				"Name":          "a",
				"MetricName":    "a_b",
				"DefaultAction": map[string]string{"Type": "ALLOW"},
			},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "MetricName",
		},
		{
			name:     "list_bad_marker",
			op:       "ListRules",
			body:     map[string]any{"NextMarker": "garbage"},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "NextMarker",
		},
		{
			name:     "list_ipsets_bad_marker",
			op:       "ListIPSets",
			body:     map[string]any{"NextMarker": "garbage"},
			want:     400,
			wantCode: "WAFInvalidParameterException",
			wantMsg:  "NextMarker",
		},
		{
			name:     "get_missing_rule",
			op:       "GetRule",
			body:     map[string]any{"RuleId": "nope"},
			want:     400,
			wantCode: "WAFNonexistentItemException",
			wantMsg:  "does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			if _, ok := tt.body["ChangeToken"]; !ok && strings.HasPrefix(tt.op, "Create") {
				tt.body["ChangeToken"] = wafGetToken(t, h)
			}

			rec := wafDo(t, h, tt.op, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())

			if tt.want == http.StatusOK {
				return
			}

			var out struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantCode, out.Type)
			assert.Contains(t, out.Message, tt.wantMsg)
			assert.False(t, strings.HasPrefix(out.Message, tt.wantCode), "message repeats code: %q", out.Message)
		})
	}
}

func TestHandler_StaleTokenMessage(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)

	first := wafDo(t, h, "CreateIPSet", map[string]any{"Name": "a", "ChangeToken": token})
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())

	second := wafDo(t, h, "CreateIPSet", map[string]any{"Name": "b", "ChangeToken": token})
	require.Equal(t, http.StatusBadRequest, second.Code)

	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &out))
	assert.Equal(t, "WAFStaleDataException", out.Type)
	assert.Equal(t, "The input token is no longer current.", out.Message)
}
