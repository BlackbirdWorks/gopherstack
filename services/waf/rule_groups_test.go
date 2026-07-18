package waf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

func wafCreateRuleGroup(t *testing.T, h *waf.Handler, name string) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateRuleGroup", map[string]any{
		"ChangeToken": token,
		"Name":        name,
		"MetricName":  name + "Metric",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rgMap, ok := resp["RuleGroup"].(map[string]any)
	require.True(t, ok)
	id, ok := rgMap["RuleGroupId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func TestRuleGroupLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"single rule group CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			// Prerequisite: create a regular rule to activate
			ruleID := wafCreateRule(t, h, "TestRule")

			// Create rule group
			rgID := wafCreateRuleGroup(t, h, "MyGroup")
			assert.Equal(t, 1, waf.RuleGroupCount(h.Backend.(*waf.InMemoryBackend)))

			// Get
			rec := wafDo(t, h, "GetRuleGroup", map[string]any{"RuleGroupId": rgID})
			require.Equal(t, http.StatusOK, rec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			rgMap := getResp["RuleGroup"].(map[string]any)
			assert.Equal(t, "MyGroup", rgMap["Name"])

			// Update: insert activated rule
			token := wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRuleGroup", map[string]any{
				"ChangeToken": token,
				"RuleGroupId": rgID,
				"Updates": []map[string]any{
					{
						"Action": "INSERT",
						"ActivatedRule": map[string]any{
							"RuleId":   ruleID,
							"Priority": 1,
							"Type":     "REGULAR",
							"Action":   map[string]any{"Type": "BLOCK"},
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// ListActivatedRulesInRuleGroup
			rec = wafDo(t, h, "ListActivatedRulesInRuleGroup", map[string]any{
				"RuleGroupId": rgID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var listARResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listARResp))
			activated := listARResp["ActivatedRules"].([]any)
			assert.Len(t, activated, 1)

			// Delete activated rule
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRuleGroup", map[string]any{
				"ChangeToken": token,
				"RuleGroupId": rgID,
				"Updates": []map[string]any{
					{
						"Action": "DELETE",
						"ActivatedRule": map[string]any{
							"RuleId":   ruleID,
							"Priority": 1,
							"Type":     "REGULAR",
							"Action":   map[string]any{"Type": "BLOCK"},
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// List rule groups
			rec = wafDo(t, h, "ListRuleGroups", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listRGResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listRGResp))
			groups := listRGResp["RuleGroups"].([]any)
			assert.Len(t, groups, 1)

			// Delete rule group
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "DeleteRuleGroup", map[string]any{
				"ChangeToken": token,
				"RuleGroupId": rgID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, 0, waf.RuleGroupCount(h.Backend.(*waf.InMemoryBackend)))
		})
	}
}

func TestRuleGroupNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{name: "GetRuleGroup not-found", action: "GetRuleGroup", body: map[string]any{"RuleGroupId": "no-such-id"}},
		{
			name:   "DeleteRuleGroup not-found",
			action: "DeleteRuleGroup",
			body:   map[string]any{"ChangeToken": "t", "RuleGroupId": "no-such-id"},
		},
		{
			name:   "ListActivatedRulesInRuleGroup not-found",
			action: "ListActivatedRulesInRuleGroup",
			body:   map[string]any{"RuleGroupId": "no-such-id"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			rec := wafDo(t, h, tc.action, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestListSubscribedRuleGroups(t *testing.T) {
	t.Parallel()

	t.Run("returns empty list", func(t *testing.T) {
		t.Parallel()

		h := newWAFHandler(t)
		rec := wafDo(t, h, "ListSubscribedRuleGroups", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		groups := resp["RuleGroups"].([]any)
		assert.Empty(t, groups)
	})
}
