package waf_test

// handler_audit2_test.go — WAF Classic AWS-accuracy audit batch-2 (go-9z5o).
//
// Covers:
//   - RateBasedRule lifecycle: Create, Get, Update (predicates + rateLimit), Delete, List, GetManagedKeys
//   - RegexPatternSet lifecycle: Create, Get, Update (INSERT/DELETE strings), Delete, List
//   - RegexMatchSet lifecycle: Create, Get, Update (INSERT/DELETE tuples), Delete, List
//   - RuleGroup lifecycle: Create, Get, Update (activated rules), Delete, List, ListActivatedRulesInRuleGroup
//   - ListSubscribedRuleGroups stub
//   - Logging: Put, Get, Delete, List
//   - PermissionPolicy: Put, Get, Delete
//   - CreateWebACLMigrationStack stub
//   - Error paths: not-found for all new resource types

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

// --- helpers for batch-2 ---

func wafCreateRateBasedRule(t *testing.T, h *waf.Handler, name string) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateRateBasedRule", map[string]any{
		"ChangeToken": token,
		"Name":        name,
		"MetricName":  name + "Metric",
		"RateKey":     "IP",
		"RateLimit":   int64(2000),
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ruleMap, ok := resp["Rule"].(map[string]any)
	require.True(t, ok)
	id, ok := ruleMap["RuleId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func wafCreateRegexPatternSet(t *testing.T, h *waf.Handler, name string) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateRegexPatternSet", map[string]any{
		"ChangeToken": token,
		"Name":        name,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	setMap, ok := resp["RegexPatternSet"].(map[string]any)
	require.True(t, ok)
	id, ok := setMap["RegexPatternSetId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func wafCreateRegexMatchSet(t *testing.T, h *waf.Handler, name string) string {
	t.Helper()

	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateRegexMatchSet", map[string]any{
		"ChangeToken": token,
		"Name":        name,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	setMap, ok := resp["RegexMatchSet"].(map[string]any)
	require.True(t, ok)
	id, ok := setMap["RegexMatchSetId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

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

// --- RateBasedRule ---

func TestRateBasedRuleLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"single rate-based rule CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			// Create
			ruleID := wafCreateRateBasedRule(t, h, "BlockBots")
			assert.Equal(t, 1, waf.RateBasedRuleCount(h.Backend.(*waf.InMemoryBackend)))

			// Get
			rec := wafDo(t, h, "GetRateBasedRule", map[string]any{"RuleId": ruleID})
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			ruleMap := getResp["Rule"].(map[string]any)
			assert.Equal(t, "BlockBots", ruleMap["Name"])
			assert.Equal(t, "IP", ruleMap["RateKey"])
			assert.InEpsilon(t, float64(2000), ruleMap["RateLimit"], 0.001)

			// Update: add predicate
			token := wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRateBasedRule", map[string]any{
				"ChangeToken": token,
				"RuleId":      ruleID,
				"RateLimit":   int64(3000),
				"Updates": []map[string]any{
					{
						"Action": "INSERT",
						"Predicate": map[string]any{
							"DataId":  "some-ip-set-id",
							"Type":    "IPMatch",
							"Negated": false,
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Get to verify updates
			rec = wafDo(t, h, "GetRateBasedRule", map[string]any{"RuleId": ruleID})
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			ruleMap = getResp["Rule"].(map[string]any)
			assert.InEpsilon(t, float64(3000), ruleMap["RateLimit"], 0.001)
			preds := ruleMap["MatchPredicates"].([]any)
			assert.Len(t, preds, 1)

			// Delete predicate
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRateBasedRule", map[string]any{
				"ChangeToken": token,
				"RuleId":      ruleID,
				"RateLimit":   int64(0),
				"Updates": []map[string]any{
					{
						"Action": "DELETE",
						"Predicate": map[string]any{
							"DataId":  "some-ip-set-id",
							"Type":    "IPMatch",
							"Negated": false,
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// List
			rec = wafDo(t, h, "ListRateBasedRules", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			rules := listResp["Rules"].([]any)
			assert.Len(t, rules, 1)

			// GetManagedKeys
			rec = wafDo(t, h, "GetRateBasedRuleManagedKeys", map[string]any{"RuleId": ruleID})
			require.Equal(t, http.StatusOK, rec.Code)
			var mkResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mkResp))
			assert.NotNil(t, mkResp["ManagedKeys"])

			// Delete
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "DeleteRateBasedRule", map[string]any{
				"ChangeToken": token,
				"RuleId":      ruleID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, 0, waf.RateBasedRuleCount(h.Backend.(*waf.InMemoryBackend)))
		})
	}
}

func TestRateBasedRuleNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{name: "GetRateBasedRule not-found", action: "GetRateBasedRule", body: map[string]any{"RuleId": "no-such-id"}},
		{
			name:   "GetRateBasedRuleManagedKeys not-found",
			action: "GetRateBasedRuleManagedKeys",
			body:   map[string]any{"RuleId": "no-such-id"},
		},
		{
			name:   "DeleteRateBasedRule not-found",
			action: "DeleteRateBasedRule",
			body:   map[string]any{"ChangeToken": "t", "RuleId": "no-such-id"},
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

// --- RegexPatternSet ---

func TestRegexPatternSetLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"single regex pattern set CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			// Create
			setID := wafCreateRegexPatternSet(t, h, "SqlPatterns")
			assert.Equal(t, 1, waf.RegexPatternSetCount(h.Backend.(*waf.InMemoryBackend)))

			// Get
			rec := wafDo(t, h, "GetRegexPatternSet", map[string]any{"RegexPatternSetId": setID})
			require.Equal(t, http.StatusOK, rec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			setMap := getResp["RegexPatternSet"].(map[string]any)
			assert.Equal(t, "SqlPatterns", setMap["Name"])
			assert.Empty(t, setMap["RegexPatternStrings"])

			// Update: insert patterns
			token := wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRegexPatternSet", map[string]any{
				"ChangeToken":       token,
				"RegexPatternSetId": setID,
				"Updates": []map[string]any{
					{"Action": "INSERT", "RegexPatternString": "(?i)select"},
					{"Action": "INSERT", "RegexPatternString": "(?i)union"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = wafDo(t, h, "GetRegexPatternSet", map[string]any{"RegexPatternSetId": setID})
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			setMap = getResp["RegexPatternSet"].(map[string]any)
			assert.Len(t, setMap["RegexPatternStrings"], 2)

			// Delete one pattern
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRegexPatternSet", map[string]any{
				"ChangeToken":       token,
				"RegexPatternSetId": setID,
				"Updates": []map[string]any{
					{"Action": "DELETE", "RegexPatternString": "(?i)select"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = wafDo(t, h, "GetRegexPatternSet", map[string]any{"RegexPatternSetId": setID})
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			setMap = getResp["RegexPatternSet"].(map[string]any)
			assert.Len(t, setMap["RegexPatternStrings"], 1)

			// List
			rec = wafDo(t, h, "ListRegexPatternSets", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			sets := listResp["RegexPatternSets"].([]any)
			assert.Len(t, sets, 1)

			// Delete set
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "DeleteRegexPatternSet", map[string]any{
				"ChangeToken":       token,
				"RegexPatternSetId": setID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, 0, waf.RegexPatternSetCount(h.Backend.(*waf.InMemoryBackend)))
		})
	}
}

func TestRegexPatternSetNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "GetRegexPatternSet not-found",
			action: "GetRegexPatternSet",
			body:   map[string]any{"RegexPatternSetId": "no-such-id"},
		},
		{
			name:   "DeleteRegexPatternSet not-found",
			action: "DeleteRegexPatternSet",
			body:   map[string]any{"ChangeToken": "t", "RegexPatternSetId": "no-such-id"},
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

// --- RegexMatchSet ---

func TestRegexMatchSetLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"single regex match set CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			patternSetID := wafCreateRegexPatternSet(t, h, "Patterns")

			// Create
			setID := wafCreateRegexMatchSet(t, h, "SqlMatchSet")
			assert.Equal(t, 1, waf.RegexMatchSetCount(h.Backend.(*waf.InMemoryBackend)))

			// Get
			rec := wafDo(t, h, "GetRegexMatchSet", map[string]any{"RegexMatchSetId": setID})
			require.Equal(t, http.StatusOK, rec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			setMap := getResp["RegexMatchSet"].(map[string]any)
			assert.Equal(t, "SqlMatchSet", setMap["Name"])

			// Update: insert tuple
			token := wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRegexMatchSet", map[string]any{
				"ChangeToken":     token,
				"RegexMatchSetId": setID,
				"Updates": []map[string]any{
					{
						"Action": "INSERT",
						"RegexMatchTuple": map[string]any{
							"FieldToMatch":       map[string]any{"Type": "QUERY_STRING"},
							"TextTransformation": "URL_DECODE",
							"RegexPatternSetId":  patternSetID,
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = wafDo(t, h, "GetRegexMatchSet", map[string]any{"RegexMatchSetId": setID})
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			setMap = getResp["RegexMatchSet"].(map[string]any)
			tuples := setMap["RegexMatchTuples"].([]any)
			assert.Len(t, tuples, 1)

			// Delete tuple
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "UpdateRegexMatchSet", map[string]any{
				"ChangeToken":     token,
				"RegexMatchSetId": setID,
				"Updates": []map[string]any{
					{
						"Action": "DELETE",
						"RegexMatchTuple": map[string]any{
							"FieldToMatch":       map[string]any{"Type": "QUERY_STRING"},
							"TextTransformation": "URL_DECODE",
							"RegexPatternSetId":  patternSetID,
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// List
			rec = wafDo(t, h, "ListRegexMatchSets", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			sets := listResp["RegexMatchSets"].([]any)
			assert.Len(t, sets, 1)

			// Delete set
			token = wafGetToken(t, h)
			rec = wafDo(t, h, "DeleteRegexMatchSet", map[string]any{
				"ChangeToken":     token,
				"RegexMatchSetId": setID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, 0, waf.RegexMatchSetCount(h.Backend.(*waf.InMemoryBackend)))
		})
	}
}

func TestRegexMatchSetNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "GetRegexMatchSet not-found",
			action: "GetRegexMatchSet",
			body:   map[string]any{"RegexMatchSetId": "no-such-id"},
		},
		{
			name:   "DeleteRegexMatchSet not-found",
			action: "DeleteRegexMatchSet",
			body:   map[string]any{"ChangeToken": "t", "RegexMatchSetId": "no-such-id"},
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

// --- RuleGroup ---

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

// --- Logging ---

func TestLoggingConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"logging config CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			aclID := wafCreateWebACL(t, h, "LoggedACL")
			aclARN := "arn:aws:waf::123456789012:webacl/" + aclID

			// Put
			firehoseARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/waf-logs"
			rec := wafDo(t, h, "PutLoggingConfiguration", map[string]any{
				"LoggingConfiguration": map[string]any{
					"ResourceArn":           aclARN,
					"LogDestinationConfigs": []string{firehoseARN},
					"RedactedFields": []map[string]any{
						{"Type": "HEADER", "Data": "cookie"},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var putResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			assert.NotNil(t, putResp["LoggingConfiguration"])

			// Get
			rec = wafDo(t, h, "GetLoggingConfiguration", map[string]any{"ResourceArn": aclARN})
			require.Equal(t, http.StatusOK, rec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			cfgMap := getResp["LoggingConfiguration"].(map[string]any)
			assert.Equal(t, aclARN, cfgMap["ResourceArn"])

			// List
			rec = wafDo(t, h, "ListLoggingConfigurations", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			cfgs := listResp["LoggingConfigurations"].([]any)
			assert.Len(t, cfgs, 1)

			// Delete
			rec = wafDo(t, h, "DeleteLoggingConfiguration", map[string]any{"ResourceArn": aclARN})
			require.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → not found
			rec = wafDo(t, h, "GetLoggingConfiguration", map[string]any{"ResourceArn": aclARN})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			// List after delete → empty
			rec = wafDo(t, h, "ListLoggingConfigurations", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			cfgs = listResp["LoggingConfigurations"].([]any)
			assert.Empty(t, cfgs)
		})
	}
}

func TestLoggingConfigurationNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "GetLoggingConfiguration not-found",
			action: "GetLoggingConfiguration",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:webacl/no-such"},
		},
		{
			name:   "DeleteLoggingConfiguration not-found",
			action: "DeleteLoggingConfiguration",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:webacl/no-such"},
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

// --- PermissionPolicy ---

func TestPermissionPolicyLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"permission policy CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			rgID := wafCreateRuleGroup(t, h, "SharedGroup")
			rgARN := "arn:aws:waf::123456789012:rulegroup/" + rgID
			policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"*"},"Action":"waf:GetRuleGroup"}]}`

			// Put
			rec := wafDo(t, h, "PutPermissionPolicy", map[string]any{
				"ResourceArn": rgARN,
				"Policy":      policy,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Get
			rec = wafDo(t, h, "GetPermissionPolicy", map[string]any{"ResourceArn": rgARN})
			require.Equal(t, http.StatusOK, rec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, policy, getResp["Policy"])

			// Delete
			rec = wafDo(t, h, "DeletePermissionPolicy", map[string]any{"ResourceArn": rgARN})
			require.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → not found
			rec = wafDo(t, h, "GetPermissionPolicy", map[string]any{"ResourceArn": rgARN})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestPermissionPolicyNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "GetPermissionPolicy not-found",
			action: "GetPermissionPolicy",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:rulegroup/no-such"},
		},
		{
			name:   "DeletePermissionPolicy not-found",
			action: "DeletePermissionPolicy",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:rulegroup/no-such"},
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

// --- CreateWebACLMigrationStack ---

func TestCreateWebACLMigrationStack(t *testing.T) {
	t.Parallel()

	t.Run("returns S3 URL", func(t *testing.T) {
		t.Parallel()

		h := newWAFHandler(t)
		aclID := wafCreateWebACL(t, h, "MigrateACL")

		rec := wafDo(t, h, "CreateWebACLMigrationStack", map[string]any{
			"WebACLId":              aclID,
			"S3BucketName":          "my-migration-bucket",
			"IgnoreUnsupportedType": true,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		s3URL, ok := resp["S3ObjectUrl"].(string)
		require.True(t, ok)
		assert.Contains(t, s3URL, "my-migration-bucket")
		assert.Contains(t, s3URL, aclID)
	})
}

// --- Snapshot/Restore batch-2 ---

func TestSnapshotRestoreBatch2(t *testing.T) {
	t.Parallel()

	t.Run("snapshot and restore preserves batch-2 state", func(t *testing.T) {
		t.Parallel()

		h := newWAFHandler(t)

		// Create one of each batch-2 resource type
		wafCreateRateBasedRule(t, h, "SnapshotRule")
		wafCreateRegexPatternSet(t, h, "SnapshotPatterns")
		wafCreateRegexMatchSet(t, h, "SnapshotMatch")
		rgID := wafCreateRuleGroup(t, h, "SnapshotGroup")
		rgARN := "arn:aws:waf::123456789012:rulegroup/" + rgID
		aclID := wafCreateWebACL(t, h, "SnapshotACL")
		aclARN := "arn:aws:waf::123456789012:webacl/" + aclID

		rec := wafDo(t, h, "PutLoggingConfiguration", map[string]any{
			"LoggingConfiguration": map[string]any{
				"ResourceArn":           aclARN,
				"LogDestinationConfigs": []string{"arn:aws:firehose:us-east-1:123:deliverystream/logs"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		rec = wafDo(t, h, "PutPermissionPolicy", map[string]any{
			"ResourceArn": rgARN,
			"Policy":      `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		// Snapshot
		snap := h.Snapshot(t.Context())

		// Restore into new handler
		h2 := newWAFHandler(t)
		require.NoError(t, h2.Restore(t.Context(), snap))

		b2 := h2.Backend.(*waf.InMemoryBackend)
		assert.Equal(t, 1, waf.RateBasedRuleCount(b2))
		assert.Equal(t, 1, waf.RegexPatternSetCount(b2))
		assert.Equal(t, 1, waf.RegexMatchSetCount(b2))
		assert.Equal(t, 1, waf.RuleGroupCount(b2))

		// Verify logging config restored
		rec = wafDo(t, h2, "GetLoggingConfiguration", map[string]any{"ResourceArn": aclARN})
		require.Equal(t, http.StatusOK, rec.Code)

		// Verify permission policy restored
		rec = wafDo(t, h2, "GetPermissionPolicy", map[string]any{"ResourceArn": rgARN})
		require.Equal(t, http.StatusOK, rec.Code)
	})
}
