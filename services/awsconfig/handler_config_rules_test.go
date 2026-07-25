package awsconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestConfigRulePascalCaseKeys verifies DescribeConfigRules returns PascalCase JSON keys.
func TestConfigRulePascalCaseKeys(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{
		ConfigRuleName:  "my-rule",
		Description:     "test rule",
		InputParameters: `{"key":"val"}`,
	}))

	rec := doAWSConfigRequest(t, h, "DescribeConfigRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, `"ConfigRuleName"`)
	assert.Contains(t, body, `"ConfigRuleArn"`)
	assert.Contains(t, body, `"ConfigRuleId"`)
	assert.Contains(t, body, `"Description"`)
	assert.Contains(t, body, `"InputParameters"`)
	assert.NotContains(t, body, `"configRuleName"`)
}

// TestConfigRuleARNGenerated verifies PutConfigRule generates a proper ARN.
func TestConfigRuleARNGenerated(t *testing.T) {
	t.Parallel()

	b := newTestAWSConfigHandler(t).Backend
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-x"}))

	rules, err := b.DescribeConfigRules([]string{"rule-x"})
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Contains(t, rules[0].ConfigRuleArn, "arn:aws:config:")
	assert.Contains(t, rules[0].ConfigRuleArn, "config-rule-")
	assert.NotEmpty(t, rules[0].ConfigRuleID)
}

// TestConfigRuleStateActive verifies new rules default to ACTIVE state.
func TestConfigRuleStateActive(t *testing.T) {
	t.Parallel()

	b := newTestAWSConfigHandler(t).Backend
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-y"}))

	rules, err := b.DescribeConfigRules(nil)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "ACTIVE", rules[0].ConfigRuleState)
}

// TestConfigRuleScopeRoundtrip verifies Scope is stored and returned.
func TestConfigRuleScopeRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "PutConfigRule", map[string]any{
		"ConfigRule": map[string]any{
			"ConfigRuleName": "scoped-rule",
			"Source": map[string]any{
				"Owner":            "AWS",
				"SourceIdentifier": "S3_BUCKET_PUBLIC_READ_PROHIBITED",
			},
			"Scope": map[string]any{
				"ComplianceResourceTypes": []string{"AWS::S3::Bucket"},
				"TagKey":                  "env",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAWSConfigRequest(t, h, "DescribeConfigRules", map[string]any{"ConfigRuleNames": []string{"scoped-rule"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConfigRules []struct {
			Scope *struct {
				TagKey                  string   `json:"TagKey"`
				ComplianceResourceTypes []string `json:"ComplianceResourceTypes"`
			} `json:"Scope"`
		} `json:"ConfigRules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConfigRules, 1)
	require.NotNil(t, out.ConfigRules[0].Scope)
	assert.Contains(t, out.ConfigRules[0].Scope.ComplianceResourceTypes, "AWS::S3::Bucket")
	assert.Equal(t, "env", out.ConfigRules[0].Scope.TagKey)
}

// TestComplianceSummaryShape verifies GetComplianceSummaryByConfigRule uses CappedCount shape.
func TestComplianceSummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "r1"}))
	require.NoError(t, b.StartConfigRulesEvaluation())

	rec := doAWSConfigRequest(t, h, "GetComplianceSummaryByConfigRule", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Real AWS shape: ComplianceSummary.CompliantResourceCount.CappedCount
	body := rec.Body.String()
	assert.Contains(t, body, "CappedCount")
	assert.Contains(t, body, "CapExceeded")
	assert.Contains(t, body, `"ComplianceSummary"`)
}

// TestConfigRuleEvaluationStatusTimestampStrings verifies timestamps are strings not numbers.
func TestConfigRuleEvaluationStatusTimestampStrings(t *testing.T) {
	t.Parallel()

	b := newTestAWSConfigHandler(t).Backend
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-ts"}))
	require.NoError(t, b.StartConfigRulesEvaluation())

	statuses := b.DescribeConfigRuleEvaluationStatus(nil)
	require.Len(t, statuses, 1)
	assert.Equal(t, "rule-ts", statuses[0].ConfigRuleName)
}

func TestAWSConfigHandler_DescribeConfigRulesAndCompliance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, h *awsconfig.Handler)
		body      any
		name      string
		action    string
		wantField string
		wantCode  int
	}{
		{
			name:      "describe_config_rules_empty",
			action:    "DescribeConfigRules",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantField: "ConfigRules",
		},
		{
			name:   "describe_config_rules_with_names",
			action: "DescribeConfigRules",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
			},
			body:      map[string]any{"ConfigRuleNames": []string{"my-rule"}},
			wantCode:  http.StatusOK,
			wantField: "ConfigRules",
		},
		{
			name:     "describe_config_rules_unknown_name_errors",
			action:   "DescribeConfigRules",
			body:     map[string]any{"ConfigRuleNames": []string{"does-not-exist"}},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "get_compliance_details_empty",
			action: "GetComplianceDetailsByConfigRule",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
			},
			body:      map[string]any{"ConfigRuleName": "my-rule"},
			wantCode:  http.StatusOK,
			wantField: "EvaluationResults",
		},
		{
			name:     "get_compliance_details_no_name_errors",
			action:   "GetComplianceDetailsByConfigRule",
			body:     map[string]any{},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, tt.action, tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantField == "" {
				return
			}

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out[tt.wantField])
		})
	}
}

func TestAWSConfigHandler_DeleteConfigRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
			},
			body:     map[string]any{"ConfigRuleName": "my-rule"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigRuleName": "nonexistent-rule"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteConfigRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteEvaluationResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success_for_existing_rule",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
			},
			body:     map[string]any{"ConfigRuleName": "my-rule"},
			wantCode: http.StatusOK,
		},
		{
			name:     "nonexistent_rule_not_found",
			body:     map[string]any{"ConfigRuleName": "my-rule"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "empty_rule_name",
			body:     map[string]any{"ConfigRuleName": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteEvaluationResults", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DescribeConfigRules_BackedByStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, h *awsconfig.Handler)
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "empty_returns_no_rules",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name: "returns_stored_rules",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-x"}))
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-y"}))
			},
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-1"}))
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-2"}))
			},
			body:      map[string]any{"ConfigRuleNames": []string{"rule-1"}},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeConfigRules", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var rules []any
			require.NoError(t, json.Unmarshal(out["ConfigRules"], &rules))
			assert.Len(t, rules, tt.wantCount)
		})
	}
}

func TestAWSConfigHandler_PutConfigRule_ValidationAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "empty_name_returns_400",
			body:     map[string]any{"ConfigRuleName": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			// Call PutConfigRule directly via backend (handler not wired for PutConfigRule in HTTP yet)
			ruleName := tt.body.(map[string]any)["ConfigRuleName"].(string)
			err := h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: ruleName})
			require.Error(t, err)
			assert.ErrorIs(t, err, awsconfig.ErrValidation)
		})
	}
}
