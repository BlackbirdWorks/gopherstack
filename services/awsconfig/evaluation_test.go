package awsconfig_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// complianceByResource collapses detailed results into a resourceID→compliance map.
func complianceByResource(results []awsconfig.DetailedEvaluationResult) map[string]string {
	m := make(map[string]string, len(results))
	for _, r := range results {
		m[r.EvaluationResultIdentifier.EvaluationResultQualifier.ResourceID] = r.ComplianceType
	}

	return m
}

// awsRule builds a managed AWS config rule with the given source identifier.
func awsRule(name, sourceID, inputParams string) *awsconfig.ConfigRule {
	return &awsconfig.ConfigRule{
		ConfigRuleName:  name,
		InputParameters: inputParams,
		Source:          &awsconfig.ConfigRuleSource{Owner: "AWS", SourceIdentifier: sourceID},
	}
}

func TestStartConfigRulesEvaluation_ManagedRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rule       *awsconfig.ConfigRule
		resources  map[string]map[string]string // resourceType → resourceID → configuration JSON
		wantPer    map[string]string            // resourceID → compliance
		wantRollup string
	}{
		{
			name: "required_tags",
			rule: awsRule("req-tags", "REQUIRED_TAGS", `{"tag1Key":"env"}`),
			resources: map[string]map[string]string{
				"AWS::S3::Bucket": {
					"tagged":   `{"Tags":[{"Key":"env","Value":"prod"}]}`,
					"untagged": `{"Tags":[{"Key":"team","Value":"x"}]}`,
				},
			},
			wantPer:    map[string]string{"tagged": "COMPLIANT", "untagged": "NON_COMPLIANT"},
			wantRollup: "NON_COMPLIANT",
		},
		{
			name: "required_tags_value_mismatch",
			rule: awsRule("req-tags-val", "REQUIRED_TAGS", `{"tag1Key":"env","tag1Value":"prod,staging"}`),
			resources: map[string]map[string]string{
				"AWS::S3::Bucket": {
					"ok":  `{"Tags":[{"Key":"env","Value":"staging"}]}`,
					"bad": `{"Tags":[{"Key":"env","Value":"dev"}]}`,
				},
			},
			wantPer:    map[string]string{"ok": "COMPLIANT", "bad": "NON_COMPLIANT"},
			wantRollup: "NON_COMPLIANT",
		},
		{
			name: "s3_versioning",
			rule: awsRule("ver", "S3_BUCKET_VERSIONING_ENABLED", ""),
			resources: map[string]map[string]string{
				"AWS::S3::Bucket": {
					"on":  `{"VersioningConfiguration":{"Status":"Enabled"}}`,
					"off": `{"VersioningConfiguration":{"Status":"Suspended"}}`,
				},
			},
			wantPer:    map[string]string{"on": "COMPLIANT", "off": "NON_COMPLIANT"},
			wantRollup: "NON_COMPLIANT",
		},
		{
			name: "s3_public_read",
			rule: awsRule("pub", "S3_BUCKET_PUBLIC_READ_PROHIBITED", ""),
			resources: map[string]map[string]string{
				"AWS::S3::Bucket": {
					"private": `{"Acl":"private"}`,
					"public":  `{"Acl":"public-read"}`,
				},
			},
			wantPer:    map[string]string{"private": "COMPLIANT", "public": "NON_COMPLIANT"},
			wantRollup: "NON_COMPLIANT",
		},
		{
			name: "s3_encryption_all_compliant",
			rule: awsRule("enc", "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED", ""),
			resources: map[string]map[string]string{
				"AWS::S3::Bucket": {
					"e1": `{"ServerSideEncryptionConfiguration":{"Rules":[{"x":1}]}}`,
					"e2": `{"Encryption":true}`,
				},
			},
			wantPer:    map[string]string{"e1": "COMPLIANT", "e2": "COMPLIANT"},
			wantRollup: "COMPLIANT",
		},
		{
			name: "encrypted_volumes",
			rule: awsRule("vol", "ENCRYPTED_VOLUMES", ""),
			resources: map[string]map[string]string{
				"AWS::EC2::Volume": {
					"enc":   `{"Encrypted":true}`,
					"plain": `{"Encrypted":false}`,
				},
			},
			wantPer:    map[string]string{"enc": "COMPLIANT", "plain": "NON_COMPLIANT"},
			wantRollup: "NON_COMPLIANT",
		},
		{
			name: "ec2_no_public_ip",
			rule: awsRule("ip", "EC2_INSTANCE_NO_PUBLIC_IP", ""),
			resources: map[string]map[string]string{
				"AWS::EC2::Instance": {
					"private": `{}`,
					"public":  `{"PublicIpAddress":"203.0.113.7"}`,
				},
			},
			wantPer:    map[string]string{"private": "COMPLIANT", "public": "NON_COMPLIANT"},
			wantRollup: "NON_COMPLIANT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			require.NoError(t, b.PutConfigRule(tt.rule))

			for resourceType, byID := range tt.resources {
				for id, cfg := range byID {
					require.NoError(t, b.PutResourceConfig(resourceType, id, cfg))
				}
			}

			require.NoError(t, b.StartConfigRulesEvaluation())

			got := complianceByResource(b.GetComplianceDetailsByConfigRule(tt.rule.ConfigRuleName, nil))
			assert.Equal(t, tt.wantPer, got)
			assert.Equal(t, tt.wantRollup, b.GetConfigRuleComplianceType(tt.rule.ConfigRuleName))
		})
	}
}

func TestStartConfigRulesEvaluation_RuleOwnership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, b *awsconfig.InMemoryBackend)
		ruleName   string
		wantRollup string
		wantDetail int
	}{
		{
			name: "unmodelable_managed_is_insufficient_data",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(awsRule("mystery", "SOME_FUTURE_RULE", "")))
				require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "b1", `{"Acl":"public-read"}`))
			},
			ruleName:   "mystery",
			wantRollup: "INSUFFICIENT_DATA",
			wantDetail: 0,
		},
		{
			name: "no_source_is_insufficient_data",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "bare"}))
			},
			ruleName:   "bare",
			wantRollup: "INSUFFICIENT_DATA",
			wantDetail: 0,
		},
		{
			name: "custom_lambda_keeps_reported_evaluations",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{
					ConfigRuleName: "custom",
					Source:         &awsconfig.ConfigRuleSource{Owner: "CUSTOM_LAMBDA"},
				}))
				require.NoError(t, b.PutEvaluations([]awsconfig.EvaluationResult{
					{
						ConfigRuleName: "custom",
						ResourceType:   "AWS::S3::Bucket",
						ResourceID:     "r1",
						ComplianceType: "NON_COMPLIANT",
					},
				}))
			},
			ruleName:   "custom",
			wantRollup: "NON_COMPLIANT",
			wantDetail: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			tt.setup(t, b)

			require.NoError(t, b.StartConfigRulesEvaluation())

			assert.Equal(t, tt.wantRollup, b.GetConfigRuleComplianceType(tt.ruleName))
			assert.Len(t, b.GetComplianceDetailsByConfigRule(tt.ruleName, nil), tt.wantDetail)
		})
	}
}

func TestStartConfigRulesEvaluation_ScopeFiltering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scope   *awsconfig.ConfigRuleScope
		wantIDs []string
	}{
		{
			name:    "no_scope_evaluates_all",
			scope:   nil,
			wantIDs: []string{"keep", "skip"},
		},
		{
			name:    "resource_id_scope",
			scope:   &awsconfig.ConfigRuleScope{ComplianceResourceID: "keep"},
			wantIDs: []string{"keep"},
		},
		{
			name:    "tag_key_scope",
			scope:   &awsconfig.ConfigRuleScope{TagKey: "keepme"},
			wantIDs: []string{"keep"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			rule := awsRule("scoped", "S3_BUCKET_VERSIONING_ENABLED", "")
			rule.Scope = tt.scope
			require.NoError(t, b.PutConfigRule(rule))
			require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "keep",
				`{"VersioningConfiguration":{"Status":"Enabled"},"Tags":[{"Key":"keepme","Value":"1"}]}`))
			require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "skip",
				`{"VersioningConfiguration":{"Status":"Enabled"}}`))

			require.NoError(t, b.StartConfigRulesEvaluation())

			got := complianceByResource(b.GetComplianceDetailsByConfigRule("scoped", nil))
			ids := make([]string, 0, len(got))
			for id := range got {
				ids = append(ids, id)
			}

			assert.ElementsMatch(t, tt.wantIDs, ids)
		})
	}
}

func TestPutEvaluations_PerResourceGranularity(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "rule", ResourceType: "AWS::S3::Bucket", ResourceID: "good", ComplianceType: "COMPLIANT"},
		{ConfigRuleName: "rule", ResourceType: "AWS::S3::Bucket", ResourceID: "bad", ComplianceType: "NON_COMPLIANT"},
	}))

	all := complianceByResource(b.GetComplianceDetailsByConfigRule("rule", nil))
	assert.Equal(t, map[string]string{"good": "COMPLIANT", "bad": "NON_COMPLIANT"}, all)
	assert.Equal(t, "NON_COMPLIANT", b.GetConfigRuleComplianceType("rule"))

	// Compliance-type filter returns only the matching subset.
	filtered := b.GetComplianceDetailsByConfigRule("rule", []string{"NON_COMPLIANT"})
	require.Len(t, filtered, 1)
	assert.Equal(t, "bad", filtered[0].EvaluationResultIdentifier.EvaluationResultQualifier.ResourceID)

	// Per-resource lookup across rules.
	byResource := b.GetComplianceDetailsByResource("AWS::S3::Bucket", "bad", nil)
	require.Len(t, byResource, 1)
	assert.Equal(t, "NON_COMPLIANT", byResource[0].ComplianceType)
	assert.Equal(t, "rule", byResource[0].EvaluationResultIdentifier.EvaluationResultQualifier.ConfigRuleName)

	// Re-evaluating the same resource overwrites rather than duplicating.
	require.NoError(t, b.PutExternalEvaluation(awsconfig.EvaluationResult{
		ConfigRuleName: "rule", ResourceType: "AWS::S3::Bucket", ResourceID: "bad", ComplianceType: "COMPLIANT",
	}))
	assert.Equal(t, "COMPLIANT", b.GetConfigRuleComplianceType("rule"))
	assert.Len(t, b.GetComplianceDetailsByConfigRule("rule", nil), 2)
}

func TestGetResourceConfigHistory_RealHistory(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	const rt, id = "AWS::S3::Bucket", "my-bucket"

	require.NoError(t, b.PutResourceConfig(rt, id, `{"v":1}`))
	require.NoError(t, b.PutResourceConfig(rt, id, `{"v":1}`)) // no change → no new entry
	require.NoError(t, b.PutResourceConfig(rt, id, `{"v":2}`))
	require.NoError(t, b.PutResourceConfig(rt, id, `{"v":3}`))

	hist := b.GetResourceConfigHistory(rt, id)
	require.Len(t, hist, 3)
	// Most-recent first.
	assert.Equal(t, `{"v":3}`, hist[0].Configuration)
	assert.Equal(t, `{"v":2}`, hist[1].Configuration)
	assert.Equal(t, `{"v":1}`, hist[2].Configuration)
	assert.NotZero(t, hist[0].ConfigurationItemCaptureTime)

	// Pagination: one item at a time.
	first, token := b.GetResourceConfigHistoryPage(rt, id, 1, "")
	require.Len(t, first, 1)
	require.NotEmpty(t, token)
	assert.Equal(t, `{"v":3}`, first[0].Configuration)

	second, token2 := b.GetResourceConfigHistoryPage(rt, id, 1, token)
	require.Len(t, second, 1)
	require.NotEmpty(t, token2)
	assert.Equal(t, `{"v":2}`, second[0].Configuration)

	third, token3 := b.GetResourceConfigHistoryPage(rt, id, 1, token2)
	require.Len(t, third, 1)
	assert.Empty(t, token3)
	assert.Equal(t, `{"v":1}`, third[0].Configuration)
}

func TestStartResourceEvaluation_RealIDAndReadback(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	id1 := b.StartResourceEvaluation("AWS::S3::Bucket", "b1", "DETAILED", `{"x":1}`)
	id2 := b.StartResourceEvaluation("AWS::EC2::Instance", "i1", "", "")

	assert.NotEqual(t, "eval-stub", id1)
	assert.NotEqual(t, id1, id2)
	assert.NotEmpty(t, id1)

	summary := b.GetResourceEvaluationSummaryByID(id1)
	require.NotNil(t, summary)
	assert.Equal(t, "AWS::S3::Bucket", summary.ResourceType)
	assert.Equal(t, "b1", summary.ResourceID)
	assert.Equal(t, "DETAILED", summary.EvaluationMode)
	assert.Equal(t, "SUCCEEDED", summary.Status)

	// Default evaluation mode is applied.
	summary2 := b.GetResourceEvaluationSummaryByID(id2)
	require.NotNil(t, summary2)
	assert.Equal(t, "DETAILED", summary2.EvaluationMode)

	assert.Nil(t, b.GetResourceEvaluationSummaryByID("nonexistent"))
	assert.Len(t, b.ListResourceEvaluationSummaries(), 2)
}

// --- Handler-level tests ---

func TestHandler_DescribeConfigRulesPagination(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend

	const total = 150
	for i := range total {
		require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: fmt.Sprintf("rule-%04d", i)}))
	}

	rec := doAWSConfigRequest(t, h, "DescribeConfigRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken   string                 `json:"NextToken"`
		ConfigRules []awsconfig.ConfigRule `json:"ConfigRules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.Len(t, page1.ConfigRules, 100)
	require.NotEmpty(t, page1.NextToken)

	rec = doAWSConfigRequest(t, h, "DescribeConfigRules", map[string]any{"NextToken": page1.NextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 struct {
		NextToken   string                 `json:"NextToken"`
		ConfigRules []awsconfig.ConfigRule `json:"ConfigRules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	require.Len(t, page2.ConfigRules, 50)
	assert.Empty(t, page2.NextToken)

	// The two pages together are contiguous and non-overlapping.
	assert.Equal(t, "rule-0099", page1.ConfigRules[99].ConfigRuleName)
	assert.Equal(t, "rule-0100", page2.ConfigRules[0].ConfigRuleName)
}

func TestHandler_DescribeConfigRules_InvalidToken(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "DescribeConfigRules", map[string]any{"NextToken": "!!!not-base64!!!"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetComplianceDetailsByConfigRule_RealResults(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend
	require.NoError(t, b.PutConfigRule(awsRule("ver", "S3_BUCKET_VERSIONING_ENABLED", "")))
	require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "on", `{"VersioningConfiguration":{"Status":"Enabled"}}`))
	require.NoError(
		t,
		b.PutResourceConfig("AWS::S3::Bucket", "off", `{"VersioningConfiguration":{"Status":"Suspended"}}`),
	)
	require.NoError(t, b.StartConfigRulesEvaluation())

	rec := doAWSConfigRequest(t, h, "GetComplianceDetailsByConfigRule", map[string]any{"ConfigRuleName": "ver"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		EvaluationResults []awsconfig.DetailedEvaluationResult `json:"EvaluationResults"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.EvaluationResults, 2)

	got := complianceByResource(out.EvaluationResults)
	assert.Equal(t, map[string]string{"on": "COMPLIANT", "off": "NON_COMPLIANT"}, got)
	// Timestamps must be epoch numbers, not strings.
	assert.NotZero(t, out.EvaluationResults[0].ResultRecordedTime)
	assert.Contains(t, rec.Body.String(), `"ResultRecordedTime"`)
	assert.NotContains(t, rec.Body.String(), "T00:00:00")
}

func TestHandler_StartResourceEvaluationAndSummary(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	rec := doAWSConfigRequest(t, h, "StartResourceEvaluation", map[string]any{
		"EvaluationMode": "DETAILED",
		"ResourceDetails": map[string]any{
			"ResourceId":            "b1",
			"ResourceType":          "AWS::S3::Bucket",
			"ResourceConfiguration": `{"x":1}`,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var started struct {
		ResourceEvaluationID string `json:"ResourceEvaluationId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &started))
	require.NotEmpty(t, started.ResourceEvaluationID)
	assert.NotEqual(t, "eval-stub", started.ResourceEvaluationID)

	rec = doAWSConfigRequest(t, h, "GetResourceEvaluationSummary", map[string]any{
		"ResourceEvaluationId": started.ResourceEvaluationID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS::S3::Bucket")
	assert.Contains(t, rec.Body.String(), "SUCCEEDED")

	// Unknown id → ResourceNotFoundException.
	rec = doAWSConfigRequest(t, h, "GetResourceEvaluationSummary", map[string]any{
		"ResourceEvaluationId": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
}

func TestHandler_GetResourceConfigHistoryPagination(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend
	const rt, id = "AWS::S3::Bucket", "bkt"
	require.NoError(t, b.PutResourceConfig(rt, id, `{"v":1}`))
	require.NoError(t, b.PutResourceConfig(rt, id, `{"v":2}`))

	rec := doAWSConfigRequest(t, h, "GetResourceConfigHistory", map[string]any{
		"resourceType": rt,
		"resourceId":   id,
		"limit":        1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken          string                         `json:"nextToken"`
		ConfigurationItems []awsconfig.ResourceConfigItem `json:"configurationItems"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConfigurationItems, 1)
	assert.Equal(t, `{"v":2}`, out.ConfigurationItems[0].Configuration)
	require.NotEmpty(t, out.NextToken)

	rec = doAWSConfigRequest(t, h, "GetResourceConfigHistory", map[string]any{
		"resourceType": rt,
		"resourceId":   id,
		"limit":        1,
		"nextToken":    out.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out2 struct {
		NextToken          string                         `json:"nextToken"`
		ConfigurationItems []awsconfig.ResourceConfigItem `json:"configurationItems"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out2))
	require.Len(t, out2.ConfigurationItems, 1)
	assert.Equal(t, `{"v":1}`, out2.ConfigurationItems[0].Configuration)
	assert.Empty(t, out2.NextToken)
}

func TestHandler_PutEvaluationsAWSKeys(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend

	rec := doAWSConfigRequest(t, h, "PutEvaluations", map[string]any{
		"ConfigRuleName": "rule",
		"ResultToken":    "tok",
		"Evaluations": []map[string]any{
			{
				"ComplianceResourceType": "AWS::S3::Bucket",
				"ComplianceResourceId":   "b1",
				"ComplianceType":         "NON_COMPLIANT",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	details := b.GetComplianceDetailsByConfigRule("rule", nil)
	require.Len(t, details, 1)
	assert.Equal(t, "b1", details[0].EvaluationResultIdentifier.EvaluationResultQualifier.ResourceID)
	assert.Equal(t, "AWS::S3::Bucket", details[0].EvaluationResultIdentifier.EvaluationResultQualifier.ResourceType)
	assert.Equal(t, "NON_COMPLIANT", details[0].ComplianceType)
}
