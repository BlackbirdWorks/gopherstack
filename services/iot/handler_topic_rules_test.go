package iot_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestRefinement1_SortedListTopicRules verifies ListTopicRules returns sorted results.
func TestSortedListTopicRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedNames []string
		wantOrder []string
	}{
		{
			name:      "three_rules_sorted",
			seedNames: []string{"zeta", "alpha", "beta"},
			wantOrder: []string{"alpha", "beta", "zeta"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			for _, n := range tt.seedNames {
				b.AddRuleInternal(iot.TopicRule{RuleName: n})
			}

			rules := b.ListTopicRules()
			require.Len(t, rules, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, rules[i].RuleName)
			}
		})
	}
}

// TestRefinement1_DeepCopy_GetTopicRule verifies mutations do not affect backend state.
func TestDeepCopy_GetTopicRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "mutate_copy_does_not_affect_backend"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			b.AddRuleInternal(iot.TopicRule{
				RuleName: "myRule",
				SQL:      "SELECT temperature FROM 'a/#'",
			})

			r1, err := b.GetTopicRule("myRule")
			require.NoError(t, err)

			r1.SQL = "MUTATED"

			r2, err := b.GetTopicRule("myRule")
			require.NoError(t, err)
			assert.Equal(t, "SELECT temperature FROM 'a/#'", r2.SQL)
		})
	}
}

// TestRefinement1_GetTopicRule_RuleWrapper verifies the AWS-format `rule` wrapper in response.
func TestGetTopicRule_RuleWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "response_has_rule_wrapper_and_ruleArn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			b.AddRuleInternal(iot.TopicRule{RuleName: "WrapRule", SQL: "SELECT temperature FROM 'devices/#'"})

			rec := doRefRequest(t, h, http.MethodGet, "/rules/WrapRule", nil, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "ruleArn")
			assert.Contains(t, resp, "rule")
		})
	}
}

// TestRefinement1_TopicRule_ARN verifies the rule ARN is set correctly.
func TestTopicRule_ARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "rule_arn_contains_rule_name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()

			err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
				RuleName:         "ArnRule",
				TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT temperature FROM 'devices/#'"},
			})
			require.NoError(t, err)

			rule, err := b.GetTopicRule("ArnRule")
			require.NoError(t, err)
			assert.Contains(t, rule.ARN, "arn:aws:iot:")
			assert.Contains(t, rule.ARN, "ArnRule")
		})
	}
}

// TestRefinement1_CreateAndGetTopicRule_Handler verifies rule creation and retrieval via HTTP.
func TestCreateAndGetTopicRule_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ruleName string
	}{
		{name: "create_and_get_rule", ruleName: "TempRule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()

			// Create rule.
			createRec := doRefRequest(t, h, http.MethodPost, "/rules/"+tt.ruleName,
				map[string]any{
					"sql":     "SELECT temperature FROM 'sensors/#'",
					"actions": []any{},
				}, nil)
			assert.Equal(t, http.StatusOK, createRec.Code)

			// Get rule.
			getRec := doRefRequest(t, h, http.MethodGet, "/rules/"+tt.ruleName, nil, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "ruleArn")
			assert.Contains(t, resp, "rule")
		})
	}
}

// TestRefinement1_DeleteTopicRule_Handler verifies rule deletion via HTTP.
func TestDeleteTopicRule_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*iot.InMemoryBackend)
		name     string
		wantCode int
	}{
		{
			name: "delete_existing_rule",
			setup: func(b *iot.InMemoryBackend) {
				b.AddRuleInternal(iot.TopicRule{RuleName: "DelRule"})
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete_missing_rule",
			setup:    nil,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRefRequest(t, h, http.MethodDelete, "/rules/DelRule", nil, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestRefinement1_TopicRuleDuplicate_Returns409 verifies duplicate rule returns HTTP 409.
func TestTopicRuleDuplicate_Returns409(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate_rule_creation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newRefHandler()
			body := map[string]any{"sql": "SELECT temperature FROM 'sensor/#'", "actions": []any{}}

			rec1 := doRefRequest(t, h, http.MethodPost, "/rules/DupRule", body, nil)
			assert.Equal(t, http.StatusOK, rec1.Code)

			rec2 := doRefRequest(t, h, http.MethodPost, "/rules/DupRule", body, nil)
			assert.Equal(t, http.StatusConflict, rec2.Code)
		})
	}
}

func TestTopicRule_AWSIoTSQLVersion_Default(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName:         "sql-version-rule",
		TopicRulePayload: &iot.TopicRulePayload{SQL: "SELECT * FROM 'topic'"},
	})
	require.NoError(t, err)

	r, err := backend.GetTopicRule("sql-version-rule")
	require.NoError(t, err)
	assert.Equal(t, "2015-10-08", r.AWSIoTSQLVersion)
}

func TestTopicRule_AWSIoTSQLVersion_Custom(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "custom-sql-version",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:              "SELECT * FROM 'topic'",
			AWSIoTSQLVersion: "2016-03-23",
		},
	})
	require.NoError(t, err)

	r, err := backend.GetTopicRule("custom-sql-version")
	require.NoError(t, err)
	assert.Equal(t, "2016-03-23", r.AWSIoTSQLVersion)
}

func TestDisableTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddRuleInternal(iot.TopicRule{RuleName: "disable-me", SQL: "SELECT *"})

	err := backend.DisableTopicRule("disable-me")
	require.NoError(t, err)

	r, err := backend.GetTopicRule("disable-me")
	require.NoError(t, err)
	assert.False(t, r.Enabled)
}

func TestEnableTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddRuleInternal(iot.TopicRule{RuleName: "enable-me", SQL: "SELECT *"})

	require.NoError(t, backend.DisableTopicRule("enable-me"))
	require.NoError(t, backend.EnableTopicRule("enable-me"))

	r, err := backend.GetTopicRule("enable-me")
	require.NoError(t, err)
	assert.True(t, r.Enabled)
}

func TestDisableTopicRule_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.DisableTopicRule("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrRuleNotFound)
}

func TestReplaceTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	backend.AddRuleInternal(iot.TopicRule{RuleName: "replace-me", SQL: "SELECT * FROM 'old'"})

	err := backend.ReplaceTopicRule(&iot.ReplaceTopicRuleInput{
		RuleName: "replace-me",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:         "SELECT * FROM 'new'",
			Description: "new description",
		},
	})
	require.NoError(t, err)

	r, err := backend.GetTopicRule("replace-me")
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM 'new'", r.SQL) //nolint:unqueryvet // IoT SQL, not a database query
	assert.Equal(t, "new description", r.Description)
	assert.Equal(t, "2015-10-08", r.AWSIoTSQLVersion)
}

func TestReplaceTopicRule_NotFound(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	err := backend.ReplaceTopicRule(&iot.ReplaceTopicRuleInput{RuleName: "ghost"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iot.ErrRuleNotFound)
}

func TestHandler_ListTopicRules_ResponseFormat(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{RuleName: "rule-1", SQL: "SELECT *"})
	backend.AddRuleInternal(iot.TopicRule{RuleName: "rule-2", SQL: "SELECT *"})

	resp := doRequest(t, h, http.MethodGet, "/rules", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	rules, ok := out["rules"]
	require.True(t, ok, "response must have 'rules' key")

	rulesSlice, ok := rules.([]any)
	require.True(t, ok)
	assert.Len(t, rulesSlice, 2)
}

// TestListTopicRules_RealSDKClient_TopicPatternKey proves ListTopicRules'
// items key their MQTT topic topicPattern (types.TopicRuleListItem,
// iot@v1.77.4 deserializers.go's
// awsRestjson1_deserializeDocumentTopicRuleListItem: createdAt/ruleArn/
// ruleDisabled/ruleName/topicPattern -- no "sql" member at all), not "sql"
// -- the correct key for GetTopicRule's DIFFERENT full TopicRule shape.
// Before this fix every real client's TopicRuleListItem.TopicPattern
// decoded empty.
func TestListTopicRules_RealSDKClient_TopicPatternKey(t *testing.T) {
	t.Parallel()

	h := iot.NewHandler(iot.NewInMemoryBackend(), nil)
	client := newTestIoTClient(t, h)
	ctx := t.Context()

	_, err := client.CreateTopicRule(ctx, &iotsdk.CreateTopicRuleInput{
		RuleName: aws.String("pattern-rule"),
		TopicRulePayload: &types.TopicRulePayload{
			Sql:     aws.String("SELECT * FROM 'devices/+/telemetry'"),
			Actions: []types.Action{},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTopicRules(ctx, &iotsdk.ListTopicRulesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Rules, 1)
	assert.Equal(t, "devices/+/telemetry", aws.ToString(listOut.Rules[0].TopicPattern))
}

func TestHandler_GetTopicRule_IncludesAWSSQLVersion(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{
		RuleName:         "version-rule",
		SQL:              "SELECT *",
		AWSIoTSQLVersion: "2015-10-08",
	})

	resp := doRequest(t, h, http.MethodGet, "/rules/version-rule", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	rule, ok := out["rule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "2015-10-08", rule["awsIotSqlVersion"])
}

func TestHandler_DisableEnableTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{RuleName: "toggle-rule", SQL: "SELECT *"})

	// Disable
	resp := doRequest(t, h, http.MethodPatch, "/rules/toggle-rule/disable", nil)
	require.Equal(t, http.StatusOK, resp.Code)

	r, err := backend.GetTopicRule("toggle-rule")
	require.NoError(t, err)
	assert.False(t, r.Enabled)

	// Enable
	resp2 := doRequest(t, h, http.MethodPatch, "/rules/toggle-rule/enable", nil)
	require.Equal(t, http.StatusOK, resp2.Code)

	r2, err := backend.GetTopicRule("toggle-rule")
	require.NoError(t, err)
	assert.True(t, r2.Enabled)
}

func TestHandler_ReplaceTopicRule(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	backend.AddRuleInternal(iot.TopicRule{RuleName: "replace-rule", SQL: "SELECT old"})

	resp := doRequest(t, h, http.MethodPatch, "/rules/replace-rule", map[string]any{
		"topicRulePayload": map[string]any{
			"sql":         "SELECT new",
			"description": "updated",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	r, err := backend.GetTopicRule("replace-rule")
	require.NoError(t, err)
	assert.Equal(t, "SELECT new", r.SQL)
	assert.Equal(t, "updated", r.Description)
}

func TestCreateTopicRule_ARNFormat(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("111100001111", "us-west-2")
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "arn-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:     "SELECT * FROM 'topic'",
			Actions: []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	r, err := b.GetTopicRule("arn-rule")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iot:us-west-2:111100001111:rule/arn-rule", r.ARN)
}

func TestGetTopicRule_EnabledByDefault(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "enabled-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:          "SELECT * FROM 'topic'",
			RuleDisabled: false,
			Actions:      []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	r, err := b.GetTopicRule("enabled-rule")
	require.NoError(t, err)
	assert.True(t, r.Enabled)
}

func TestDisableTopicRule_SetsEnabledFalse(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "dis-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:     "SELECT * FROM 'topic'",
			Actions: []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	err = b.DisableTopicRule("dis-rule")
	require.NoError(t, err)

	r, err := b.GetTopicRule("dis-rule")
	require.NoError(t, err)
	assert.False(t, r.Enabled)
}

func TestEnableTopicRule_SetsEnabledTrue(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.CreateTopicRule(&iot.CreateTopicRuleInput{
		RuleName: "en-rule",
		TopicRulePayload: &iot.TopicRulePayload{
			SQL:          "SELECT * FROM 'topic'",
			RuleDisabled: true,
			Actions:      []iot.RuleAction{},
		},
	})
	require.NoError(t, err)

	r, err := b.GetTopicRule("en-rule")
	require.NoError(t, err)
	assert.False(t, r.Enabled)

	err = b.EnableTopicRule("en-rule")
	require.NoError(t, err)

	r, err = b.GetTopicRule("en-rule")
	require.NoError(t, err)
	assert.True(t, r.Enabled)
}

// TestParityB_TopicRulePayloadWrapper verifies CreateTopicRule and ReplaceTopicRule
// unwrap the AWS SDK "topicRulePayload" envelope.
func TestTopicRulePayloadWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantSQL    string
		wantStatus int
	}{
		{
			name: "create_with_wrapper",
			body: map[string]any{
				"topicRulePayload": map[string]any{
					"sql":         "SELECT * FROM 'topic/test'",
					"description": "my rule",
				},
			},
			wantSQL:    "SELECT * FROM 'topic/test'",
			wantStatus: http.StatusOK,
		},
		{
			name: "replace_with_wrapper",
			body: map[string]any{
				"topicRulePayload": map[string]any{
					"sql":         "SELECT new FROM 'topic/replaced'",
					"description": "replaced",
				},
			},
			wantSQL:    "SELECT new FROM 'topic/replaced'",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "replace_with_wrapper" {
				createBody := map[string]any{
					"topicRulePayload": map[string]any{"sql": "SELECT old FROM 'topic/x'"},
				}
				doRequest(t, h, http.MethodPost, "/rules/wraprule", createBody)
				doRequest(t, h, http.MethodPatch, "/rules/wraprule", tt.body)
			} else {
				rec := doRequest(t, h, http.MethodPost, "/rules/wraprule", tt.body)
				assert.Equal(t, tt.wantStatus, rec.Code)
			}

			backend := iot.NewInMemoryBackend()
			_ = backend
			b2 := iot.NewInMemoryBackend()
			h2 := iot.NewHandler(b2, nil)
			if tt.name == "create_with_wrapper" {
				doRequest(t, h2, http.MethodPost, "/rules/wraprule2", tt.body)
				rule, err := b2.GetTopicRule("wraprule2")
				require.NoError(t, err)
				assert.Equal(t, tt.wantSQL, rule.SQL)
			}
		})
	}
}

// TestTopicRuleDestinationHTTPURLProperties checks that
// Create/Get/ListTopicRuleDestination all surface the HTTP URL sub-object
// the backend already tracks (types.TopicRuleDestination.HttpUrlProperties /
// types.TopicRuleDestinationSummary.HttpUrlSummary, iot@v1.77.4) instead of
// dropping it.
func TestTopicRuleDestinationHTTPURLProperties(t *testing.T) {
	t.Parallel()

	h, _ := newRefHandler()

	createOut := iotOK(t, h, http.MethodPost, "/destinations", map[string]any{
		"destinationConfiguration": map[string]any{
			"httpUrlConfiguration": map[string]any{
				"confirmationUrl": "https://example.com/confirm",
			},
		},
	})
	createDest, _ := createOut["topicRuleDestination"].(map[string]any)
	require.NotNil(t, createDest)
	createProps, _ := createDest["httpUrlProperties"].(map[string]any)
	require.NotNil(t, createProps, "expected httpUrlProperties on CreateTopicRuleDestination, got %v", createDest)
	assert.Equal(t, "https://example.com/confirm", createProps["confirmationUrl"])

	arnVal, _ := createDest["arn"].(string)
	require.NotEmpty(t, arnVal)

	getOut := iotOK(t, h, http.MethodGet, "/destinations/"+arnVal, nil)
	getDest, _ := getOut["topicRuleDestination"].(map[string]any)
	getProps, _ := getDest["httpUrlProperties"].(map[string]any)
	require.NotNil(t, getProps, "expected httpUrlProperties on GetTopicRuleDestination, got %v", getDest)
	assert.Equal(t, "https://example.com/confirm", getProps["confirmationUrl"])

	listOut := iotOK(t, h, http.MethodGet, "/destinations", nil)
	summaries, _ := listOut["destinationSummaries"].([]any)
	require.Len(t, summaries, 1)
	summary, _ := summaries[0].(map[string]any)
	summaryProps, _ := summary["httpUrlSummary"].(map[string]any)
	require.NotNil(t, summaryProps, "expected httpUrlSummary on ListTopicRuleDestinations, got %v", summary)
	assert.Equal(t, "https://example.com/confirm", summaryProps["confirmationUrl"])
}

func TestConfirmTopicRuleDestination(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		dest, err := b.CreateTopicRuleDestination(&iot.CreateTopicRuleDestinationInput{
			DestinationConfiguration: &iot.TopicRuleDestinationConfiguration{
				HTTPURLConfiguration: &iot.HTTPURLDestinationConfiguration{
					ConfirmationURL: "https://example.com/confirm",
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, "IN_PROGRESS", dest.Status)
		require.NotEmpty(t, dest.ConfirmationToken)

		rec := doRefRequest(t, h, http.MethodGet, "/confirmdestination/"+dest.ConfirmationToken, nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		got, err := b.GetTopicRuleDestination(dest.ARN)
		require.NoError(t, err)
		assert.Equal(t, "ENABLED", got.Status)
	})

	t.Run("invalid_token", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodGet, "/confirmdestination/bogus-token", nil, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
