package eventbridge_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListRuleNamesByTarget(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "h-rule",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "h-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:lambda:us-east-1:123456789012:function:fn"},
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "ListRuleNamesByTarget", map[string]any{
		"TargetArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-rule")
}

func TestHandler_TestEventPattern(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "TestEventPattern", map[string]any{
		"EventPattern": `{"source":["myapp"]}`,
		"Event":        `{"source":"myapp","detail-type":"X","detail":{}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result bool `json:"Result"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result)
}

func TestHandler_DisableAndEnableRule(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "toggle-h-rule",
		EventPattern: `{"source":["x"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "DisableRule", map[string]any{"Name": "toggle-h-rule"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "EnableRule", map[string]any{"Name": "toggle-h-rule"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestPutRule_TagsPersisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]any
		wantTags map[string]string
		name     string
		ruleName string
	}{
		{
			name:     "tags at rule creation are stored",
			ruleName: "tagged-rule",
			tags:     map[string]any{"owner": "alice", "cost-center": "eng"},
			wantTags: map[string]string{
				"owner":       "alice",
				"cost-center": "eng",
			},
		},
		{
			name:     "rule with no tags has empty tag set",
			ruleName: "bare-rule",
			tags:     nil,
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			body := map[string]any{
				"Name":         tt.ruleName,
				"EventPattern": `{"source":["test"]}`,
				"State":        "ENABLED",
			}
			if tt.tags != nil {
				body["Tags"] = tt.tags
			}
			rec := auditMakeRequest(t, h, e, "PutRule", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var putResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			ruleArn := putResp["RuleArn"]
			require.NotEmpty(t, ruleArn)

			rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{"ResourceARN": ruleArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var tagsResp struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))

			got := make(map[string]string, len(tagsResp.Tags))
			for _, kv := range tagsResp.Tags {
				got[kv.Key] = kv.Value
			}
			assert.Equal(t, tt.wantTags, got)
		})
	}
}

func TestHandler_PutRule_MissingName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "missing name returns bad request",
			body:     `{"EventBusName":"default"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, "PutRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestDeleteRule_CleansUpTags verifies that deleting a rule removes its
// tag entry from the handler so the tags map doesn't grow unbounded.
func TestDeleteRule_CleansUpTags(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)
	e := echo.New()

	// Create a rule (Tags is map[string]string in this backend's JSON encoding).
	rec := makeRequestWithHandler(t, handler, e, "PutRule",
		`{"Name":"temp-rule","EventPattern":"{\"source\":[\"test\"]}","Tags":{"team":"backend"}}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var putOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putOut))
	ruleARN := putOut["RuleArn"]
	require.NotEmpty(t, ruleARN)

	// Confirm tag is set.
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource",
		`{"ResourceARN":"`+ruleARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "team")

	// Delete the rule.
	rec = makeRequestWithHandler(t, handler, e, "DeleteRule",
		`{"Name":"temp-rule","EventBusName":"default"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Tag entry should be gone.
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource",
		`{"ResourceARN":"`+ruleARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "team")
}

func TestHandler_PutRuleAndListRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		putBody      string
		wantArnPart  string
		wantRuleName string
		wantRuleLen  int
	}{
		{
			name:         "put rule then list returns that rule with correct ARN",
			putBody:      `{"Name":"my-rule","EventPattern":"{\"source\":[\"my.app\"]}","State":"ENABLED"}`,
			wantArnPart:  "my-rule",
			wantRuleName: "my-rule",
			wantRuleLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			rec := makeRequestWithHandler(t, handler, e, "PutRule", tt.putBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var putResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			assert.Contains(t, putResp["RuleArn"], tt.wantArnPart)

			rec = makeRequestWithHandler(t, handler, e, "ListRules", `{}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp struct {
				Rules []eventbridge.Rule `json:"Rules"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Rules, tt.wantRuleLen)
			assert.Equal(t, tt.wantRuleName, listResp.Rules[0].Name)
		})
	}
}

func TestHandler_DescribeRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		putBody         string
		describeBody    string
		wantRuleName    string
		wantDescription string
		wantState       string
	}{
		{
			name: "describe rule returns name description and state",
			putBody: `{"Name":"desc-rule","Description":"a description",` +
				`"State":"DISABLED","ScheduleExpression":"rate(1 minute)"}`,
			describeBody:    `{"Name":"desc-rule"}`,
			wantRuleName:    "desc-rule",
			wantDescription: "a description",
			wantState:       "DISABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			makeRequestWithHandler(t, handler, e, "PutRule", tt.putBody)

			rec := makeRequestWithHandler(t, handler, e, "DescribeRule", tt.describeBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var rule eventbridge.Rule
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
			assert.Equal(t, tt.wantRuleName, rule.Name)
			assert.Equal(t, tt.wantDescription, rule.Description)
			assert.Equal(t, tt.wantState, rule.State)
		})
	}
}

func TestHandler_EnableDisableRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		ruleName          string
		wantDisabledState string
		wantEnabledState  string
	}{
		{
			name:              "toggle rule: disable then re-enable changes state correctly",
			ruleName:          "toggle",
			wantDisabledState: "DISABLED",
			wantEnabledState:  "ENABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := eventbridge.NewInMemoryBackend()
			handler := eventbridge.NewHandler(backend)

			makeRequestWithHandler(
				t,
				handler,
				e,
				"PutRule",
				`{"Name":"`+tt.ruleName+`","State":"ENABLED","ScheduleExpression":"rate(1 minute)"}`,
			)

			rec := makeRequestWithHandler(t, handler, e, "DisableRule", `{"Name":"`+tt.ruleName+`"}`)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = makeRequestWithHandler(t, handler, e, "DescribeRule", `{"Name":"`+tt.ruleName+`"}`)
			var rule eventbridge.Rule
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
			assert.Equal(t, tt.wantDisabledState, rule.State)

			makeRequestWithHandler(t, handler, e, "EnableRule", `{"Name":"`+tt.ruleName+`"}`)

			rec = makeRequestWithHandler(t, handler, e, "DescribeRule", `{"Name":"`+tt.ruleName+`"}`)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rule))
			assert.Equal(t, tt.wantEnabledState, rule.State)
		})
	}
}
