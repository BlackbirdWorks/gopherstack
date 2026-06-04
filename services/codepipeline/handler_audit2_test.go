package codepipeline_test

// handler_audit2_test.go — Tests for AWS-accuracy audit items from issue go-pq9o.
// All tests use table-driven format.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Bug 1: PipelineNameInUseException for duplicate CreatePipeline ──────────

func TestAudit2_CreatePipeline_DuplicateName_PipelineNameInUseException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate pipeline name returns PipelineNameInUseException"},
		{name: "second duplicate also returns PipelineNameInUseException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			pipeline := samplePipeline("my-pipeline")

			// First create succeeds.
			rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
			require.Equal(t, http.StatusOK, rec.Code, "first CreatePipeline must succeed")

			// Second create with same name must return PipelineNameInUseException.
			rec2 := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
			require.Equal(t, http.StatusBadRequest, rec2.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &body))

			errType, ok := body["__type"].(string)
			require.True(t, ok, "response must have __type field")
			assert.Equal(t, "PipelineNameInUseException", errType,
				"duplicate pipeline name must return PipelineNameInUseException not InvalidStructureException")
		})
	}
}

func TestAudit2_CreatePipeline_DuplicateName_NotInvalidStructureException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	pipeline := samplePipeline("collision-pipeline")

	rec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
	require.Equal(t, http.StatusBadRequest, rec2.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &body))

	errType, _ := body["__type"].(string)
	assert.NotEqual(t, "InvalidStructureException", errType,
		"duplicate pipeline name must NOT return InvalidStructureException")
}

// ── Bug 2: ActionConfigurationProperties [] not absent ──────────────────────

func TestAudit2_CreateCustomActionType_ActionConfigurationProperties_EmptyNotAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		includeEmptyVal bool // true → send configurationProperties:[], false → omit key
	}{
		{
			name:            "no configurationProperties returns actionConfigurationProperties:[]",
			includeEmptyVal: false,
		},
		{
			name:            "empty configurationProperties returns actionConfigurationProperties:[]",
			includeEmptyVal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"category":              "Build",
				"provider":              "MyProvider",
				"version":               "1",
				"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
				"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
			}
			if tt.includeEmptyVal {
				body["configurationProperties"] = []any{}
			}

			rec := doRequest(t, h, "CreateCustomActionType", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			actionType, ok := raw["actionType"].(map[string]any)
			require.True(t, ok, "actionType must be an object")

			props, hasKey := actionType["actionConfigurationProperties"]
			assert.True(t, hasKey,
				"CreateCustomActionType must include 'actionConfigurationProperties' key even when empty")
			assert.IsType(t, []any{}, props,
				"'actionConfigurationProperties' must be an array, not null or absent")
			assert.Empty(t, props, "'actionConfigurationProperties' must be [] not populated")
		})
	}
}

func TestAudit2_GetActionType_ActionConfigurationProperties_EmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create action type without configuration properties.
	createRec := doRequest(t, h, "CreateCustomActionType", map[string]any{
		"category":              "Build",
		"provider":              "MyProvider",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// GetActionType must also return actionConfigurationProperties: [].
	rec := doRequest(t, h, "GetActionType", map[string]any{
		"category": "Build",
		"owner":    "Custom",
		"provider": "MyProvider",
		"version":  "1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	actionType, ok := raw["actionType"].(map[string]any)
	require.True(t, ok, "actionType must be an object")

	props, hasKey := actionType["actionConfigurationProperties"]
	assert.True(t, hasKey,
		"GetActionType must include 'actionConfigurationProperties' key even when empty")
	assert.IsType(t, []any{}, props,
		"'actionConfigurationProperties' must be an array, not null or absent")
}

func TestAudit2_CreateCustomActionType_ActionConfigurationProperties_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create with explicit configuration properties.
	rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
		"category":              "Build",
		"provider":              "MyProvider",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
		"configurationProperties": []any{
			map[string]any{"name": "ProjectName", "required": true, "key": true, "secret": false},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	actionType, ok := raw["actionType"].(map[string]any)
	require.True(t, ok)

	props, hasKey := actionType["actionConfigurationProperties"]
	assert.True(t, hasKey, "must have actionConfigurationProperties key")
	propsSlice, ok := props.([]any)
	require.True(t, ok, "must be an array")
	assert.Len(t, propsSlice, 1, "must have the one property we created")
}

// ── Bug 3: Webhook Filters [] not absent ────────────────────────────────────

func TestAudit2_PutWebhook_Filters_EmptyNotAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		includeEmptyVal bool // true → send filters:[], false → omit key
	}{
		{
			name:            "no filters returns filters:[]",
			includeEmptyVal: false,
		},
		{
			name:            "empty filters slice returns filters:[]",
			includeEmptyVal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create the target pipeline first.
			pipeline := samplePipeline("webhook-target")
			createRec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
			require.Equal(t, http.StatusOK, createRec.Code)

			webhookDef := map[string]any{
				"name":                        "my-webhook",
				"targetPipeline":              "webhook-target",
				"targetAction":                "SourceAction",
				"authentication":              "UNAUTHENTICATED",
				"authenticationConfiguration": map[string]any{},
			}
			if tt.includeEmptyVal {
				webhookDef["filters"] = []any{}
			}

			rec := doRequest(t, h, "PutWebhook", map[string]any{"webhook": webhookDef})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			webhook, ok := raw["webhook"].(map[string]any)
			require.True(t, ok, "webhook must be an object")

			defn, ok := webhook["definition"].(map[string]any)
			require.True(t, ok, "definition must be an object")

			filters, hasKey := defn["filters"]
			assert.True(t, hasKey, "PutWebhook response must include 'filters' key even when empty")
			assert.IsType(t, []any{}, filters,
				"'filters' must be an array, not null or absent")
		})
	}
}

func TestAudit2_ListWebhooks_Filters_EmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create target pipeline.
	pipeline := samplePipeline("list-webhook-target")
	createRec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Create webhook with no filters.
	putRec := doRequest(t, h, "PutWebhook", map[string]any{
		"webhook": map[string]any{
			"name":                        "list-test-webhook",
			"targetPipeline":              "list-webhook-target",
			"targetAction":                "SourceAction",
			"authentication":              "UNAUTHENTICATED",
			"authenticationConfiguration": map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// ListWebhooks must return filters: [] for the webhook.
	rec := doRequest(t, h, "ListWebhooks", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	webhooks, ok := raw["webhooks"].([]any)
	require.True(t, ok, "webhooks must be an array")
	require.Len(t, webhooks, 1, "must have one webhook")

	webhook, ok := webhooks[0].(map[string]any)
	require.True(t, ok)

	defn, ok := webhook["definition"].(map[string]any)
	require.True(t, ok, "definition must be an object")

	filters, hasKey := defn["filters"]
	assert.True(t, hasKey, "ListWebhooks response must include 'filters' key even when empty")
	assert.IsType(t, []any{}, filters, "'filters' must be an array, not null or absent")
	assert.Empty(t, filters, "'filters' must be [] not populated")
}

func TestAudit2_ListWebhooks_Filters_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create target pipeline.
	pipeline := samplePipeline("filter-roundtrip-target")
	createRec := doRequest(t, h, "CreatePipeline", map[string]any{"pipeline": pipeline})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Create webhook with filters.
	putRec := doRequest(t, h, "PutWebhook", map[string]any{
		"webhook": map[string]any{
			"name":                        "filter-roundtrip-webhook",
			"targetPipeline":              "filter-roundtrip-target",
			"targetAction":                "SourceAction",
			"authentication":              "UNAUTHENTICATED",
			"authenticationConfiguration": map[string]any{},
			"filters": []any{
				map[string]any{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"},
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doRequest(t, h, "ListWebhooks", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	webhooks, ok := raw["webhooks"].([]any)
	require.True(t, ok)
	require.Len(t, webhooks, 1)

	webhook := webhooks[0].(map[string]any)
	defn := webhook["definition"].(map[string]any)
	filters, hasKey := defn["filters"]
	assert.True(t, hasKey)
	filtersSlice, ok := filters.([]any)
	require.True(t, ok, "filters must be an array")
	assert.Len(t, filtersSlice, 1, "must round-trip the one filter")
}
