package codepipeline_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestHandler_DeleteWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
	}{
		{
			name: "success_existing",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{Name: "wh-1", TargetPipeline: "pl-1"})
			},
			input:      map[string]any{"name": "wh-1"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "idempotent_nonexistent",
			setup:      nil,
			input:      map[string]any{"name": "no-such-webhook"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			setup:      nil,
			input:      map[string]any{},
			httpStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteWebhook", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)
		})
	}
}

func TestHandler_DeregisterWebhookWithThirdParty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
	}{
		{
			name: "success_existing",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{
					Name:                     "wh-2",
					TargetPipeline:           "pl-2",
					RegisteredWithThirdParty: true,
				})
			},
			input:      map[string]any{"webhookName": "wh-2"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "idempotent_nonexistent",
			setup:      nil,
			input:      map[string]any{"webhookName": "no-such"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "empty_name_is_ok",
			setup:      nil,
			input:      map[string]any{},
			httpStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeregisterWebhookWithThirdParty", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if tt.httpStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Empty(t, out)
			}
		})
	}
}

func TestHandler_Webhook_FullModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		webhook    map[string]any
		checkFn    func(t *testing.T, out map[string]any)
		name       string
		tags       []map[string]any
		wantStatus int
	}{
		{
			name: "webhook with filters and GITHUB_HMAC auth",
			webhook: map[string]any{
				"name":           "wh-full",
				"targetPipeline": "my-pipeline",
				"targetAction":   "SourceAction",
				"authentication": "GITHUB_HMAC",
				"authenticationConfiguration": map[string]any{
					"secretToken": "super-secret",
				},
				"filters": []map[string]any{
					{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"},
				},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, out map[string]any) {
				t.Helper()

				wh, ok := out["webhook"].(map[string]any)
				require.True(t, ok, "webhook key missing")

				def, ok := wh["definition"].(map[string]any)
				require.True(t, ok, "definition key missing")
				assert.Equal(t, "wh-full", def["name"])
				assert.Equal(t, "GITHUB_HMAC", def["authentication"])

				filters, _ := def["filters"].([]any)
				require.Len(t, filters, 1)
				f0, _ := filters[0].(map[string]any)
				assert.Equal(t, "$.ref", f0["jsonPath"])
				assert.Equal(t, "refs/heads/main", f0["matchEquals"])

				// URL and ARN should be populated
				assert.NotEmpty(t, wh["url"])
				assert.NotEmpty(t, wh["arn"])
			},
		},
		{
			name: "webhook tags supplied at PutWebhook round-trip onto the response",
			webhook: map[string]any{
				"name":           "wh-tagged",
				"targetPipeline": "my-pipeline",
				"targetAction":   "SourceAction",
				"authentication": "UNAUTHENTICATED",
			},
			tags: []map[string]any{
				{"key": "env", "value": "prod"},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, out map[string]any) {
				t.Helper()

				wh, ok := out["webhook"].(map[string]any)
				require.True(t, ok, "webhook key missing")

				tags, ok := wh["tags"].([]any)
				require.True(t, ok, "tags key missing from PutWebhook response")
				require.Len(t, tags, 1)
				tag, _ := tags[0].(map[string]any)
				assert.Equal(t, "env", tag["key"])
				assert.Equal(t, "prod", tag["value"])
			},
		},
		{
			name: "webhook with IP auth",
			webhook: map[string]any{
				"name":           "wh-ip",
				"targetPipeline": "my-pipeline",
				"targetAction":   "SourceAction",
				"authentication": "IP",
				"authenticationConfiguration": map[string]any{
					"allowedIPRange": "192.168.1.0/24",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid authentication type rejected",
			webhook: map[string]any{
				"name":           "wh-bad",
				"targetPipeline": "my-pipeline",
				"targetAction":   "SourceAction",
				"authentication": "INVALID_AUTH",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing webhook name rejected",
			webhook:    map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"webhook": tt.webhook}
			if tt.tags != nil {
				body["tags"] = tt.tags
			}
			rec := doRequest(t, h, "PutWebhook", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkFn != nil && tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.checkFn(t, out)
			}
		})
	}
}

func TestHandler_ListWebhooks_DefinitionWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		checkFn    func(t *testing.T, webhooks []any)
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "ListWebhooks wraps in definition",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{
					Name:           "list-wh-1",
					TargetPipeline: "pl-1",
					TargetAction:   "src",
					Authentication: "GITHUB_HMAC",
					Filters: []codepipeline.WebhookFilter{
						{JSONPath: "$.ref", MatchEquals: "refs/heads/main"},
					},
				})
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
			checkFn: func(t *testing.T, webhooks []any) {
				t.Helper()

				wh0, _ := webhooks[0].(map[string]any)
				def, ok := wh0["definition"].(map[string]any)
				require.True(t, ok, "definition key must be present in ListWebhooks response")
				assert.Equal(t, "list-wh-1", def["name"])
				assert.Equal(t, "GITHUB_HMAC", def["authentication"])

				filters, _ := def["filters"].([]any)
				require.Len(t, filters, 1)
			},
		},
		{
			name:       "empty list returns empty array",
			setup:      nil,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "multiple webhooks sorted",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{Name: "b-hook", TargetPipeline: "pl"})
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{Name: "a-hook", TargetPipeline: "pl"})
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
			checkFn: func(t *testing.T, webhooks []any) {
				t.Helper()

				first := webhooks[0].(map[string]any)["definition"].(map[string]any)
				assert.Equal(t, "a-hook", first["name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListWebhooks", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			webhooks, _ := out["webhooks"].([]any)
			assert.Len(t, webhooks, tt.wantCount)

			if tt.checkFn != nil && len(webhooks) > 0 {
				tt.checkFn(t, webhooks)
			}
		})
	}
}

// --------------------------------------------------------------------------
// #13 PollForJobs filtered by ActionTypeID
// --------------------------------------------------------------------------

func TestHandler_WebhookLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("wh-pipeline"),
	})

	// Put webhook
	rec := doRequest(t, h, "PutWebhook", map[string]any{
		"webhook": map[string]any{
			"name":           "my-webhook",
			"targetPipeline": "wh-pipeline",
			"targetAction":   "Source",
			"authentication": "GITHUB_HMAC",
		},
	})
	require.Equal(t, 200, rec.Code)

	// Put with missing name
	rec = doRequest(t, h, "PutWebhook", map[string]any{"webhook": map[string]any{}})
	assert.Equal(t, 400, rec.Code)

	// Register webhook
	rec = doRequest(t, h, "RegisterWebhookWithThirdParty", map[string]any{
		"webhookName": "my-webhook",
	})
	assert.Equal(t, 200, rec.Code)

	// Register with missing name
	rec = doRequest(t, h, "RegisterWebhookWithThirdParty", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// List webhooks
	rec = doRequest(t, h, "ListWebhooks", map[string]any{})
	require.Equal(t, 200, rec.Code)

	// Deregister webhook
	rec = doRequest(t, h, "DeregisterWebhookWithThirdParty", map[string]any{
		"webhookName": "my-webhook",
	})
	assert.Equal(t, 200, rec.Code)

	// Deregister with missing name (returns 200 - no validation on webhook name)
	rec = doRequest(t, h, "DeregisterWebhookWithThirdParty", map[string]any{})
	assert.Equal(t, 200, rec.Code)

	// Delete webhook
	rec = doRequest(t, h, "DeleteWebhook", map[string]any{
		"name": "my-webhook",
	})
	assert.Equal(t, 200, rec.Code)

	// Delete with missing name
	rec = doRequest(t, h, "DeleteWebhook", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Job tests ----

func TestCPBounds_ListWebhooks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Seed one pipeline for the webhook target.
	setupRec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("wh-pipe"),
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	setupRec = doRequest(t, h, "PutWebhook", map[string]any{
		"webhook": map[string]any{
			"name":                        "wh-bounds",
			"targetPipeline":              "wh-pipe",
			"targetAction":                "SourceAction",
			"authentication":              "UNAUTHENTICATED",
			"filters":                     []any{},
			"authenticationConfiguration": map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, setupRec.Code)

	tests := []struct {
		name       string
		maxResults int32
		wantError  bool
	}{
		{"0 uses cap", 0, false},
		{"1 is valid", 1, false},
		{"60 is valid cap", 60, false},
		{"61 exceeds cap", 61, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListWebhooks", map[string]any{
				"MaxResults": tc.maxResults,
			})

			if tc.wantError {
				assert.NotEqual(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestCPPagination_ListWebhooks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("wh-page-pipe"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	for i := range 5 {
		rec = doRequest(t, h, "PutWebhook", map[string]any{
			"webhook": map[string]any{
				"name":                        fmt.Sprintf("wh-%02d", i),
				"targetPipeline":              "wh-page-pipe",
				"targetAction":                "SourceAction",
				"authentication":              "UNAUTHENTICATED",
				"filters":                     []any{},
				"authenticationConfiguration": map[string]any{},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var nextToken string
	total := 0
	pages := 0

	for {
		body := map[string]any{
			"MaxResults": int32(2),
		}
		if nextToken != "" {
			body["NextToken"] = nextToken
		}

		rec = doRequest(t, h, "ListWebhooks", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken string           `json:"NextToken"`
			Webhooks  []map[string]any `json:"webhooks"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

		total += len(out.Webhooks)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}

// ---------------------------------------------------------------------------
// ListActionExecutions — MaxResults (1-100) + multi-page
// ---------------------------------------------------------------------------

func TestPutWebhook_Filters_EmptyNotAbsent(t *testing.T) {
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

func TestListWebhooks_Filters_EmptyNotAbsent(t *testing.T) {
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

func TestListWebhooks_Filters_RoundTrip(t *testing.T) {
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
