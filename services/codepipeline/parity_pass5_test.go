package codepipeline_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// TestParity_ListPipelines_Pagination verifies ListPipelines MaxResults/NextToken support.
func TestParity_ListPipelines_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pipe-a", "pipe-b", "pipe-c", "pipe-d"} {
		rec := doRequest(t, h, "CreatePipeline", map[string]any{
			"pipeline": samplePipeline(name),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"maxResults": int32(2)},
			wantLen:       2,
			wantNextToken: true,
		},
		{
			name:          "over_cap_rejected",
			body:          map[string]any{"maxResults": int32(1001)},
			wantLen:       0,
			wantNextToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "ListPipelines", tt.body)
			if tt.name == "over_cap_rejected" {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}
			require.Equal(t, http.StatusOK, rec.Code)
			var out struct {
				NextToken string           `json:"nextToken"`
				Pipelines []map[string]any `json:"pipelines"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Pipelines, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListPipelines_FullPagination walks all pages and collects all pipelines.
func TestParity_ListPipelines_FullPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	const total = 5
	for i := range total {
		names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
		rec := doRequest(t, h, "CreatePipeline", map[string]any{
			"pipeline": samplePipeline(names[i]),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		body := map[string]any{"maxResults": int32(2)}
		if token != "" {
			body["nextToken"] = token
		}

		rec := doRequest(t, h, "ListPipelines", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken string           `json:"nextToken"`
			Pipelines []map[string]any `json:"pipelines"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.LessOrEqual(t, len(out.Pipelines), 2)

		for _, p := range out.Pipelines {
			name := p["name"].(string)
			assert.False(t, seen[name], "pipeline %s seen twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10)

		token = out.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total)
	assert.GreaterOrEqual(t, pages, 3)
}

// TestParity_GetPipelineState_PipelineVersion verifies pipelineVersion is returned.
func TestParity_GetPipelineState_PipelineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name        string
		wantVersion float64
		updates     int
	}{
		{name: "version_1_on_create", updates: 0, wantVersion: 1},
		{name: "version_2_after_update", updates: 1, wantVersion: 2},
		{name: "version_3_after_two_updates", updates: 2, wantVersion: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h2 := newTestHandler(t)
			pl := samplePipeline("state-version-pipe")

			rec := doRequest(t, h2, "CreatePipeline", map[string]any{"pipeline": pl})
			require.Equal(t, http.StatusOK, rec.Code)

			for range tt.updates {
				rec = doRequest(t, h2, "UpdatePipeline", map[string]any{"pipeline": pl})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec = doRequest(t, h2, "GetPipelineState", map[string]any{"name": pl.Name})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.InDelta(t, tt.wantVersion, out["pipelineVersion"], 0, "pipelineVersion mismatch")
		})
	}

	_ = h
}

// TestParity_GetJobDetails_DataPopulated verifies data.actionTypeId is populated.
func TestParity_GetJobDetails_DataPopulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category string
		provider string
		version  string
	}{
		{name: "build_action", category: "Build", provider: "MyBuild", version: "1"},
		{name: "deploy_action", category: "Deploy", provider: "MyDeploy", version: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			h.Backend.AddJobInternal(&codepipeline.Job{
				ID:    "job-" + tt.name,
				Nonce: "nonce-1",
				ActionTypeID: codepipeline.ActionTypeID{
					Category: tt.category,
					Owner:    "Custom",
					Provider: tt.provider,
					Version:  tt.version,
				},
				Status: "Queued",
			})

			rec := doRequest(t, h, "GetJobDetails", map[string]any{"jobId": "job-" + tt.name})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			details, ok := out["jobDetails"].(map[string]any)
			require.True(t, ok, "jobDetails must be present")

			data, ok := details["data"].(map[string]any)
			require.True(t, ok, "data must be present")

			atID, ok := data["actionTypeId"].(map[string]any)
			require.True(t, ok, "data.actionTypeId must be present")
			assert.Equal(t, tt.category, atID["category"])
			assert.Equal(t, tt.provider, atID["provider"])
			assert.Equal(t, tt.version, atID["version"])
		})
	}
}

// TestParity_PutApprovalResult_ApprovedAt verifies approvedAt is non-empty.
func TestParity_PutApprovalResult_ApprovedAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "approved", status: "Approved"},
		{name: "rejected", status: "Rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "CreatePipeline", map[string]any{
				"pipeline": samplePipeline("approval-pipe"),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "PutApprovalResult", map[string]any{
				"pipelineName": "approval-pipe",
				"stageName":    "Source",
				"actionName":   "SourceAction",
				"approvalResult": map[string]any{
					"status":  tt.status,
					"summary": "looks good",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			approvedAt, _ := out["approvedAt"].(string)
			assert.NotEmpty(t, approvedAt, "approvedAt must be set")
		})
	}
}

// TestParity_ListPipelineExecutions_TriggerObject verifies trigger is an object.
func TestParity_ListPipelineExecutions_TriggerObject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("trigger-pipe"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	for range 2 {
		rec = doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "trigger-pipe"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = doRequest(t, h, "ListPipelineExecutions", map[string]any{"pipelineName": "trigger-pipe"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		PipelineExecutionSummaries []map[string]any `json:"pipelineExecutionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.PipelineExecutionSummaries, 2)

	for _, exec := range out.PipelineExecutionSummaries {
		trigger, ok := exec["trigger"].(map[string]any)
		assert.True(t, ok, "trigger must be an object, not a string")
		if ok {
			assert.NotEmpty(t, trigger["triggerType"], "trigger.triggerType must be set")
		}
	}
}

// TestParity_Webhook_Tagging verifies TagResource/UntagResource/ListTagsForResource on webhooks.
func TestParity_Webhook_Tagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]any
		untag    []string
		wantTags []string
	}{
		{
			name:     "tag_then_list",
			tags:     []map[string]any{{"key": "env", "value": "prod"}, {"key": "team", "value": "infra"}},
			wantTags: []string{"env", "team"},
		},
		{
			name:     "tag_then_untag",
			tags:     []map[string]any{{"key": "env", "value": "prod"}, {"key": "team", "value": "infra"}},
			untag:    []string{"env"},
			wantTags: []string{"team"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "CreatePipeline", map[string]any{
				"pipeline": samplePipeline("wh-tag-pipe"),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Create webhook.
			putRec := doRequest(t, h, "PutWebhook", map[string]any{
				"webhook": map[string]any{
					"name":                        "tag-wh",
					"targetPipeline":              "wh-tag-pipe",
					"targetAction":                "SourceAction",
					"authentication":              "UNAUTHENTICATED",
					"authenticationConfiguration": map[string]any{},
				},
			})
			require.Equal(t, http.StatusOK, putRec.Code)

			// Get the webhook ARN.
			var putOut map[string]any
			require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
			webhookEntry := putOut["webhook"].(map[string]any)
			arn := webhookEntry["arn"].(string)
			require.NotEmpty(t, arn)

			// Tag.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"resourceArn": arn,
				"tags":        tt.tags,
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			// Untag.
			if len(tt.untag) > 0 {
				untagRec := doRequest(t, h, "UntagResource", map[string]any{
					"resourceArn": arn,
					"tagKeys":     tt.untag,
				})
				require.Equal(t, http.StatusOK, untagRec.Code)
			}

			// List tags.
			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": arn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
			rawTags, _ := listOut["tags"].([]any)
			gotKeys := make([]string, 0, len(rawTags))

			for _, rt := range rawTags {
				tag := rt.(map[string]any)
				gotKeys = append(gotKeys, tag["key"].(string))
			}

			for _, wantKey := range tt.wantTags {
				assert.Contains(t, gotKeys, wantKey)
			}

			assert.Len(t, gotKeys, len(tt.wantTags))
		})
	}
}
