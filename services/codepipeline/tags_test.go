package codepipeline_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func TestHandler_TaggingOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler) string
		input      func(arn string) any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:   "list tags - empty",
			action: "ListTagsForResource",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("list-empty-pipeline"), nil)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			input: func(arn string) any {
				return map[string]any{"resourceArn": arn}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "tag resource",
			action: "TagResource",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("tag-resource-pipeline"), nil)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			input: func(arn string) any {
				return map[string]any{
					"resourceArn": arn,
					"tags":        []map[string]string{{"key": "Environment", "value": "test"}},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "untag resource",
			action: "UntagResource",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(context.Background(),
					samplePipeline("untag-resource-pipeline"),
					map[string]string{"Env": "test"},
				)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			input: func(arn string) any {
				return map[string]any{
					"resourceArn": arn,
					"tagKeys":     []string{"Env"},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "list tags - not found ARN",
			action: "ListTagsForResource",
			setup:  nil,
			input: func(_ string) any {
				return map[string]any{
					"resourceArn": "arn:aws:codepipeline:us-east-1:000:nonexistent",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "tag resource - missing ARN",
			action: "TagResource",
			setup:  nil,
			input: func(_ string) any {
				return map[string]any{"tags": []map[string]string{}}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "untag resource - missing ARN",
			action: "UntagResource",
			setup:  nil,
			input: func(_ string) any {
				return map[string]any{"tagKeys": []string{}}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list tags - missing ARN",
			action:     "ListTagsForResource",
			setup:      nil,
			input:      func(_ string) any { return map[string]any{} },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(h)
			}

			rec := doRequest(t, h, tt.action, tt.input(arn))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource_WebhookARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler) string
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "pipeline ARN returns tags",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(
					context.Background(),
					samplePipeline("tags-pl"),
					map[string]string{"Env": "prod"},
				)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "unknown ARN returns PipelineNotFoundException",
			setup: func(_ *codepipeline.Handler) string {
				return "arn:aws:codepipeline:us-east-1:000000000000:nonexistent"
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "PipelineNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": resourceARN})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantType, out["__type"])
			}
		})
	}
}

// --------------------------------------------------------------------------
// #26 DisableStageTransition: stage existence validation
// --------------------------------------------------------------------------

func TestSortedListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("tag-pl"), map[string]string{
		"zzz": "last", "aaa": "first", "mmm": "mid",
	})
	require.NoError(t, err)

	// Get the ARN by listing pipelines.
	summaries := h.Backend.ListPipelines(context.Background())
	require.Len(t, summaries, 1)
	pipelineARN := summaries[0].PipelineArn
	require.NotEmpty(t, pipelineARN)

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": pipelineARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tags, ok := out["tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 3)

	keys := make([]string, len(tags))
	for i, tag := range tags {
		keys[i] = tag.(map[string]any)["key"].(string)
	}
	assert.Equal(t, []string{"aaa", "mmm", "zzz"}, keys)
}

func TestListTagsForResource_EmptySlice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(context.Background(), samplePipeline("notag-pl"), nil)
	require.NoError(t, err)

	summaries := h.Backend.ListPipelines(context.Background())
	pipelineARN := summaries[0].PipelineArn

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": pipelineARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tags := out["tags"]
	assert.NotNil(t, tags)
}

func TestWebhookTagging(t *testing.T) {
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
