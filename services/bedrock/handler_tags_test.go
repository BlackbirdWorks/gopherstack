package bedrock_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func TestHandler_Tags_NotFound(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	t.Run("list tags for nonexistent resource", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
			"resourceARN": "arn:aws:bedrock:us-east-1:000000000000:guardrail/nonexistent",
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("tag nonexistent resource", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, http.MethodPost, "/tagResource", map[string]any{
			"resourceARN": "arn:aws:bedrock:us-east-1:000000000000:guardrail/nonexistent",
			"tags": []map[string]string{
				{"key": "k", "value": "v"},
			},
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandler_UntagResource_AllTypes(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup  func(*bedrock.Handler) string
		name   string
		tagKey string
	}{
		{
			name:   "untag evaluation job",
			tagKey: "project",
			setup: func(h *bedrock.Handler) string {
				job, err := h.Backend.CreateEvaluationJob("untag-job",
					[]bedrock.Tag{{Key: "project", Value: "x"}})
				require.NoError(t, err)

				return job.JobArn
			},
		},
		{
			name:   "untag automated reasoning policy",
			tagKey: "team",
			setup: func(h *bedrock.Handler) string {
				p, err := h.Backend.CreateAutomatedReasoningPolicy("untag-policy", "",
					[]bedrock.Tag{{Key: "team", Value: "x"}})
				require.NoError(t, err)

				return p.PolicyArn
			},
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, http.MethodPost, "/untagResource", map[string]any{
				"resourceARN": resourceARN,
				"tagKeys":     []string{tt.tagKey},
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
				"resourceARN": resourceARN,
			})
			assert.Equal(t, http.StatusOK, rec2.Code)

			var tagsOut map[string]any
			mustUnmarshal(t, rec2, &tagsOut)
			assert.Empty(t, tagsOut["tags"].([]any))
		})
	}
}

func TestHandler_TagsMergedSorted(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	g, err := h.Backend.CreateGuardrail("sorted-tags-guardrail", "", "", "", nil)
	require.NoError(t, err)

	// Add tags in non-alphabetical order.
	rec := doRequest(t, h, http.MethodPost, "/tagResource", map[string]any{
		"resourceARN": g.GuardrailArn,
		"tags": []map[string]string{
			{"key": "z-last", "value": "3"},
			{"key": "a-first", "value": "1"},
			{"key": "m-middle", "value": "2"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": g.GuardrailArn,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	tags := tagsOut["tags"].([]any)
	require.Len(t, tags, 3)

	// Verify sorted by key.
	assert.Equal(t, "a-first", tags[0].(map[string]any)["key"])
	assert.Equal(t, "m-middle", tags[1].(map[string]any)["key"])
	assert.Equal(t, "z-last", tags[2].(map[string]any)["key"])
}
