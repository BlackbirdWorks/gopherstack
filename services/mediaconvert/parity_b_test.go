package mediaconvert_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildNestedMap(depth int, leafValue string) map[string]any {
	if depth == 0 {
		return map[string]any{"leaf": leafValue}
	}

	return map[string]any{"nested": buildNestedMap(depth-1, leafValue)}
}

// extractLeaf drills into nested maps following "nested" keys until it finds "leaf".
func extractLeaf(m map[string]any) (string, bool) {
	cur := m
	for {
		if leaf, ok := cur["leaf"]; ok {
			s, isStr := leaf.(string)

			return s, isStr
		}

		next, ok := cur["nested"]
		if !ok {
			return "", false
		}

		nextMap, ok := next.(map[string]any)
		if !ok {
			return "", false
		}

		cur = nextMap
	}
}

func TestParity_DeepCloneValueAt_PreservesDeepNesting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		leafValue string
		depth     int
	}{
		{
			name:      "depth_20_preserved",
			depth:     20,
			leafValue: "value-at-20",
		},
		{
			name:      "depth_25_preserved",
			depth:     25,
			leafValue: "value-at-25",
		},
		{
			name:      "depth_50_preserved",
			depth:     50,
			leafValue: "value-at-50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			nestedSettings := buildNestedMap(tt.depth, tt.leafValue)

			// Create job with deeply nested settings.
			rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
				"role":     fmt.Sprintf("arn:aws:iam::%s:role/MediaConvert_Role", testAccountID),
				"settings": nestedSettings,
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
			jobData, ok := createResp["job"].(map[string]any)
			require.True(t, ok)
			jobID, _ := jobData["id"].(string)
			require.NotEmpty(t, jobID)

			// Retrieve via list and find the job.
			listRec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobs", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listResp))
			jobs, ok := listResp["jobs"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, jobs)

			// Find our job and check settings.
			var found map[string]any
			for _, j := range jobs {
				jm, isMap := j.(map[string]any)
				if !isMap {
					continue
				}

				if jm["id"] == jobID {
					found = jm

					break
				}
			}
			require.NotNil(t, found, "job %s not found in list", jobID)

			settings, ok := found["settings"].(map[string]any)
			require.True(t, ok, "settings field missing or wrong type")

			got, ok := extractLeaf(settings)
			assert.True(t, ok, "leaf value not found in nested settings")
			assert.Equal(t, tt.leafValue, got)
		})
	}
}
