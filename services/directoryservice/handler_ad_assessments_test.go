package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestADAssessments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start list describe delete cycle"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Start
			rec1 := doRequest(t, h, "StartADAssessment", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			assessID, _ := r1["AssessmentId"].(string)
			assert.NotEmpty(t, assessID)

			// List
			rec2 := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec2.Code)
			var r2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &r2))
			assessments, _ := r2["ADAssessments"].([]any)
			assert.Len(t, assessments, 1)

			// Describe
			rec3 := doRequest(t, h, "DescribeADAssessment", map[string]any{
				"DirectoryId":  dirID,
				"AssessmentId": assessID,
			})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			assessment, _ := r3["ADAssessment"].(map[string]any)
			assert.Equal(t, assessID, assessment["AssessmentId"])

			// Delete
			rec4 := doRequest(t, h, "DeleteADAssessment", map[string]any{
				"DirectoryId":  dirID,
				"AssessmentId": assessID,
			})
			assert.Equal(t, http.StatusOK, rec4.Code)

			// List after delete
			rec5 := doRequest(t, h, "ListADAssessments", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec5.Code)
			var r5 map[string]any
			require.NoError(t, json.Unmarshal(rec5.Body.Bytes(), &r5))
			assessments2, _ := r5["ADAssessments"].([]any)
			assert.Empty(t, assessments2)

			_ = tc
		})
	}
}
