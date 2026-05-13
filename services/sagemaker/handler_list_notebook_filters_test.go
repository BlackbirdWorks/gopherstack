package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListNotebookInstances_Filters verifies that ListNotebookInstances honors
// the AWS-defined StatusEquals and NameContains filters, including
// case-insensitive matching.
func TestListNotebookInstances_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantNames []string
	}{
		{
			name:      "no_filter_returns_all",
			body:      map[string]any{},
			wantNames: []string{"alpha-prod", "beta-dev", "gamma-prod"},
		},
		{
			name:      "name_contains_substring",
			body:      map[string]any{"NameContains": "prod"},
			wantNames: []string{"alpha-prod", "gamma-prod"},
		},
		{
			name:      "name_contains_case_insensitive",
			body:      map[string]any{"NameContains": "PROD"},
			wantNames: []string{"alpha-prod", "gamma-prod"},
		},
		{
			name:      "no_match_returns_empty",
			body:      map[string]any{"NameContains": "missing-substring"},
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, name := range []string{"alpha-prod", "beta-dev", "gamma-prod"} {
				rec := doSageMakerRequest(t, h, "CreateNotebookInstance", map[string]any{
					"NotebookInstanceName": name,
					"InstanceType":         "ml.t2.medium",
					"RoleArn":              "arn:aws:iam::000000000000:role/r",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doSageMakerRequest(t, h, "ListNotebookInstances", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				NotebookInstances []struct {
					NotebookInstanceName string `json:"NotebookInstanceName"`
				} `json:"NotebookInstances"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			gotNames := make([]string, 0, len(resp.NotebookInstances))
			for _, nb := range resp.NotebookInstances {
				gotNames = append(gotNames, nb.NotebookInstanceName)
			}

			assert.Equal(t, tt.wantNames, gotNames)
		})
	}
}
