package emr_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestDescribeJobFlows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "jf-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create, jsonx.V1()))

	rec := doEMRRequest(t, h, "DescribeJobFlows", map[string]any{
		"JobFlowIds": []string{create.JobFlowID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		JobFlows []struct {
			JobFlowID string `json:"JobFlowId"`
			Name      string `json:"Name"`
		} `json:"JobFlows"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))
	require.Len(t, out.JobFlows, 1)
	assert.Equal(t, create.JobFlowID, out.JobFlows[0].JobFlowID)
	assert.Equal(t, "jf-cluster", out.JobFlows[0].Name)
}

func TestDescribeJobFlows_EmptyNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "empty backend returns JobFlows:[]",
			body: map[string]any{},
		},
		{
			name: "filter by nonexistent IDs returns JobFlows:[]",
			body: map[string]any{"JobFlowIds": []string{"j-DOESNOTEXIST"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doEMRRequest(t, h, "DescribeJobFlows", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw, jsonx.V1()))

			flows, hasKey := raw["JobFlows"]
			assert.True(t, hasKey, "DescribeJobFlows must include 'JobFlows' key")
			assert.IsType(t, []any{}, flows, "'JobFlows' must be [] not null when empty")
			assert.Empty(t, flows)
		})
	}
}
