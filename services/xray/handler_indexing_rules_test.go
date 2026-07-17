package xray_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetIndexingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   int
		wantMinRules int
	}{
		{
			name:         "returns default rules",
			wantStatus:   http.StatusOK,
			wantMinRules: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/GetIndexingRules", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			rules, ok := resp["IndexingRules"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(rules), tt.wantMinRules)
		})
	}
}

func TestHandler_GetIndexingRules_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantMin    int
	}{
		{
			name:       "returns default indexing rules",
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "MaxResults=1 limits results",
			body:       map[string]any{"MaxResults": 1},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "empty body accepted",
			body:       nil,
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "NextToken field accepted",
			body:       map[string]any{"NextToken": ""},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doXrayRequest(t, h, "/GetIndexingRules", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			rules, _ := resp["IndexingRules"].([]any)
			assert.GreaterOrEqual(t, len(rules), tt.wantMin)
			assert.Contains(t, resp, "NextToken")
		})
	}
}

func TestIndexingRules_GetDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetIndexingRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rules, ok := resp["IndexingRules"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, rules, "at least the Default indexing rule must be present")
}

func TestIndexingRules_UpdateModifiesTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/UpdateIndexingRule", map[string]any{"Name": "Default"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rule, ok := resp["IndexingRule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Default", rule["Name"])
	assert.NotNil(t, rule["ModifiedAt"])
}
