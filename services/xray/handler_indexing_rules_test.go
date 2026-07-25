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
	require.NotNil(t, rule["ModifiedAt"])
	// ModifiedAt must be an epoch-seconds JSON number, not an RFC3339 string --
	// guards against a real bug found during parity audit where this specific
	// handler hand-built its response with json.Marshal of a raw time.Time.
	_, isFloat := rule["ModifiedAt"].(float64)
	assert.True(t, isFloat, "ModifiedAt must be a JSON number (epoch seconds), got %T", rule["ModifiedAt"])
}

// TestIndexingRules_UpdateAppliesProbabilisticRule guards against a real stub-class
// bug found during parity audit: UpdateIndexingRule ignored the request's Rule field
// entirely (the actual point of the operation -- changing the sampling percentage)
// and only ever bumped ModifiedAt.
func TestIndexingRules_UpdateAppliesProbabilisticRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/UpdateIndexingRule", map[string]any{
		"Name": "Default",
		"Rule": map[string]any{
			"Probabilistic": map[string]any{
				"DesiredSamplingPercentage": 42.5,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rule, ok := resp["IndexingRule"].(map[string]any)
	require.True(t, ok)

	ruleField, ok := rule["Rule"].(map[string]any)
	require.True(t, ok, "expected Rule field in response")

	probabilistic, ok := ruleField["Probabilistic"].(map[string]any)
	require.True(t, ok, "expected Rule.Probabilistic field in response")
	assert.InDelta(t, 42.5, probabilistic["DesiredSamplingPercentage"], 0.001)
	assert.InDelta(t, 42.5, probabilistic["ActualSamplingPercentage"], 0.001)

	// GetIndexingRules must reflect the same change.
	getRec := doXrayRequest(t, h, "/GetIndexingRules", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	rules, ok := getResp["IndexingRules"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, rules)

	found := false
	for _, r := range rules {
		rm, rmOK := r.(map[string]any)
		require.True(t, rmOK)
		if rm["Name"] != "Default" {
			continue
		}
		found = true
		rf, rfOK := rm["Rule"].(map[string]any)
		require.True(t, rfOK)
		p, pOK := rf["Probabilistic"].(map[string]any)
		require.True(t, pOK)
		assert.InDelta(t, 42.5, p["DesiredSamplingPercentage"], 0.001)
	}
	assert.True(t, found, "Default rule must be present in GetIndexingRules")
}

func TestHandler_UpdateIndexingRule_NotFoundReturnsResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/UpdateIndexingRule", map[string]any{"Name": "does-not-exist"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// UpdateIndexingRule's modeled error set uses ResourceNotFoundException, unlike
	// most other X-Ray not-found cases (which use InvalidRequestException).
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}
