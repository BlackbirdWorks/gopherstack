package bedrock_test

// parity_a_test.go — §A parity fixes: foundation model ARN format, modelLifecycle field,
// and GetFoundationModel lookup by ARN.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_FoundationModel_ARNIncludesRegion verifies that foundation model ARNs contain
// the region and omit the account ID, matching real AWS format:
// arn:aws:bedrock:{region}::foundation-model/{modelId}.
func TestParity_FoundationModel_ARNIncludesRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{"titan_text", "amazon.titan-text-express-v1"},
		{"claude_v2", "anthropic.claude-v2"},
		{"llama3", "meta.llama3-8b-instruct-v1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/foundation-models/"+tt.modelID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ModelDetails struct {
					ModelArn string `json:"modelArn"`
					ModelID  string `json:"modelId"`
				} `json:"modelDetails"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arn := resp.ModelDetails.ModelArn
			assert.True(t, strings.HasPrefix(arn, "arn:aws:bedrock:us-east-1::foundation-model/"),
				"foundation model ARN must be arn:aws:bedrock:{region}::foundation-model/... but got %q", arn)
			assert.True(t, strings.HasSuffix(arn, tt.modelID),
				"foundation model ARN must end with modelId %q but got %q", tt.modelID, arn)
			assert.NotContains(t, arn, "000000000000",
				"foundation model ARN must NOT contain account ID but got %q", arn)
		})
	}
}

// TestParity_ListFoundationModels_ARNFormat verifies that all models returned by
// ListFoundationModels have correctly formatted ARNs (region, no account ID).
func TestParity_ListFoundationModels_ARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ModelSummaries []struct {
			ModelArn string `json:"modelArn"`
		} `json:"modelSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.ModelSummaries, "ListFoundationModels must return at least one model")

	for _, m := range resp.ModelSummaries {
		assert.True(t, strings.HasPrefix(m.ModelArn, "arn:aws:bedrock:us-east-1::foundation-model/"),
			"ARN must have region and no account ID, got %q", m.ModelArn)
		assert.NotContains(t, m.ModelArn, "000000000000",
			"ARN must not contain account ID, got %q", m.ModelArn)
	}
}

// TestParity_FoundationModel_ModelLifecyclePresent verifies that GetFoundationModel includes
// the modelLifecycle field with status ACTIVE, matching real AWS behavior.
func TestParity_FoundationModel_ModelLifecyclePresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{"titan_text", "amazon.titan-text-express-v1"},
		{"claude_sonnet", "anthropic.claude-3-sonnet-20240229-v1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/foundation-models/"+tt.modelID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ModelDetails struct {
					ModelLifecycle struct {
						Status string `json:"status"`
					} `json:"modelLifecycle"`
				} `json:"modelDetails"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			assert.Equal(t, "ACTIVE", resp.ModelDetails.ModelLifecycle.Status,
				"modelLifecycle.status must be ACTIVE for available models")
		})
	}
}

// TestParity_ListFoundationModels_ModelLifecyclePresent verifies that all models returned
// by ListFoundationModels include modelLifecycle, matching real AWS behavior.
func TestParity_ListFoundationModels_ModelLifecyclePresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ModelSummaries []struct {
			ModelLifecycle *struct {
				Status string `json:"status"`
			} `json:"modelLifecycle"`
			ModelID string `json:"modelId"`
		} `json:"modelSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.ModelSummaries)

	for _, m := range resp.ModelSummaries {
		require.NotNil(t, m.ModelLifecycle,
			"model %q must have modelLifecycle field", m.ModelID)
		assert.Equal(t, "ACTIVE", m.ModelLifecycle.Status,
			"model %q modelLifecycle.status must be ACTIVE", m.ModelID)
	}
}

// TestParity_GetFoundationModel_ByARN verifies that GetFoundationModel accepts a full ARN
// as the model identifier, matching real AWS behavior.
func TestParity_GetFoundationModel_ByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{"titan_text", "amazon.titan-text-express-v1"},
		{"titan_embed", "amazon.titan-embed-text-v1"},
		{"claude_v2", "anthropic.claude-v2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			expectedARN := "arn:aws:bedrock:us-east-1::foundation-model/" + tt.modelID

			// Look up by ARN (URL-encoded).
			rec := doRequest(t, h, http.MethodGet, "/foundation-models/"+expectedARN, nil)
			require.Equal(t, http.StatusOK, rec.Code,
				"GetFoundationModel by ARN must return 200")

			var resp struct {
				ModelDetails struct {
					ModelID  string `json:"modelId"`
					ModelArn string `json:"modelArn"`
				} `json:"modelDetails"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.modelID, resp.ModelDetails.ModelID,
				"GetFoundationModel by ARN must return the correct model")
			assert.Equal(t, expectedARN, resp.ModelDetails.ModelArn,
				"returned ARN must match the looked-up ARN")
		})
	}
}

// TestParity_GetFoundationModel_NotFound verifies GetFoundationModel returns 404 for
// unknown model IDs and ARNs, with ResourceNotFoundException error type.
func TestParity_GetFoundationModel_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{"unknown_id", "unknown.model-v1"},
		{"unknown_arn", "arn:aws:bedrock:us-east-1::foundation-model/unknown.model-v1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/foundation-models/"+tt.id, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			errType, _ := resp["__type"].(string)
			assert.Contains(t, errType, "ResourceNotFoundException")
		})
	}
}
