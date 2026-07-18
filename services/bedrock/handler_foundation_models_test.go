package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Foundation model tests --- //nolint:godot // existing issue.
func TestHandler_ListFoundationModels(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	summaries := out["modelSummaries"].([]any)
	assert.NotEmpty(t, summaries)
}

func TestHandler_GetFoundationModel(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name       string
		modelID    string
		wantStatus int
	}{
		{
			name:       "existing model",
			modelID:    "amazon.titan-text-express-v1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent model",
			modelID:    "nonexistent-model",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/foundation-models/"+tt.modelID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotNil(t, out["modelDetails"])
			}
		})
	}
}

func TestHandler_ListFoundationModelsPagination(t *testing.T) { //nolint:paralleltest // existing issue.
	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	// Append enough models to exceed the page size (bedrockDefaultPageSize=100).
	extra := make([]*bedrock.FoundationModelSummary, 100)
	for i := range 100 {
		extra[i] = &bedrock.FoundationModelSummary{
			ModelID:      fmt.Sprintf("test.model-%04d", i),
			ModelName:    fmt.Sprintf("Test Model %04d", i),
			ProviderName: "TestProvider",
		}
	}
	b.AppendFoundationModelsForTest(extra)

	h := bedrock.NewHandler(b)

	// First page.
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	models := out["modelSummaries"].([]any)
	assert.Len(t, models, 100)
	nextToken, ok := out["nextToken"].(string)
	require.True(t, ok, "nextToken should be present")
	assert.NotEmpty(t, nextToken)

	// Second page.
	rec2 := doRequest(t, h, http.MethodGet, "/foundation-models?nextToken="+url.QueryEscape(nextToken), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)

	// Default backend seeds 5 models; we added 100, so total is 105.
	models2 := out2["modelSummaries"].([]any)
	assert.Len(t, models2, 5)
	assert.Empty(t, out2["nextToken"])
}

func TestBatch1_ListFoundationModels_InferenceTypesSupported(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	summaries := out["modelSummaries"].([]any)
	require.NotEmpty(t, summaries)

	// Every seeded model should have inferenceTypesSupported.
	for i, raw := range summaries {
		m := raw.(map[string]any)
		types, ok := m["inferenceTypesSupported"]
		assert.True(t, ok, "model[%d] missing inferenceTypesSupported", i)
		assert.NotEmpty(t, types, "model[%d] inferenceTypesSupported should not be empty", i)
	}
}

func TestBatch1_GetFoundationModel_InferenceTypesSupported(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-text-express-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	types := details["inferenceTypesSupported"].([]any)
	assert.Contains(t, typesAsStrings(types), "ON_DEMAND")
	assert.Contains(t, typesAsStrings(types), "PROVISIONED")

	customizations := details["customizationsSupported"].([]any)
	assert.Contains(t, typesAsStrings(customizations), "FINE_TUNING")

	streaming, ok := details["responseStreamingSupported"].(bool)
	assert.True(t, ok, "responseStreamingSupported should be bool")
	assert.True(t, streaming)
}

func TestBatch1_GetFoundationModel_EmbeddingModel_InferenceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-embed-text-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	types := typesAsStrings(details["inferenceTypesSupported"].([]any))
	assert.Contains(t, types, "ON_DEMAND")
	assert.NotContains(t, types, "PROVISIONED", "embedding model should not support PROVISIONED")

	streaming := details["responseStreamingSupported"].(bool)
	assert.False(t, streaming, "embedding model should not support streaming")
}

func TestBatch1_GetFoundationModel_Claude3Sonnet_ImageInputModality(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/foundation-models/anthropic.claude-3-sonnet-20240229-v1:0",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	inputMods := typesAsStrings(details["inputModalities"].([]any))
	assert.Contains(t, inputMods, "TEXT")
	assert.Contains(t, inputMods, "IMAGE")
}

func TestBatch1_GetFoundationModel_Llama3_InferenceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/meta.llama3-8b-instruct-v1:0", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)

	types := typesAsStrings(details["inferenceTypesSupported"].([]any))
	assert.Contains(t, types, "ON_DEMAND")

	customizations := typesAsStrings(details["customizationsSupported"].([]any))
	assert.Contains(t, customizations, "FINE_TUNING")
}

func TestBatch1_FoundationModel_ResponseStreamingSupported_Bool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Titan text supports streaming.
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-text-express-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	details := out["modelDetails"].(map[string]any)
	rawBytes, err := json.Marshal(details)
	require.NoError(t, err)

	var typed struct {
		InferenceTypesSupported    []string `json:"inferenceTypesSupported"`
		ResponseStreamingSupported bool     `json:"responseStreamingSupported"`
	}
	require.NoError(t, json.Unmarshal(rawBytes, &typed))
	assert.True(t, typed.ResponseStreamingSupported)
	assert.NotEmpty(t, typed.InferenceTypesSupported)

	// Embedding model does not support streaming.
	rec2 := doRequest(t, h, http.MethodGet, "/foundation-models/amazon.titan-embed-text-v1", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	details2 := out2["modelDetails"].(map[string]any)
	rawBytes2, err := json.Marshal(details2)
	require.NoError(t, err)

	var typed2 struct {
		ResponseStreamingSupported bool `json:"responseStreamingSupported"`
	}
	require.NoError(t, json.Unmarshal(rawBytes2, &typed2))
	assert.False(t, typed2.ResponseStreamingSupported)
}

func TestAccuracy_FoundationModel_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models/nonexistent.model-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_FoundationModel_ListIsNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "foundation models seeded on init"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["modelSummaries"].([]any)
			assert.NotEmpty(t, summaries, "foundation models must be seeded on init")
		})
	}
}

func TestAccuracy_FoundationModel_FieldsPresentOnList(t *testing.T) {
	t.Parallel()

	requiredFields := []string{
		"modelId",
		"modelName",
		"providerName",
		"inferenceTypesSupported",
		"inputModalities",
		"outputModalities",
	}

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	summaries := out["modelSummaries"].([]any)
	require.NotEmpty(t, summaries)

	for _, field := range requiredFields {
		for i, raw := range summaries {
			m := raw.(map[string]any)
			_, present := m[field]
			assert.True(t, present, "model[%d] missing field %q", i, field)
		}
	}
}

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
