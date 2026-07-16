package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateFoundationModelAgreement(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
		wantModel  bool
	}{
		{
			name: "valid agreement",
			input: map[string]any{
				"modelId":    "anthropic.claude-v2",
				"offerToken": "token-abc123",
			},
			wantStatus: http.StatusOK,
			wantModel:  true,
		},
		{
			name: "missing model id",
			input: map[string]any{
				"modelId":    "",
				"offerToken": "token",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/create-foundation-model-agreement", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantModel {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Equal(t, "anthropic.claude-v2", out["modelId"])
			}
		})
	}
}

func TestAccuracy_FoundationModelAgreement_ListOffersEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/foundation-model-agreement-offers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	offers, _ := out["modelAgreementOffers"].([]any)
	// Initially no agreements — list should be empty or nil.
	assert.Empty(t, offers)
}

func TestAccuracy_FoundationModelAgreement_CreateAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "titan model agreement", modelID: "amazon.titan-text-express-v1"},
		{name: "claude model agreement", modelID: "anthropic.claude-3-sonnet-20240229-v1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			// Create agreement.
			rec := doRequest(t, h, http.MethodPost, "/create-foundation-model-agreement",
				map[string]any{"modelId": tt.modelID})
			require.Equal(t, http.StatusOK, rec.Code)

			// Delete agreement.
			recDel := doRequest(t, h, http.MethodDelete,
				"/delete-foundation-model-agreement/"+url.PathEscape(tt.modelID), nil)
			assert.Equal(t, http.StatusNoContent, recDel.Code)

			// Delete again — not found.
			recDel2 := doRequest(t, h, http.MethodDelete,
				"/delete-foundation-model-agreement/"+url.PathEscape(tt.modelID), nil)
			assert.Equal(t, http.StatusNotFound, recDel2.Code)
		})
	}
}

func TestAccuracy_UseCaseForModelAccess_PutAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		useCaseType string
		description string
	}{
		{
			name:        "business use case",
			useCaseType: "BUSINESS",
			description: "Using models for customer support",
		},
		{
			name:        "research use case",
			useCaseType: "RESEARCH",
			description: "Academic research on LLMs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			recPut := doRequest(
				t, h, http.MethodPut, "/usecase-for-model-access",
				map[string]any{
					"useCaseType":        tt.useCaseType,
					"useCaseDescription": tt.description,
				},
			)
			assert.Equal(t, http.StatusNoContent, recPut.Code)

			recGet := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, tt.useCaseType, out["useCaseType"])
			assert.Equal(t, tt.description, out["useCaseDescription"])
		})
	}
}

func TestAccuracy_UseCaseForModelAccess_TypeAndDescriptionRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		useCaseType string
		desc        string
		name        string
	}{
		{name: "research use case", useCaseType: "RESEARCH", desc: "academic research"},
		{name: "commercial use case", useCaseType: "COMMERCIAL", desc: "production service"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			putRec := doRequest(t, h, http.MethodPut, "/usecase-for-model-access",
				map[string]any{
					"useCaseType":        tt.useCaseType,
					"useCaseDescription": tt.desc,
				})
			require.Equal(t, http.StatusNoContent, putRec.Code)

			getRec := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
			assert.Equal(t, tt.useCaseType, out["useCaseType"])
			assert.Equal(t, tt.desc, out["useCaseDescription"])
		})
	}
}

func TestHandler_FoundationModelAgreement_ListAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// List before any agreements
	rec := doRequest(t, h, http.MethodGet, "/foundation-model-agreement-offers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["modelAgreementOffers"])

	// Create an agreement
	doRequest(t, h, http.MethodPost, "/create-foundation-model-agreement",
		map[string]any{"modelId": "amazon.titan-text-express-v1"})

	// List now has one
	rec2 := doRequest(t, h, http.MethodGet, "/foundation-model-agreement-offers", nil)
	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	assert.Len(t, out2["modelAgreementOffers"], 1)
}

func TestHandler_UseCaseForModelAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Get default
	rec := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["useCaseType"])

	// Put
	rec2 := doRequest(t, h, http.MethodPut, "/usecase-for-model-access",
		map[string]any{"useCaseType": "RESEARCH", "useCaseDescription": "ML research"})
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Get after put
	rec3 := doRequest(t, h, http.MethodGet, "/usecase-for-model-access", nil)
	var out3 map[string]any
	mustUnmarshal(t, rec3, &out3)
	assert.Equal(t, "RESEARCH", out3["useCaseType"])
	assert.Equal(t, "ML research", out3["useCaseDescription"])
}
