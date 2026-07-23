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

// TestAccuracy_FoundationModelAgreementOffers_ListRequiresModelID locks in the
// parity fix: ListFoundationModelAgreementOffers is a per-model catalog lookup
// keyed by a required "{modelId}" path parameter (real AWS path:
// "/list-foundation-model-agreement-offers/{modelId}"), NOT a list of
// already-created agreements -- gopherstack previously served it from
// "/foundation-model-agreement-offers" (no modelId) and returned whatever
// agreements the account had created via CreateFoundationModelAgreement, a
// different resource entirely.
func TestAccuracy_FoundationModelAgreementOffers_ListRequiresModelID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet,
		"/list-foundation-model-agreement-offers/amazon.titan-text-express-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "amazon.titan-text-express-v1", out["modelId"])

	offers, _ := out["offers"].([]any)
	require.Len(t, offers, 1)

	offer := offers[0].(map[string]any)
	assert.NotEmpty(t, offer["offerToken"])
	assert.NotEmpty(t, offer["offerId"])
	assert.NotEmpty(t, offer["termDetails"])

	// The previous invented path must no longer route.
	recOld := doRequest(t, h, http.MethodGet, "/foundation-model-agreement-offers", nil)
	assert.Equal(t, http.StatusNotFound, recOld.Code)
}

// TestAccuracy_FoundationModelAgreement_CreateAndDelete locks in the parity
// fix for DeleteFoundationModelAgreement: real AWS uses POST
// "/delete-foundation-model-agreement" with modelId in the JSON body, not
// DELETE with modelId as a path suffix.
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
			recDel := doRequest(t, h, http.MethodPost,
				"/delete-foundation-model-agreement", map[string]any{"modelId": tt.modelID})
			assert.Equal(t, http.StatusOK, recDel.Code)

			// Delete again — not found.
			recDel2 := doRequest(t, h, http.MethodPost,
				"/delete-foundation-model-agreement", map[string]any{"modelId": tt.modelID})
			assert.Equal(t, http.StatusNotFound, recDel2.Code)

			// The previous invented DELETE-with-path-suffix route must no longer work.
			recOldPath := doRequest(t, h, http.MethodDelete,
				"/delete-foundation-model-agreement/"+url.PathEscape(tt.modelID), nil)
			assert.Equal(t, http.StatusNotFound, recOldPath.Code)
		})
	}
}

// TestAccuracy_FoundationModelAgreement_DeleteMissingModelIDRejected locks in
// that a body without modelId is a ValidationException, not a silent no-op
// (gopherstack's old DELETE-path-param version treated an empty ID as a
// successful no-op).
func TestAccuracy_FoundationModelAgreement_DeleteMissingModelIDRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/delete-foundation-model-agreement", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_GetFoundationModelAvailability locks in the full real-shape
// response: authorizationStatus, entitlementAvailability, modelId, and
// regionAvailability are all required GetFoundationModelAvailabilityOutput
// fields alongside agreementAvailability -- gopherstack previously returned
// only agreementAvailability, silently zero-valuing the rest for any client
// that inspects them.
func TestHandler_GetFoundationModelAvailability(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/foundation-model-availability/amazon.titan-text-express-v1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	assert.Equal(t, "amazon.titan-text-express-v1", out["modelId"])
	assert.NotEmpty(t, out["authorizationStatus"])
	assert.NotEmpty(t, out["entitlementAvailability"])
	assert.NotEmpty(t, out["regionAvailability"])

	agreementAvailability := out["agreementAvailability"].(map[string]any)
	assert.Equal(t, "AVAILABLE", agreementAvailability["status"])
}
