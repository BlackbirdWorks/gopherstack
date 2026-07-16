package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ServicecatalogPortfolio_Toggle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assertStatus := func(want string) {
		rec := doSageMakerRequest(t, h, "GetSagemakerServicecatalogPortfolioStatus", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, want, out["Status"])
	}

	assertStatus("Disabled")

	rec := doSageMakerRequest(t, h, "EnableSagemakerServicecatalogPortfolio", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertStatus("Enabled")

	rec = doSageMakerRequest(t, h, "DisableSagemakerServicecatalogPortfolio", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertStatus("Disabled")
}

// ---------------------------------------------------------------------------
// ListResourceCatalogs
// ---------------------------------------------------------------------------

func TestHandler_ListResourceCatalogs_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListResourceCatalogs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out["ResourceCatalogs"])
}

// ---------------------------------------------------------------------------
// TrialComponent association extras
// ---------------------------------------------------------------------------
