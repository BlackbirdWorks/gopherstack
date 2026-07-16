package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTerms_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "terms-crud-pool")

	// Describe before create — returns empty terms
	rec := doCognitoRequest(t, h, "DescribeTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// List before create — empty
	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp struct {
		Terms []any `json:"Terms,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Terms)

	// Create
	rec = doCognitoRequest(t, h, "CreateTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after create — one entry
	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Terms, 1)

	// Update
	rec = doCognitoRequest(t, h, "UpdateTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doCognitoRequest(t, h, "DeleteTerms", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after delete — empty
	rec = doCognitoRequest(t, h, "ListTerms", map[string]any{"UserPoolId": poolID})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.Terms)
}

func TestTerms_InvalidPool(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateTerms", map[string]any{"UserPoolId": "bad-pool"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
