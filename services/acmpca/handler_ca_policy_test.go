package acmpca_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Policy operations ----

func TestACMPCAHandler_PolicyOperations(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	// PutPolicy
	rec := doACMPCARequest(t, h, "PutPolicy", map[string]any{
		"ResourceArn": caARN,
		"Policy":      `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetPolicy
	rec = doACMPCARequest(t, h, "GetPolicy", map[string]any{
		"ResourceArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetPolicy - not found
	rec = doACMPCARequest(t, h, "GetPolicy", map[string]any{
		"ResourceArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// DeletePolicy
	rec = doACMPCARequest(t, h, "DeletePolicy", map[string]any{
		"ResourceArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DeletePolicy - not found
	rec = doACMPCARequest(t, h, "DeletePolicy", map[string]any{
		"ResourceArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestACMPCAHandler_PolicyLifecycle verifies the put/get/delete policy
// lifecycle via the handler dispatch path.
func TestACMPCAHandler_PolicyLifecycle(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)
	policy := `{"Version":"2012-10-17","Statement":[]}`

	putRec := doACMPCARequest(t, h, "PutPolicy", map[string]any{
		"Policy":      policy,
		"ResourceArn": caARN,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doACMPCARequest(t, h, "GetPolicy", map[string]any{
		"ResourceArn": caARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseACMPCAResponse(t, getRec)
	assert.Equal(t, policy, getResp["Policy"])

	deleteRec := doACMPCARequest(t, h, "DeletePolicy", map[string]any{
		"ResourceArn": caARN,
	})
	require.Equal(t, http.StatusOK, deleteRec.Code)
}

// TestACMPCAHandler_GetPolicy_RequiresResourceArn verifies that GetPolicy
// without a ResourceArn returns InvalidArnException, matching GetPolicy's
// own deserializeOpError.
func TestACMPCAHandler_GetPolicy_RequiresResourceArn(t *testing.T) {
	t.Parallel()

	rec := doACMPCARequest(t, newACMPCAHandler(), "GetPolicy", map[string]any{})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := parseACMPCAResponse(t, rec)
	assert.Equal(t, "InvalidArnException", resp["__type"])
}
