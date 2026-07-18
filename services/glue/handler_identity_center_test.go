package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlueIdentityCenter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create config
	rec := doGlueRequest(t, h, "CreateGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get config
	rec = doGlueRequest(t, h, "GetGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update
	rec = doGlueRequest(t, h, "UpdateGlueIdentityCenterConfiguration", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doGlueRequest(t, h, "DeleteGlueIdentityCenterConfiguration", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGlueIdentityCenter_Stateful verifies IdentityCenter configuration lifecycle.
func TestGlueIdentityCenter_Stateful(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateGlueIdentityCenterConfiguration", map[string]any{
		"InstanceArn": "arn:aws:sso:::instance/ssoins-1234",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	getRec := doGlueRequest(t, h, "GetGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		InstanceArn string `json:"InstanceArn"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "arn:aws:sso:::instance/ssoins-1234", getOut.InstanceArn)

	updateRec := doGlueRequest(t, h, "UpdateGlueIdentityCenterConfiguration", map[string]any{
		"InstanceArn": "arn:aws:sso:::instance/ssoins-5678",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	deleteRec := doGlueRequest(t, h, "DeleteGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, deleteRec.Code)
}
