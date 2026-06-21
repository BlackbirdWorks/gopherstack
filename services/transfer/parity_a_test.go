package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_WebAppCustomizationValidatesWebAppID verifies that the
// WebApp customization operations (Describe/Update/Delete) return 404
// when the WebAppId does not exist. The previous stub implementation
// accepted any (including non-existent) WebAppId and returned 200 with
// empty data, making it impossible to distinguish "bad ID" from "no customization".
func TestParity_WebAppCustomizationValidatesWebAppID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		operation string
	}{
		{
			operation: "DescribeWebAppCustomization",
			body:      map[string]any{"WebAppId": "webapp-nonexistent"},
		},
		{
			operation: "UpdateWebAppCustomization",
			body:      map[string]any{"WebAppId": "webapp-nonexistent", "Title": "My App"},
		},
		{
			operation: "DeleteWebAppCustomization",
			body:      map[string]any{"WebAppId": "webapp-nonexistent"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, tt.operation, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"%s on non-existent WebAppId must return 400 (ResourceNotFoundException)", tt.operation)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))

			errType, _ := errResp["__type"].(string)
			assert.Contains(t, errType, "ResourceNotFoundException",
				"%s must return ResourceNotFoundException for unknown WebAppId", tt.operation)
		})
	}
}

// TestParity_WebAppCustomizationRoundTrip verifies the full lifecycle:
// create a WebApp, update its customization, describe it and see the values,
// then delete the customization.
func TestParity_WebAppCustomizationRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityProviderType": "AWS_IAM_IDP",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	webAppID, _ := createResp["WebAppId"].(string)
	require.NotEmpty(t, webAppID)

	updateRec := doTransferRequest(t, h, "UpdateWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
		"Title":    "Parity Test App",
		"LogoFile": "bG9nbw==",
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateWebAppCustomization must succeed")

	describeRec := doTransferRequest(t, h, "DescribeWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
	})
	require.Equal(t, http.StatusOK, describeRec.Code, "DescribeWebAppCustomization must succeed")

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &descResp))

	customization, ok := descResp["WebAppCustomization"].(map[string]any)
	require.True(t, ok, "WebAppCustomization must be present in response")
	assert.Equal(t, "Parity Test App", customization["Title"],
		"Title must round-trip through Update→Describe")
	assert.Equal(t, "bG9nbw==", customization["LogoFile"],
		"LogoFile must round-trip through Update→Describe")

	deleteRec := doTransferRequest(t, h, "DeleteWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, deleteRec.Code, "DeleteWebAppCustomization must succeed on existing WebApp")
}
