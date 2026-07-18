package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_WebAppCustomizationValidatesWebAppID verifies that the
// WebApp customization operations (Describe/Update/Delete) return 404
// when the WebAppId does not exist. The previous stub implementation
// accepted any (including non-existent) WebAppId and returned 200 with
// empty data, making it impossible to distinguish "bad ID" from "no customization".
func TestHandler_WebAppCustomizationValidatesWebAppID(t *testing.T) {
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

// TestHandler_WebAppCustomizationRoundTrip verifies the full lifecycle:
// create a WebApp, update its customization, describe it and see the values,
// then delete the customization.
func TestHandler_WebAppCustomizationRoundTrip(t *testing.T) {
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

// TestHandler_CreateWebAppReturnsWebAppID verifies response schema.
func TestHandler_CreateWebAppReturnsWebAppID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateWebApp", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["WebAppId"])
}

// TestHandler_DescribeWebAppIncludesArnTagsAndIdentityProvider verifies that
// DescribeWebApp returns the (required, per AWS) Arn field plus Tags and
// IdentityProviderDetails, all of which the backend already stores.
func TestHandler_DescribeWebAppIncludesArnTagsAndIdentityProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"Tags": []map[string]any{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	updateRec := doTransferRequest(t, h, "UpdateWebApp", map[string]any{
		"WebAppId": webAppID,
		"IdentityProviderDetails": map[string]any{
			"Role": "arn:aws:iam::123456789012:role/webapp-idp",
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	descRec := doTransferRequest(t, h, "DescribeWebApp", map[string]any{"WebAppId": webAppID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	webApp := descResp["WebApp"].(map[string]any)

	assert.Equal(t,
		"arn:aws:transfer:us-east-1:123456789012:webapp/"+webAppID,
		webApp["Arn"],
		"Arn is a required field on DescribedWebApp",
	)

	tags := webApp["Tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])

	idp := webApp["IdentityProviderDetails"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::123456789012:role/webapp-idp", idp["Role"])
}

// TestHandler_ListWebAppsIncludesArn verifies that ListWebApps returns the
// (required, per AWS) Arn field for each entry.
func TestHandler_ListWebAppsIncludesArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	listRec := doTransferRequest(t, h, "ListWebApps", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	webApps := listResp["WebApps"].([]any)
	require.Len(t, webApps, 1)
	item := webApps[0].(map[string]any)
	assert.Equal(t, "arn:aws:transfer:us-east-1:123456789012:webapp/"+webAppID, item["Arn"])
}

func TestHandler_CreateWebApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success no tags",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "success with tags",
			body: map[string]any{
				"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateWebApp", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["WebAppId"])
		})
	}
}

func TestHandler_DescribeWebApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-1234567890abcdef0",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	rec := doTransferRequest(t, h, "DescribeWebApp", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListWebApps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListWebApps", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteWebApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-delete",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	rec := doTransferRequest(t, h, "DeleteWebApp", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateWebApp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-update",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	rec := doTransferRequest(t, h, "UpdateWebApp", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_WebAppCustomization(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
		"IdentityProviderDetails": map[string]any{
			"IdentityCenterConfig": map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-custom",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webAppID := createResp["WebAppId"].(string)

	// DescribeWebAppCustomization
	descRec := doTransferRequest(t, h, "DescribeWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// UpdateWebAppCustomization
	updRec := doTransferRequest(t, h, "UpdateWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
		"Title":    "My Transfer Portal",
	})
	assert.Equal(t, http.StatusOK, updRec.Code)

	// DeleteWebAppCustomization
	delRec := doTransferRequest(t, h, "DeleteWebAppCustomization", map[string]any{
		"WebAppId": webAppID,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}
