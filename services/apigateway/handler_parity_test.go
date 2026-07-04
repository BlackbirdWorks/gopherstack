package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// createParityAPI creates a REST API and returns its ID.
func createParityAPI(t *testing.T, h *apigateway.Handler, name string) string {
	t.Helper()

	rec := restRequest(t, h, http.MethodPost, "/restapis", fmt.Sprintf(`{"name":%q}`, name))
	require.Equal(t, http.StatusCreated, rec.Code, "create api: %s", rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	id, _ := out["id"].(string)
	require.NotEmpty(t, id)

	return id
}

// getRootResourceID fetches the root resource ID of a REST API.
func getRootResourceID(t *testing.T, h *apigateway.Handler, apiID string) string {
	t.Helper()

	rec := restRequest(t, h, http.MethodGet, fmt.Sprintf("/restapis/%s/resources", apiID), "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Items []map[string]any `json:"item"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	for _, r := range out.Items {
		if r["path"] == "/" {
			id, _ := r["id"].(string)

			return id
		}
	}

	t.Fatal("root resource not found")

	return ""
}

// TestParity_PutMethod_AuthTypeValidation verifies authorizationType validation.
// Real AWS returns BadRequestException for invalid authorizationType.
func TestParity_PutMethod_AuthTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		authType     string
		authorizerID string
		wantCode     int
	}{
		{
			name:     "NONE accepted",
			authType: "NONE",
			wantCode: http.StatusCreated,
		},
		{
			name:     "AWS_IAM accepted",
			authType: "AWS_IAM",
			wantCode: http.StatusCreated,
		},
		{
			name:     "invalid type rejected",
			authType: "INVALID",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty type rejected",
			authType: "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			apiID := createParityAPI(t, h, "auth-test-api")
			rootID := getRootResourceID(t, h, apiID)

			body := fmt.Sprintf(
				`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","authorizationType":%q}`,
				apiID, rootID, tt.authType,
			)
			rec := restRequest(t, h, http.MethodPut,
				fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", apiID, rootID), body)

			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestParity_PutMethod_CustomRequiresAuthorizerId verifies that CUSTOM and COGNITO_USER_POOLS
// authorizationType require an authorizerId. Real AWS returns BadRequestException otherwise.
func TestParity_PutMethod_CustomRequiresAuthorizerId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		authType     string
		authorizerID string
		wantCode     int
	}{
		{
			name:     "CUSTOM without authorizerId rejected",
			authType: "CUSTOM",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "COGNITO_USER_POOLS without authorizerId rejected",
			authType: "COGNITO_USER_POOLS",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			apiID := createParityAPI(t, h, "custom-auth-api")
			rootID := getRootResourceID(t, h, apiID)

			body := fmt.Sprintf(
				`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","authorizationType":%q}`,
				apiID, rootID, tt.authType,
			)
			rec := restRequest(t, h, http.MethodPut,
				fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", apiID, rootID), body)

			assert.Equal(t, tt.wantCode, rec.Code,
				"authorizationType %q without authorizerId must be rejected; body: %s",
				tt.authType, rec.Body.String())
		})
	}
}

// TestParity_PutMethod_RequestParametersRoundtrip verifies that RequestParameters are stored
// and returned. Real AWS preserves the requestParameters map on a method.
func TestParity_PutMethod_RequestParametersRoundtrip(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID := createParityAPI(t, h, "reqparam-api")
	rootID := getRootResourceID(t, h, apiID)

	body := fmt.Sprintf(
		`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","authorizationType":"NONE",`+
			`"requestParameters":{"method.request.header.X-Custom":true}}`,
		apiID, rootID,
	)
	rec := restRequest(t, h, http.MethodPut,
		fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", apiID, rootID), body)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	var method map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &method))

	params, _ := method["requestParameters"].(map[string]any)
	require.NotNil(t, params, "requestParameters must be present in response")
	assert.Equal(t, true, params["method.request.header.X-Custom"],
		"requestParameters must round-trip correctly")
}

// TestParity_PutIntegration_TypeValidation verifies that invalid integration types are rejected.
// Real AWS returns BadRequestException for unknown integration types.
func TestParity_PutIntegration_TypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		intType  string
		wantCode int
	}{
		{name: "MOCK accepted", intType: "MOCK", wantCode: http.StatusCreated},
		{name: "HTTP accepted", intType: "HTTP", wantCode: http.StatusCreated},
		{name: "HTTP_PROXY accepted", intType: "HTTP_PROXY", wantCode: http.StatusCreated},
		{name: "AWS_PROXY accepted", intType: "AWS_PROXY", wantCode: http.StatusCreated},
		{name: "invalid type rejected", intType: "FAKE", wantCode: http.StatusBadRequest},
		{name: "empty type rejected", intType: "", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			apiID := createParityAPI(t, h, "int-type-api")
			rootID := getRootResourceID(t, h, apiID)

			// First put the method.
			methodBody := fmt.Sprintf(
				`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","authorizationType":"NONE"}`,
				apiID, rootID,
			)
			rec := restRequest(t, h, http.MethodPut,
				fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", apiID, rootID), methodBody)
			require.Equal(t, http.StatusCreated, rec.Code)

			// Now put integration.
			uri := ""
			if tt.intType == "HTTP" || tt.intType == "HTTP_PROXY" {
				uri = "https://example.com"
			}

			intBody := fmt.Sprintf(
				`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","type":%q,"uri":%q}`,
				apiID, rootID, tt.intType, uri,
			)
			rec = restRequest(t, h, http.MethodPut,
				fmt.Sprintf("/restapis/%s/resources/%s/methods/GET/integration", apiID, rootID), intBody)

			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestParity_FlushStageCache_NotFound verifies that FlushStageCache returns 404 for unknown
// API or stage. Real AWS returns NotFoundException.
func TestParity_FlushStageCache_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apiID     string
		stageName string
		setup     bool
		wantCode  int
	}{
		{
			name:      "unknown API returns 404",
			apiID:     "nonexistent",
			stageName: "prod",
			setup:     false,
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "unknown stage returns 404",
			stageName: "nonexistent-stage",
			setup:     true,
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			apiID := tt.apiID
			if tt.setup {
				apiID = createParityAPI(t, h, "flush-test-api")
			}

			rec := restRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/restapis/%s/stages/%s/cache", apiID, tt.stageName), "")

			assert.Equal(t, tt.wantCode, rec.Code,
				"FlushStageCache on non-existent resource must return 404; body: %s", rec.Body.String())
		})
	}
}

// TestParity_DeleteDeployment_StageProtection verifies that deleting a deployment referenced
// by a stage returns an error. Real AWS returns BadRequestException in this case.
func TestParity_DeleteDeployment_StageProtection(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID := createParityAPI(t, h, "depl-protect-api")

	// Create a deployment via CreateDeployment.
	rec := restRequest(t, h, http.MethodPost,
		fmt.Sprintf("/restapis/%s/deployments", apiID),
		`{"stageName":"prod","description":"initial"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300,
		"create deployment: %d %s", rec.Code, rec.Body.String())

	var depl map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &depl))
	deplID, _ := depl["id"].(string)
	require.NotEmpty(t, deplID)

	// Attempt to delete the deployment — stage "prod" references it.
	rec = restRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/restapis/%s/deployments/%s", apiID, deplID), "")

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"deleting a deployment referenced by a stage must return 400; body: %s", rec.Body.String())
}

// TestParity_DeleteDeployment_Unreferenced verifies that a deployment not referenced by
// any stage can be deleted successfully.
func TestParity_DeleteDeployment_Unreferenced(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID := createParityAPI(t, h, "depl-unref-api")

	// Create a deployment with no stage.
	rec := restRequest(t, h, http.MethodPost,
		fmt.Sprintf("/restapis/%s/deployments", apiID),
		`{"description":"no stage"}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300,
		"create deployment: %d %s", rec.Code, rec.Body.String())

	var depl map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &depl))
	deplID, _ := depl["id"].(string)
	require.NotEmpty(t, deplID)

	// Delete succeeds — no stage references it.
	rec = restRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/restapis/%s/deployments/%s", apiID, deplID), "")

	assert.Equal(t, http.StatusNoContent, rec.Code,
		"deleting an unreferenced deployment must succeed; body: %s", rec.Body.String())
}

// TestParity_UpdateUsage_ValidatesUsagePlanExists verifies that UpdateUsage returns 404
// for an unknown usage plan. Real AWS returns NotFoundException.
func TestParity_UpdateUsage_ValidatesUsagePlanExists(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(t, h, http.MethodPatch,
		"/usageplans/nonexistent-plan/keys/somekey/usage",
		`{"patchOperations":[{"op":"replace","path":"/remaining","value":"100"}]}`,
	)

	assert.Equal(t, http.StatusNotFound, rec.Code,
		"UpdateUsage with unknown planId must return 404; body: %s", rec.Body.String())
}

// TestParity_UpdateUsage_ValidatesKeyExists verifies that UpdateUsage returns 404 for an
// unknown key. Real AWS returns NotFoundException.
func TestParity_UpdateUsage_ValidatesKeyExists(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// Create a usage plan.
	rec := restRequest(t, h, http.MethodPost, "/usageplans",
		`{"name":"test-plan","throttle":{"rateLimit":100,"burstLimit":50}}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "create plan: %s", rec.Body.String())

	var plan map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &plan))
	planID, _ := plan["id"].(string)
	require.NotEmpty(t, planID)

	rec = restRequest(t, h, http.MethodPatch,
		fmt.Sprintf("/usageplans/%s/keys/nonexistent-key/usage", planID),
		`{"patchOperations":[{"op":"replace","path":"/remaining","value":"100"}]}`,
	)

	assert.Equal(t, http.StatusNotFound, rec.Code,
		"UpdateUsage with unknown keyId must return 404; body: %s", rec.Body.String())
}
