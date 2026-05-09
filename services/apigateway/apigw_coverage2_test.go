package apigateway_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func newAPIGWHandler() *apigateway.Handler {
	return apigateway.NewHandler(apigateway.NewInMemoryBackend())
}

// TestAPIGW_APIKeys covers CreateAPIKey, GetApiKey, GetApiKeys, UpdateApiKey, DeleteApiKey.
func TestAPIGW_APIKeys(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// CreateAPIKey via REST POST /apikeys.
	rec := restRequest(t, h, http.MethodPost, "/apikeys", `{"name":"test-key","enabled":true}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx for CreateAPIKey")

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	keyID, _ := createResp["id"].(string)
	require.NotEmpty(t, keyID)

	// GetApiKey.
	rec = restRequest(t, h, http.MethodGet, "/apikeys/"+keyID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetApiKeys.
	rec = restRequest(t, h, http.MethodGet, "/apikeys", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// UpdateApiKey.
	rec = restRequest(
		t,
		h,
		http.MethodPatch,
		"/apikeys/"+keyID,
		`[{"op":"replace","path":"/description","value":"updated"}]`,
	)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteApiKey.
	rec = restRequest(t, h, http.MethodDelete, "/apikeys/"+keyID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

// TestAPIGW_DomainNames covers CreateDomainName, GetDomainName, GetDomainNames,
// DeleteDomainName, CreateBasePathMapping, GetBasePathMapping, GetBasePathMappings,
// DeleteBasePathMapping.
func TestAPIGW_DomainNames(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// Create a rest API for base path mappings.
	rec := postWithHandler(t, h, nil, "CreateRestApi", `{"name":"dn-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID, _ := apiResp["id"].(string)
	require.NotEmpty(t, apiID)

	// CreateDomainName.
	rec = restRequest(t, h, http.MethodPost, "/domainnames",
		`{"domainName":"api.example.com","certificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/abc"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetDomainName.
	rec = restRequest(t, h, http.MethodGet, "/domainnames/api.example.com", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetDomainNames.
	rec = restRequest(t, h, http.MethodGet, "/domainnames", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// CreateBasePathMapping.
	rec = restRequest(t, h, http.MethodPost, "/domainnames/api.example.com/basepathmappings",
		`{"basePath":"v1","restApiId":"`+apiID+`","stage":"prod"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetBasePathMappings.
	rec = restRequest(t, h, http.MethodGet, "/domainnames/api.example.com/basepathmappings", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetBasePathMapping.
	rec = restRequest(t, h, http.MethodGet, "/domainnames/api.example.com/basepathmappings/v1", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteBasePathMapping.
	rec = restRequest(t, h, http.MethodDelete, "/domainnames/api.example.com/basepathmappings/v1", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteDomainName.
	rec = restRequest(t, h, http.MethodDelete, "/domainnames/api.example.com", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

// TestAPIGW_UsagePlans covers CreateUsagePlan, GetUsagePlan, GetUsagePlans, UpdateUsagePlan,
// DeleteUsagePlan, CreateUsagePlanKey, GetUsagePlanKey, GetUsagePlanKeys, DeleteUsagePlanKey.
func TestAPIGW_UsagePlans(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// CreateApiKey first for key association.
	rec := restRequest(t, h, http.MethodPost, "/apikeys", `{"name":"plan-key","enabled":true}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	var keyResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&keyResp))
	keyID, _ := keyResp["id"].(string)
	require.NotEmpty(t, keyID)

	// CreateUsagePlan via X-Amz-Target (using same handler h).
	rec = postWithHandler(
		t,
		h,
		nil,
		"CreateUsagePlan",
		`{"name":"test-plan","throttle":{"burstLimit":100,"rateLimit":50}}`,
	)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	var planResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&planResp))
	planID, _ := planResp["id"].(string)
	require.NotEmpty(t, planID)

	// GetUsagePlan via REST.
	rec = restRequest(t, h, http.MethodGet, "/usageplans/"+planID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetUsagePlans via REST.
	rec = restRequest(t, h, http.MethodGet, "/usageplans", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// UpdateUsagePlan.
	rec = restRequest(t, h, http.MethodPatch, "/usageplans/"+planID,
		`[{"op":"replace","path":"/name","value":"updated-plan"}]`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// CreateUsagePlanKey.
	rec = restRequest(t, h, http.MethodPost, "/usageplans/"+planID+"/keys",
		`{"keyId":"`+keyID+`","keyType":"API_KEY"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetUsagePlanKeys.
	rec = restRequest(t, h, http.MethodGet, "/usageplans/"+planID+"/keys", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetUsagePlanKey.
	rec = restRequest(t, h, http.MethodGet, "/usageplans/"+planID+"/keys/"+keyID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteUsagePlanKey.
	rec = restRequest(t, h, http.MethodDelete, "/usageplans/"+planID+"/keys/"+keyID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteUsagePlan.
	rec = restRequest(t, h, http.MethodDelete, "/usageplans/"+planID, "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

// TestAPIGW_Models covers GetModel, GetModels, CreateModel, DeleteModel.
func TestAPIGW_Models(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// Create a REST API.
	rec := postWithHandler(t, h, nil, "CreateRestApi", `{"name":"model-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID, _ := apiResp["id"].(string)
	require.NotEmpty(t, apiID)

	// CreateModel via REST.
	rec = restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/models",
		`{"name":"MyModel","contentType":"application/json","schema":"{\"type\":\"object\"}"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetModels.
	rec = restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/models", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// GetModel.
	rec = restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/models/MyModel", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// DeleteModel.
	rec = restRequest(t, h, http.MethodDelete, "/restapis/"+apiID+"/models/MyModel", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

// TestAPIGW_Account covers GetAccount, UpdateAccount.
func TestAPIGW_Account(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// GetAccount.
	rec := restRequest(t, h, http.MethodGet, "/account", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// UpdateAccount.
	rec = restRequest(t, h, http.MethodPatch, "/account",
		`{"cloudwatchRoleArn":"arn:aws:iam::000000000000:role/test-role"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

// TestAPIGW_BackendReset covers Reset.
func TestAPIGW_BackendReset(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	b.Reset()
}
