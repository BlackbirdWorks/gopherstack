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

// TestGetApiKey_ValueHiddenByDefault verifies that GetApiKey omits
// the value field unless includeValue=true is specified, matching AWS behaviour.
// Previously the value was always returned.
func TestGetApiKey_ValueHiddenByDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		wantValue   bool
	}{
		{
			name:        "no_include_value_hides_value",
			queryString: "",
			wantValue:   false,
		},
		{
			name:        "include_value_false_hides_value",
			queryString: "?includeValue=false",
			wantValue:   false,
		},
		{
			name:        "include_value_true_returns_value",
			queryString: "?includeValue=true",
			wantValue:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			// Create a key so there is a value to retrieve.
			createRec := restRequest(t, h, http.MethodPost, "/apikeys",
				`{"name":"test-key","enabled":true,"value":"my-secret-key"}`)
			require.True(t, createRec.Code >= 200 && createRec.Code < 300)

			var createResp map[string]any
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createResp))
			keyID, _ := createResp["id"].(string)
			require.NotEmpty(t, keyID)

			rec := restRequest(t, h, http.MethodGet, "/apikeys/"+keyID+tt.queryString, "")
			require.True(t, rec.Code >= 200 && rec.Code < 300)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			if tt.wantValue {
				assert.NotEmpty(t, resp["value"], "value must be present when includeValue=true")
			} else {
				assert.Empty(t, resp["value"], "value must be absent when includeValue is not true")
			}
		})
	}
}

// TestGetApiKeys_ValueHiddenByDefault verifies that GetApiKeys omits
// the value field from all keys unless includeValue=true, matching AWS behaviour.
func TestGetApiKeys_ValueHiddenByDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queryString string
		wantValue   bool
	}{
		{
			name:        "no_include_value_hides_values",
			queryString: "",
			wantValue:   false,
		},
		{
			name: "include_value_true_returns_values",
			// Real wire key for the list op is "includeValues" (plural,
			// apigateway@v1.42.4 serializers.go:4106) -- distinct from the
			// singular "includeValue" GetApiKey (single-key op) uses
			// (serializers.go:4036). This test used to assert the singular
			// key against a handler that itself only read the singular key,
			// so it passed even though a real client sending "includeValues"
			// got nothing back.
			queryString: "?includeValues=true",
			wantValue:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createRec := restRequest(t, h, http.MethodPost, "/apikeys",
				`{"name":"keys-test-key","enabled":true,"value":"secret-value"}`)
			require.True(t, createRec.Code >= 200 && createRec.Code < 300)

			rec := restRequest(t, h, http.MethodGet, "/apikeys"+tt.queryString, "")
			require.True(t, rec.Code >= 200 && rec.Code < 300)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			items, ok := resp["item"].([]any)
			require.True(t, ok && len(items) > 0, "expected at least one key in response")

			firstKey, ok := items[0].(map[string]any)
			require.True(t, ok)

			if tt.wantValue {
				assert.NotEmpty(t, firstKey["value"], "value must be present when includeValue=true")
			} else {
				assert.Empty(t, firstKey["value"], "value must be absent when includeValue is not set")
			}
		})
	}
}

// TestPatchOps_UpdateApiKey verifies that JSON patch operations
// ([{"op":"replace","path":"/description","value":"x"}]) are applied when
// PATCH /apikeys/{id} is called, matching AWS REST API behaviour. Previously
// patch ops were silently dropped and the key was returned unchanged.
func TestPatchOps_UpdateApiKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		patchBody       string
		wantDescription string
	}{
		{
			name:            "replace_description_via_patch_op",
			patchBody:       `[{"op":"replace","path":"/description","value":"new description"}]`,
			wantDescription: "new description",
		},
		{
			name:            "add_description_via_patch_op",
			patchBody:       `[{"op":"add","path":"/description","value":"added description"}]`,
			wantDescription: "added description",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createRec := restRequest(t, h, http.MethodPost, "/apikeys",
				`{"name":"patch-key","enabled":true}`)
			require.True(t, createRec.Code >= 200 && createRec.Code < 300)

			var createResp map[string]any
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createResp))
			keyID, _ := createResp["id"].(string)
			require.NotEmpty(t, keyID)

			patchRec := restRequest(t, h, http.MethodPatch, "/apikeys/"+keyID, tt.patchBody)
			require.True(t, patchRec.Code >= 200 && patchRec.Code < 300)

			getKey, _ := b.GetAPIKey(keyID)
			assert.Equal(t, tt.wantDescription, getKey.Description,
				"patch op must update the description field")
		})
	}
}

// TestApiKey_CustomerID verifies that customerId (AWS Marketplace
// SaaS integration field, types.ApiKey.CustomerId / types.CreateApiKeyInput.CustomerId
// in the SDK) round-trips through CreateApiKey, GetApiKey, and the UpdateApiKey
// PATCH /customerId path (a "Supported" replace path per AWS's patch-operations
// reference). This field was previously entirely absent from gopherstack's
// APIKey/CreateAPIKeyInput/UpdateAPIKeyInput models, so it was silently dropped
// on create and unpatchable.
func TestApiKey_CustomerID(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(b)

	createRec := restRequest(t, h, http.MethodPost, "/apikeys",
		`{"name":"customer-key","enabled":true,"customerId":"mp-cust-123"}`)
	require.True(t, createRec.Code >= 200 && createRec.Code < 300)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createResp))
	keyID, _ := createResp["id"].(string)
	require.NotEmpty(t, keyID)
	assert.Equal(t, "mp-cust-123", createResp["customerId"],
		"CreateApiKey response must echo customerId back on the wire")

	stored, err := b.GetAPIKey(keyID)
	require.NoError(t, err)
	assert.Equal(t, "mp-cust-123", stored.CustomerID, "customerId must be persisted on create")

	patchRec := restRequest(t, h, http.MethodPatch, "/apikeys/"+keyID,
		`[{"op":"replace","path":"/customerId","value":"mp-cust-456"}]`)
	require.True(t, patchRec.Code >= 200 && patchRec.Code < 300)

	updated, err := b.GetAPIKey(keyID)
	require.NoError(t, err)
	assert.Equal(t, "mp-cust-456", updated.CustomerID,
		"PATCH /customerId must update the API key's customerId")
}

func TestBackend_GetAPIKeyByValue_IndexConsistency(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	key, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "k", Value: "secret-value-1", Enabled: true})
	require.NoError(t, err)

	got, err := b.GetAPIKeyByValue("secret-value-1")
	require.NoError(t, err)
	assert.Equal(t, key.ID, got.ID)

	// After deletion the value must no longer resolve.
	require.NoError(t, b.DeleteAPIKey(key.ID))
	_, err = b.GetAPIKeyByValue("secret-value-1")
	require.Error(t, err)
}

// TestUpdateApiKey tests UpdateApiKey.
func TestUpdateApiKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name",
			wantCode: http.StatusOK,
			useValid: true,
			newName:  "new-key-name",
		},
		{
			name:     "key_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			createRec := postWithHandler(t, handler, e, "CreateApiKey", `{"name":"orig-key","enabled":true}`)
			require.Equal(t, http.StatusCreated, createRec.Code)
			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			keyID := createResp["id"].(string)

			lookupID := keyID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateApiKey",
				fmt.Sprintf(`{"apiKeyId":%q,"name":%q}`, lookupID, tt.newName))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_GetAPIKeyByValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		findValue string
		wantFound bool
	}{
		{name: "found_by_value", findValue: "auto", wantFound: true},
		{name: "not_found", findValue: "nonexistent-value-xyz", wantFound: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			key, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{
				Name:    "test-key",
				Enabled: true,
			})
			require.NoError(t, err)
			require.NotEmpty(t, key.Value)

			lookupValue := key.Value
			if tt.findValue != "auto" {
				lookupValue = tt.findValue
			}

			got, err := b.GetAPIKeyByValue(lookupValue)
			if tt.wantFound {
				require.NoError(t, err)
				assert.Equal(t, key.ID, got.ID)
				assert.Equal(t, key.Value, got.Value)
			} else {
				require.Error(t, err)
				assert.Nil(t, got)
			}
		})
	}
}

func TestBackend_GetAPIKeyByValue_MultipleKeys(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	k1, _ := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "key1", Enabled: true})
	k2, _ := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "key2", Enabled: true})

	got1, err := b.GetAPIKeyByValue(k1.Value)
	require.NoError(t, err)
	assert.Equal(t, k1.ID, got1.ID)

	got2, err := b.GetAPIKeyByValue(k2.Value)
	require.NoError(t, err)
	assert.Equal(t, k2.ID, got2.ID)
}

// TestBackend_CreateAPIKey_StageKeys exercises CreateApiKeyInput.StageKeys
// (types.CreateApiKeyInput.StageKeys is []types.StageKey -- typed
// restApiId/stageName objects, unlike the resulting APIKey.StageKeys, which
// mirrors CreateApiKeyOutput.StageKeys' []string "{restApiId}/{stageName}"
// wire format). AWS's SDK doc comment marks this field "DEPRECATED FOR USAGE
// PLANS" but it remains a real, settable field.
func TestBackend_CreateAPIKey_StageKeys(t *testing.T) {
	t.Parallel()

	t.Run("valid stage keys are resolved and formatted", func(t *testing.T) {
		t.Parallel()

		b := apigateway.NewInMemoryBackend()
		api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "stagekeys-api"})
		require.NoError(t, err)
		depl, err := b.CreateDeployment(api.ID, "", "v1")
		require.NoError(t, err)
		_, err = b.CreateStage(apigateway.CreateStageInput{
			RestAPIID: api.ID, StageName: "prod", DeploymentID: depl.ID,
		})
		require.NoError(t, err)

		key, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{
			Name: "stagekeys-key",
			StageKeys: []apigateway.StageKeyInput{
				{RestAPIID: api.ID, StageName: "prod"},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, []string{api.ID + "/prod"}, key.StageKeys)

		got, err := b.GetAPIKey(key.ID)
		require.NoError(t, err)
		assert.Equal(t, []string{api.ID + "/prod"}, got.StageKeys, "GetApiKey returns the same stageKeys")
	})

	t.Run("referencing a nonexistent stage is rejected", func(t *testing.T) {
		t.Parallel()

		b := apigateway.NewInMemoryBackend()
		_, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{
			Name: "bad-stagekeys-key",
			StageKeys: []apigateway.StageKeyInput{
				{RestAPIID: "nonexistent-api", StageName: "prod"},
			},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, apigateway.ErrStageNotFound)
	})
}
