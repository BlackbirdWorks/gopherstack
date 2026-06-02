package apigateway_test

// accuracy_batch2_ops_test.go — AWS-accuracy fixes for ApiGateway batch-2 ops audit (go-dq7e):
//   - GetApiKey/GetApiKeys: value field only returned when ?includeValue=true
//   - CreateDeployment: stageDescription and variables propagated to created stage
//   - PATCH endpoints: RFC 6902 patch ops applied (not silently dropped)

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// ---------------------------------------------------------------------------
// GetApiKey / GetApiKeys: value only returned with ?includeValue=true
// ---------------------------------------------------------------------------

// TestBatch2Ops_GetApiKey_ValueHiddenByDefault verifies that GetApiKey omits
// the value field unless includeValue=true is specified, matching AWS behaviour.
// Previously the value was always returned.
func TestBatch2Ops_GetApiKey_ValueHiddenByDefault(t *testing.T) {
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

// TestBatch2Ops_GetApiKeys_ValueHiddenByDefault verifies that GetApiKeys omits
// the value field from all keys unless includeValue=true, matching AWS behaviour.
func TestBatch2Ops_GetApiKeys_ValueHiddenByDefault(t *testing.T) {
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
			name:        "include_value_true_returns_values",
			queryString: "?includeValue=true",
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

// ---------------------------------------------------------------------------
// CreateDeployment: stageDescription and variables propagated to created stage
// ---------------------------------------------------------------------------

// TestBatch2Ops_CreateDeployment_StageDescription verifies that stageDescription
// provided in CreateDeployment is applied to the automatically-created stage,
// matching AWS behaviour. Previously stageDescription was silently dropped.
func TestBatch2Ops_CreateDeployment_StageDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		stageDescription string
		wantDescription  string
	}{
		{
			name:             "stage_description_propagated",
			stageDescription: "prod stage for v1",
			wantDescription:  "prod stage for v1",
		},
		{
			name:             "no_stage_description_means_stage_description_empty",
			stageDescription: "",
			wantDescription:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createAPIRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"depl-sd-api"}`)
			require.True(t, createAPIRec.Code >= 200 && createAPIRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(createAPIRec.Body).Decode(&apiResp))
			apiID, _ := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			deplBody := `{"stageName":"prod","description":"deploy v1","stageDescription":"` +
				tt.stageDescription + `"}`
			deplRec := restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/deployments", deplBody)
			require.True(t, deplRec.Code >= 200 && deplRec.Code < 300)

			stageRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/stages/prod", "")
			require.True(t, stageRec.Code >= 200 && stageRec.Code < 300)

			var stage map[string]any
			require.NoError(t, json.NewDecoder(stageRec.Body).Decode(&stage))
			// JSON omitempty means empty description is absent (nil), not "".
			got, _ := stage["description"].(string)
			assert.Equal(t, tt.wantDescription, got)
		})
	}
}

// TestBatch2Ops_CreateDeployment_StageVariables verifies that variables provided
// in CreateDeployment are applied to the automatically-created stage, matching
// AWS behaviour. Previously variables were silently dropped.
func TestBatch2Ops_CreateDeployment_StageVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		variables    map[string]string
		name         string
		wantVarKey   string
		wantVarValue string
		wantVarsLen  int
	}{
		{
			name:         "variables_propagated_to_stage",
			variables:    map[string]string{"env": "production", "version": "v1"},
			wantVarsLen:  2,
			wantVarKey:   "env",
			wantVarValue: "production",
		},
		{
			name:        "no_variables_is_fine",
			variables:   nil,
			wantVarsLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createAPIRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"depl-vars-api"}`)
			require.True(t, createAPIRec.Code >= 200 && createAPIRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(createAPIRec.Body).Decode(&apiResp))
			apiID, _ := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			var deplBody string
			if tt.variables != nil {
				varsJSON, _ := json.Marshal(tt.variables)
				deplBody = `{"stageName":"prod","variables":` + string(varsJSON) + `}`
			} else {
				deplBody = `{"stageName":"prod"}`
			}
			deplRec := restRequest(t, h, http.MethodPost, "/restapis/"+apiID+"/deployments", deplBody)
			require.True(t, deplRec.Code >= 200 && deplRec.Code < 300)

			stageRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/stages/prod", "")
			require.True(t, stageRec.Code >= 200 && stageRec.Code < 300)

			var stage map[string]any
			require.NoError(t, json.NewDecoder(stageRec.Body).Decode(&stage))

			vars, _ := stage["variables"].(map[string]any)
			assert.Len(t, vars, tt.wantVarsLen)
			if tt.wantVarKey != "" {
				assert.Equal(t, tt.wantVarValue, vars[tt.wantVarKey])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// RFC 6902 JSON Patch ops applied for REST PATCH endpoints
// ---------------------------------------------------------------------------

// TestBatch2Ops_PatchOps_UpdateApiKey verifies that JSON patch operations
// ([{"op":"replace","path":"/description","value":"x"}]) are applied when
// PATCH /apikeys/{id} is called, matching AWS REST API behaviour. Previously
// patch ops were silently dropped and the key was returned unchanged.
func TestBatch2Ops_PatchOps_UpdateApiKey(t *testing.T) {
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

// TestBatch2Ops_PatchOps_UpdateUsagePlan verifies that JSON patch operations
// are applied when PATCH /usageplans/{id} is called. Previously patch ops were
// silently dropped, leaving the plan name unchanged.
func TestBatch2Ops_PatchOps_UpdateUsagePlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		patchBody string
		wantName  string
	}{
		{
			name:      "replace_name_via_patch_op",
			patchBody: `[{"op":"replace","path":"/name","value":"renamed-plan"}]`,
			wantName:  "renamed-plan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			plan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{Name: "original-plan"})
			require.NoError(t, err)

			patchRec := restRequest(t, h, http.MethodPatch, "/usageplans/"+plan.ID, tt.patchBody)
			require.True(t, patchRec.Code >= 200 && patchRec.Code < 300)

			updated, err := b.GetUsagePlan(plan.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, updated.Name,
				"patch op must update the usage plan name")
		})
	}
}
