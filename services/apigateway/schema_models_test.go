package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestGetModel_Flatten covers GetModel's Flatten query param
// (api_op_GetModel.go: "resolve all external model references and returns a
// flattened model schema"): a $ref to another model in the same REST API is
// inlined recursively, a $ref to an unknown model is left as-is, and a
// self-referencing cycle doesn't hang.
func TestGetModel_Flatten(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		flattenQS  string
		wantSchema string
	}{
		{
			name:       "unflattened_keeps_ref",
			flattenQS:  "",
			wantSchema: `{"$ref":"https://apigateway.amazonaws.com/restapis/{restApiId}/models/Address"}`,
		},
		{
			name:       "flattened_inlines_ref",
			flattenQS:  "?flatten=true",
			wantSchema: `{"properties":{"city":{"type":"string"}},"type":"object"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			postWithHandler(t, handler, e, "CreateModel",
				fmt.Sprintf(`{"restApiId":%q,"name":"Address","contentType":"application/json",`+
					`"schema":%q}`, apiID, `{"type":"object","properties":{"city":{"type":"string"}}}`))
			postWithHandler(t, handler, e, "CreateModel",
				fmt.Sprintf(`{"restApiId":%q,"name":"Person","contentType":"application/json",`+
					`"schema":%q}`, apiID,
					`{"$ref":"https://apigateway.amazonaws.com/restapis/{restApiId}/models/Address"}`))

			rec := restRequest(t, handler, http.MethodGet,
				"/restapis/"+apiID+"/models/Person"+tt.flattenQS, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var got struct {
				Schema string `json:"schema"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
			assert.JSONEq(t, tt.wantSchema, got.Schema)
		})
	}
}

// TestGetModel_FlattenSelfCycle proves a self-referencing model's Flatten
// doesn't infinite-loop: the cyclic $ref is left in place rather than
// expanded.
func TestGetModel_FlattenSelfCycle(t *testing.T) {
	t.Parallel()

	handler, e := boostSetup()
	apiID := boostAPI(t, handler, e)

	postWithHandler(t, handler, e, "CreateModel",
		fmt.Sprintf(`{"restApiId":%q,"name":"Node","contentType":"application/json",`+
			`"schema":%q}`, apiID,
			`{"$ref":"https://apigateway.amazonaws.com/restapis/{restApiId}/models/Node"}`))

	rec := restRequest(t, handler, http.MethodGet,
		"/restapis/"+apiID+"/models/Node?flatten=true", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var got struct {
		Schema string `json:"schema"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.JSONEq(t, `{"$ref":"https://apigateway.amazonaws.com/restapis/{restApiId}/models/Node"}`, got.Schema)
}

// TestUpdateModel tests UpdateModel.
func TestUpdateModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		modelName string
		schema    string
		wantCode  int
		useValid  bool
	}{
		{
			name:      "update_schema",
			modelName: "User",
			schema:    `{"type":"object","properties":{"id":{"type":"string"}}}`,
			wantCode:  http.StatusOK,
			useValid:  true,
		},
		{
			name:      "model_not_found",
			modelName: "notexist",
			wantCode:  http.StatusNotFound,
			useValid:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			// Create the model first
			postWithHandler(t, handler, e, "CreateModel",
				fmt.Sprintf(`{"restApiId":%q,"name":"User","contentType":"application/json"}`, apiID))

			lookupName := "User"
			if !tt.useValid {
				lookupName = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateModel",
				fmt.Sprintf(`{"restApiId":%q,"modelName":%q,"schema":%q}`, apiID, lookupName, tt.schema))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestGetModelTemplate tests GetModelTemplate.
func TestGetModelTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		schema       string
		wantContains string
		wantCode     int
		useValid     bool
	}{
		{
			name:         "with_schema",
			wantCode:     http.StatusOK,
			useValid:     true,
			schema:       `{"type":"object"}`,
			wantContains: `{\"type\":\"object\"}`,
		},
		{
			name:         "without_schema_default",
			wantCode:     http.StatusOK,
			useValid:     true,
			schema:       "",
			wantContains: "inputRoot",
		},
		{
			name:     "model_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			modelName := "User"
			body := fmt.Sprintf(`{"restApiId":%q,"name":%q,"contentType":"application/json"`, apiID, modelName)
			if tt.schema != "" {
				body += fmt.Sprintf(`,"schema":%q`, tt.schema)
			}
			body += "}"
			postWithHandler(t, handler, e, "CreateModel", body)

			lookupModel := modelName
			if !tt.useValid {
				lookupModel = "notexist"
			}

			rec := postWithHandler(t, handler, e, "GetModelTemplate",
				fmt.Sprintf(`{"restApiId":%q,"modelName":%q}`, apiID, lookupModel))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}
