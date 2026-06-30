package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// boostSetup is a local helper that creates a shared handler+echo for multi-step tests.
func boostSetup() (*apigateway.Handler, *echo.Echo) {
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend)
	e := echo.New()

	return handler, e
}

// boostAPI creates a REST API and returns its ID.
func boostAPI(t *testing.T, handler *apigateway.Handler, e *echo.Echo) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"boost-api"}`)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostRootResource returns the root resource ID for an API.
func boostRootResource(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID string) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "GetResources", fmt.Sprintf(`{"restApiId":%q}`, apiID))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["item"].([]any)
	require.NotEmpty(t, items)

	return items[0].(map[string]any)["id"].(string)
}

// boostDeployment creates a deployment and returns its ID.
func boostDeployment(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID, stageName string) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateDeployment",
		fmt.Sprintf(`{"restApiId":%q,"stageName":%q}`, apiID, stageName))
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostAuthorizer creates an authorizer and returns its ID.
func boostAuthorizer(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID string) string {
	t.Helper()
	rec := postWithHandler(
		t,
		handler,
		e,
		"CreateAuthorizer",
		fmt.Sprintf(
			`{"restApiId":%q,"name":"auth","type":"TOKEN",`+
				`"authorizerUri":"arn:aws:lambda:us-east-1:123:function:auth",`+
				`"identitySource":"method.request.header.Authorization"}`,
			apiID,
		),
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostDocPart creates a documentation part and returns its ID.
func boostDocPart(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID string) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateDocumentationPart",
		fmt.Sprintf(`{"restApiId":%q,"location":{"type":"API"},"properties":"{}"}`, apiID))
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostDocVersion creates a documentation version and returns its name.
func boostDocVersion(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID, version string) {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateDocumentationVersion",
		fmt.Sprintf(`{"restApiId":%q,"documentationVersion":%q}`, apiID, version))
	require.Equal(t, http.StatusCreated, rec.Code)
}

// TestBoost_DocumentationPart_CRUD tests GetDocumentationPart, GetDocumentationParts, DeleteDocumentationPart.
func TestBoost_DocumentationPart_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		getWantCode     int
		deleteWantCode  int
		afterDeleteCode int
		listWantLen     int
		useValidID      bool
	}{
		{
			name:            "get_and_delete_existing",
			getWantCode:     http.StatusOK,
			deleteWantCode:  http.StatusNoContent,
			afterDeleteCode: http.StatusNotFound,
			listWantLen:     1,
			useValidID:      true,
		},
		{
			name:        "get_not_found",
			getWantCode: http.StatusNotFound,
			useValidID:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			partID := boostDocPart(t, handler, e, apiID)

			lookupID := partID
			if !tt.useValidID {
				lookupID = "notexist"
			}

			// GetDocumentationPart
			rec := postWithHandler(t, handler, e, "GetDocumentationPart",
				fmt.Sprintf(`{"restApiId":%q,"docPartId":%q}`, apiID, lookupID))
			assert.Equal(t, tt.getWantCode, rec.Code)

			if tt.listWantLen > 0 {
				// GetDocumentationParts
				rec2 := postWithHandler(t, handler, e, "GetDocumentationParts",
					fmt.Sprintf(`{"restApiId":%q}`, apiID))
				assert.Equal(t, http.StatusOK, rec2.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Len(t, resp["item"].([]any), tt.listWantLen)
			}

			if tt.deleteWantCode != 0 {
				rec3 := postWithHandler(t, handler, e, "DeleteDocumentationPart",
					fmt.Sprintf(`{"restApiId":%q,"docPartId":%q}`, apiID, partID))
				assert.Equal(t, tt.deleteWantCode, rec3.Code)

				rec4 := postWithHandler(t, handler, e, "GetDocumentationPart",
					fmt.Sprintf(`{"restApiId":%q,"docPartId":%q}`, apiID, partID))
				assert.Equal(t, tt.afterDeleteCode, rec4.Code)
			}
		})
	}
}

// TestBoost_DocumentationVersion_CRUD tests doc versions. //nolint:lll // existing issue.
func TestBoost_DocumentationVersion_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		version         string
		getWantCode     int
		deleteWantCode  int
		afterDeleteCode int
		listWantLen     int
		useValidVersion bool
	}{
		{
			name:            "get_and_delete_existing",
			version:         "1.0",
			getWantCode:     http.StatusOK,
			deleteWantCode:  http.StatusNoContent,
			afterDeleteCode: http.StatusNotFound,
			listWantLen:     1,
			useValidVersion: true,
		},
		{
			name:            "get_not_found",
			version:         "notexist",
			getWantCode:     http.StatusNotFound,
			useValidVersion: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			boostDocVersion(t, handler, e, apiID, "1.0")

			lookupVersion := "1.0"
			if !tt.useValidVersion {
				lookupVersion = "notexist"
			}

			rec := postWithHandler(t, handler, e, "GetDocumentationVersion",
				fmt.Sprintf(`{"restApiId":%q,"documentationVersion":%q}`, apiID, lookupVersion))
			assert.Equal(t, tt.getWantCode, rec.Code)

			if tt.listWantLen > 0 {
				rec2 := postWithHandler(t, handler, e, "GetDocumentationVersions",
					fmt.Sprintf(`{"restApiId":%q}`, apiID))
				assert.Equal(t, http.StatusOK, rec2.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Len(t, resp["item"].([]any), tt.listWantLen)
			}

			if tt.deleteWantCode != 0 {
				rec3 := postWithHandler(t, handler, e, "DeleteDocumentationVersion",
					fmt.Sprintf(`{"restApiId":%q,"documentationVersion":"1.0"}`, apiID))
				assert.Equal(t, tt.deleteWantCode, rec3.Code)

				rec4 := postWithHandler(t, handler, e, "GetDocumentationVersion",
					fmt.Sprintf(`{"restApiId":%q,"documentationVersion":"1.0"}`, apiID))
				assert.Equal(t, tt.afterDeleteCode, rec4.Code)
			}
		})
	}
}

// TestBoost_UpdateDocumentationPart tests UpdateDocumentationPart.
func TestBoost_UpdateDocumentationPart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		properties string
		wantCode   int
		useValid   bool
	}{
		{
			name:       "update_properties",
			properties: `{"description":"updated"}`,
			wantCode:   http.StatusOK,
			useValid:   true,
		},
		{
			name:     "not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			partID := boostDocPart(t, handler, e, apiID)

			lookupID := partID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateDocumentationPart",
				fmt.Sprintf(`{"restApiId":%q,"docPartId":%q,"properties":%q}`, apiID, lookupID, tt.properties))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateDocumentationVersion tests UpdateDocumentationVersion.
func TestBoost_UpdateDocumentationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		version     string
		description string
		wantCode    int
	}{
		{
			name:        "update_description",
			version:     "1.0",
			description: "new description",
			wantCode:    http.StatusOK,
		},
		{
			name:     "not_found",
			version:  "notexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			boostDocVersion(t, handler, e, apiID, "1.0")

			rec := postWithHandler(t, handler, e, "UpdateDocumentationVersion",
				fmt.Sprintf(`{"restApiId":%q,"documentationVersion":%q,"description":%q}`,
					apiID, tt.version, tt.description))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateDeployment tests UpdateDeployment.
func TestBoost_UpdateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		wantCode    int
		useValid    bool
	}{
		{
			name:        "update_description",
			description: "updated deployment",
			wantCode:    http.StatusOK,
			useValid:    true,
		},
		{
			name:     "deployment_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			deplID := boostDeployment(t, handler, e, apiID, "prod")

			lookupID := deplID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateDeployment",
				fmt.Sprintf(`{"restApiId":%q,"deploymentId":%q,"description":%q}`,
					apiID, lookupID, tt.description))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.description, resp["description"])
			}
		})
	}
}

// TestBoost_UpdateModel tests UpdateModel.
func TestBoost_UpdateModel(t *testing.T) {
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

// TestBoost_UpdateBasePathMapping tests UpdateBasePathMapping.
func TestBoost_UpdateBasePathMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newStage string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_stage",
			wantCode: http.StatusOK,
			useValid: true,
			newStage: "v2",
		},
		{
			name:     "not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			// Create base path mapping first
			postWithHandler(t, handler, e, "CreateBasePathMapping",
				`{"domainName":"api.example.com","basePath":"v1","restApiId":"abc123","stage":"prod"}`)

			domainName := "api.example.com"
			basePath := "v1"
			if !tt.useValid {
				domainName = "notexist.example.com"
			}

			rec := postWithHandler(t, handler, e, "UpdateBasePathMapping",
				fmt.Sprintf(`{"domainName":%q,"basePath":%q,"stage":%q}`, domainName, basePath, tt.newStage))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_ResourceTags tests GetResourceTags, TagResource, UntagResource.
func TestBoost_ResourceTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags       map[string]string
		wantTagsAfter map[string]string
		name          string
		initialTags   string
		removeTags    []string
	}{
		{
			name:        "tag_and_untag",
			initialTags: `{"env":"test"}`,
			addTags:     map[string]string{"version": "2"},
			removeTags:  []string{"env"},
			wantTagsAfter: map[string]string{
				"version": "2",
			},
		},
		{
			name:        "tag_only",
			initialTags: `{}`,
			addTags:     map[string]string{"team": "backend"},
			wantTagsAfter: map[string]string{
				"team": "backend",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			// Create API with tags
			rec := postWithHandler(t, handler, e, "CreateRestApi",
				fmt.Sprintf(`{"name":"tagged-api","tags":%s}`, tt.initialTags))
			require.Equal(t, http.StatusCreated, rec.Code)
			var apiResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiResp))
			apiID := apiResp["id"].(string)

			arn := fmt.Sprintf("arn:aws:apigateway:us-east-1::/restapis/%s", apiID)

			// TagResource
			tagsJSON, err := json.Marshal(tt.addTags)
			require.NoError(t, err)
			tagRec := postWithHandler(t, handler, e, "TagResource",
				fmt.Sprintf(`{"resourceArn":%q,"tags":%s}`, arn, tagsJSON))
			assert.Equal(t, http.StatusNoContent, tagRec.Code)

			// UntagResource (if applicable)
			if len(tt.removeTags) > 0 {
				keysJSON, err2 := json.Marshal(tt.removeTags)
				require.NoError(t, err2)
				untagRec := postWithHandler(t, handler, e, "UntagResource",
					fmt.Sprintf(`{"resourceArn":%q,"tagKeys":%s}`, arn, keysJSON))
				assert.Equal(t, http.StatusNoContent, untagRec.Code)
			}

			// GetTags
			getRec := postWithHandler(t, handler, e, "GetTags",
				fmt.Sprintf(`{"resourceArn":%q}`, arn))
			assert.Equal(t, http.StatusOK, getRec.Code)
			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &tagsResp))
			tagsMap, _ := tagsResp["tags"].(map[string]any)
			for k, v := range tt.wantTagsAfter {
				assert.Equal(t, v, tagsMap[k])
			}
			for _, removedKey := range tt.removeTags {
				_, exists := tagsMap[removedKey]
				assert.False(t, exists, "key %q should have been removed", removedKey)
			}
		})
	}
}

// TestBoost_TestInvokeMethod tests TestInvokeMethod.
func TestBoost_TestInvokeMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		httpMethod       string
		integration      string
		wantCode         int
		wantStatusOK     bool
		useValidAPI      bool
		useValidResource bool
		useValidMethod   bool
	}{
		{
			name:             "invoke_mock_integration",
			httpMethod:       "GET",
			integration:      "MOCK",
			wantCode:         http.StatusOK,
			wantStatusOK:     true,
			useValidAPI:      true,
			useValidResource: true,
			useValidMethod:   true,
		},
		{
			name:             "invoke_no_integration",
			httpMethod:       "POST",
			integration:      "",
			wantCode:         http.StatusOK,
			wantStatusOK:     true,
			useValidAPI:      true,
			useValidResource: true,
			useValidMethod:   true,
		},
		{
			name:        "api_not_found",
			httpMethod:  "GET",
			wantCode:    http.StatusNotFound,
			useValidAPI: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			rootID := boostRootResource(t, handler, e, apiID)

			if tt.useValidMethod {
				postWithHandler(t, handler, e, "PutMethod",
					fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q,"authorizationType":"NONE"}`,
						apiID, rootID, tt.httpMethod))

				if tt.integration != "" {
					postWithHandler(t, handler, e, "PutIntegration",
						fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q,"type":%q}`,
							apiID, rootID, tt.httpMethod, tt.integration))
				}
			}

			lookupAPIID := apiID
			lookupResID := rootID
			lookupMethod := tt.httpMethod
			if !tt.useValidAPI {
				lookupAPIID = "notexist"
				lookupResID = "notexist"
				lookupMethod = "GET"
			}

			rec := postWithHandler(t, handler, e, "TestInvokeMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q}`,
					lookupAPIID, lookupResID, lookupMethod))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantStatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.EqualValues(t, http.StatusOK, resp["status"])
			}
		})
	}
}

// TestBoost_TestInvokeAuthorizer tests TestInvokeAuthorizer.
func TestBoost_TestInvokeAuthorizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantCode     int
		useValidAPI  bool
		useValidAuth bool
	}{
		{
			name:         "invoke_ok",
			wantCode:     http.StatusOK,
			useValidAPI:  true,
			useValidAuth: true,
		},
		{
			name:         "authorizer_not_found",
			wantCode:     http.StatusNotFound,
			useValidAPI:  true,
			useValidAuth: false,
		},
		{
			name:        "api_not_found",
			wantCode:    http.StatusNotFound,
			useValidAPI: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			authID := boostAuthorizer(t, handler, e, apiID)

			lookupAPIID := apiID
			lookupAuthID := authID
			if !tt.useValidAPI {
				lookupAPIID = "notexist"
				lookupAuthID = "notexist"
			} else if !tt.useValidAuth {
				lookupAuthID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "TestInvokeAuthorizer",
				fmt.Sprintf(`{"restApiId":%q,"authorizerId":%q}`, lookupAPIID, lookupAuthID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "test-principal", resp["principalId"])
			}
		})
	}
}

// TestBoost_GetModelTemplate tests GetModelTemplate.
func TestBoost_GetModelTemplate(t *testing.T) {
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

// TestBoost_GatewayResponses tests GetGatewayResponse, GetGatewayResponses, PutGatewayResponse, DeleteGatewayResponse.
func TestBoost_GatewayResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		responseType    string
		putStatusCode   string
		wantPutCode     int
		wantGetCode     int
		wantGetListCode int
		wantDeleteCode  int
		wantDefaultResp bool
		doDelete        bool
		wantAfterDelete bool
		apiNotFound     bool
	}{
		{
			name:            "default_response_returned_when_not_set",
			responseType:    "UNAUTHORIZED",
			wantGetCode:     http.StatusOK,
			wantDefaultResp: true,
		},
		{
			name:            "put_and_get_custom",
			responseType:    "RESOURCE_NOT_FOUND",
			putStatusCode:   "404",
			wantPutCode:     http.StatusCreated,
			wantGetCode:     http.StatusOK,
			wantGetListCode: http.StatusOK,
		},
		{
			name:            "put_and_delete",
			responseType:    "THROTTLED",
			putStatusCode:   "429",
			wantPutCode:     http.StatusCreated,
			doDelete:        true,
			wantDeleteCode:  http.StatusNoContent,
			wantAfterDelete: true,
		},
		{
			name:            "api_not_found_get_list",
			responseType:    "UNAUTHORIZED",
			wantGetListCode: http.StatusNotFound,
			apiNotFound:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			lookupAPIID := apiID
			if tt.apiNotFound {
				lookupAPIID = "notexist"
			}

			if tt.wantPutCode != 0 {
				putRec := postWithHandler(t, handler, e, "PutGatewayResponse",
					fmt.Sprintf(`{"restApiId":%q,"responseType":%q,"statusCode":%q}`,
						apiID, tt.responseType, tt.putStatusCode))
				assert.Equal(t, tt.wantPutCode, putRec.Code)
			}

			if tt.wantGetCode != 0 {
				getRec := postWithHandler(t, handler, e, "GetGatewayResponse",
					fmt.Sprintf(`{"restApiId":%q,"responseType":%q}`, apiID, tt.responseType))
				assert.Equal(t, tt.wantGetCode, getRec.Code)

				if tt.wantDefaultResp {
					var resp map[string]any
					require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
					assert.Equal(t, true, resp["defaultResponse"])
				}
			}

			if tt.wantGetListCode != 0 {
				listRec := postWithHandler(t, handler, e, "GetGatewayResponses",
					fmt.Sprintf(`{"restApiId":%q}`, lookupAPIID))
				assert.Equal(t, tt.wantGetListCode, listRec.Code)
			}

			if tt.doDelete {
				delRec := postWithHandler(t, handler, e, "DeleteGatewayResponse",
					fmt.Sprintf(`{"restApiId":%q,"responseType":%q}`, apiID, tt.responseType))
				assert.Equal(t, tt.wantDeleteCode, delRec.Code)

				if tt.wantAfterDelete {
					getRec := postWithHandler(t, handler, e, "GetGatewayResponse",
						fmt.Sprintf(`{"restApiId":%q,"responseType":%q}`, apiID, tt.responseType))
					assert.Equal(t, http.StatusOK, getRec.Code)
					var resp map[string]any
					require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
					// After delete, should revert to default
					assert.Equal(t, true, resp["defaultResponse"])
				}
			}
		})
	}
}

// TestBoost_ClientCertificates tests GenerateClientCertificate, GetClientCertificate, GetClientCertificates,
// DeleteClientCertificate, UpdateClientCertificate.
func TestBoost_ClientCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		description     string
		updateDesc      string
		wantCreateCode  int
		wantGetCode     int
		wantListMinLen  int
		wantDeleteCode  int
		wantUpdateCode  int
		doDelete        bool
		doUpdate        bool
		useInvalidGetID bool
	}{
		{
			name:           "full_lifecycle",
			description:    "test cert",
			updateDesc:     "updated description",
			wantCreateCode: http.StatusCreated,
			wantGetCode:    http.StatusOK,
			wantListMinLen: 1,
			doDelete:       true,
			wantDeleteCode: http.StatusNoContent,
			doUpdate:       true,
			wantUpdateCode: http.StatusOK,
		},
		{
			name:            "get_not_found",
			description:     "cert2",
			wantCreateCode:  http.StatusCreated,
			useInvalidGetID: true,
			wantGetCode:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			// Create
			createRec := postWithHandler(t, handler, e, "GenerateClientCertificate",
				fmt.Sprintf(`{"description":%q}`, tt.description))
			assert.Equal(t, tt.wantCreateCode, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			certID := createResp["clientCertificateId"].(string)

			// Get
			lookupID := certID
			if tt.useInvalidGetID {
				lookupID = "notexist"
			}
			getRec := postWithHandler(t, handler, e, "GetClientCertificate",
				fmt.Sprintf(`{"clientCertificateId":%q}`, lookupID))
			assert.Equal(t, tt.wantGetCode, getRec.Code)

			// List
			if tt.wantListMinLen > 0 {
				listRec := postWithHandler(t, handler, e, "GetClientCertificates", `{}`)
				assert.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				assert.GreaterOrEqual(t, len(listResp["item"].([]any)), tt.wantListMinLen)
			}

			// Update
			if tt.doUpdate {
				updateRec := postWithHandler(t, handler, e, "UpdateClientCertificate",
					fmt.Sprintf(`{"clientCertificateId":%q,"description":%q}`, certID, tt.updateDesc))
				assert.Equal(t, tt.wantUpdateCode, updateRec.Code)
			}

			// Delete
			if tt.doDelete {
				delRec := postWithHandler(t, handler, e, "DeleteClientCertificate",
					fmt.Sprintf(`{"clientCertificateId":%q}`, certID))
				assert.Equal(t, tt.wantDeleteCode, delRec.Code)

				getRec2 := postWithHandler(t, handler, e, "GetClientCertificate",
					fmt.Sprintf(`{"clientCertificateId":%q}`, certID))
				assert.Equal(t, http.StatusNotFound, getRec2.Code)
			}
		})
	}
}

// TestBoost_VpcLinks tests CreateVpcLink, GetVpcLink, GetVpcLinks, DeleteVpcLink, UpdateVpcLink.
func TestBoost_VpcLinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		linkName        string
		updateName      string
		wantCreateCode  int
		wantGetCode     int
		wantDeleteCode  int
		wantGetAfterDel int
		doDelete        bool
		useInvalidID    bool
	}{
		{
			name:            "full_lifecycle",
			linkName:        "my-vpc-link",
			updateName:      "updated-link",
			wantCreateCode:  http.StatusCreated,
			wantGetCode:     http.StatusOK,
			doDelete:        true,
			wantDeleteCode:  http.StatusNoContent,
			wantGetAfterDel: http.StatusNotFound,
		},
		{
			name:           "get_not_found",
			linkName:       "other-link",
			wantCreateCode: http.StatusCreated,
			useInvalidID:   true,
			wantGetCode:    http.StatusNotFound,
		},
		{
			name:           "missing_name_fails",
			linkName:       "",
			wantCreateCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			var linkID string

			// Create
			body := fmt.Sprintf(
				`{"name":%q,"targetArns":["arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/net/my-nlb/abc"]}`,
				tt.linkName,
			)
			createRec := postWithHandler(t, handler, e, "CreateVpcLink", body)
			assert.Equal(t, tt.wantCreateCode, createRec.Code)

			if tt.wantCreateCode == http.StatusCreated {
				var createResp map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				linkID = createResp["id"].(string)

				// Get
				lookupID := linkID
				if tt.useInvalidID {
					lookupID = "notexist"
				}
				getRec := postWithHandler(t, handler, e, "GetVpcLink",
					fmt.Sprintf(`{"vpcLinkId":%q}`, lookupID))
				assert.Equal(t, tt.wantGetCode, getRec.Code)

				// List
				listRec := postWithHandler(t, handler, e, "GetVpcLinks", `{}`)
				assert.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				assert.GreaterOrEqual(t, len(listResp["item"].([]any)), 1)

				// Update
				if tt.updateName != "" {
					updateRec := postWithHandler(t, handler, e, "UpdateVpcLink",
						fmt.Sprintf(`{"vpcLinkId":%q,"name":%q}`, linkID, tt.updateName))
					assert.Equal(t, http.StatusOK, updateRec.Code)
					var updateResp map[string]any
					require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
					assert.Equal(t, tt.updateName, updateResp["name"])
				}

				// Delete
				if tt.doDelete {
					delRec := postWithHandler(t, handler, e, "DeleteVpcLink",
						fmt.Sprintf(`{"vpcLinkId":%q}`, linkID))
					assert.Equal(t, tt.wantDeleteCode, delRec.Code)

					getRec2 := postWithHandler(t, handler, e, "GetVpcLink",
						fmt.Sprintf(`{"vpcLinkId":%q}`, linkID))
					assert.Equal(t, tt.wantGetAfterDel, getRec2.Code)
				}
			}
		})
	}
}

// TestBoost_GetUsage tests GetUsage.
func TestBoost_GetUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		useValid bool
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "plan_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			// Create a usage plan
			planRec := postWithHandler(t, handler, e, "CreateUsagePlan", `{"name":"my-plan"}`)
			require.Equal(t, http.StatusCreated, planRec.Code)
			var planResp map[string]any
			require.NoError(t, json.Unmarshal(planRec.Body.Bytes(), &planResp))
			planID := planResp["id"].(string)

			lookupID := planID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "GetUsage",
				fmt.Sprintf(`{"usagePlanId":%q,"startDate":"2024-01-01","endDate":"2024-01-31"}`, lookupID))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateMethod tests UpdateMethod.
func TestBoost_UpdateMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		updateInput    string
		wantCode       int
		useValidAPI    bool
		useValidRes    bool
		useValidMethod bool
	}{
		{
			name:           "update_auth_type",
			updateInput:    `"authorizationType":"AWS_IAM"`,
			wantCode:       http.StatusOK,
			useValidAPI:    true,
			useValidRes:    true,
			useValidMethod: true,
		},
		{
			name:           "method_not_found",
			updateInput:    `"authorizationType":"AWS_IAM"`,
			wantCode:       http.StatusNotFound,
			useValidAPI:    true,
			useValidRes:    true,
			useValidMethod: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			rootID := boostRootResource(t, handler, e, apiID)

			// PutMethod GET first
			postWithHandler(t, handler, e, "PutMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","authorizationType":"NONE"}`,
					apiID, rootID))

			lookupMethod := "GET"
			if !tt.useValidMethod {
				lookupMethod = "DELETE"
			}

			rec := postWithHandler(t, handler, e, "UpdateMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q,%s}`,
					apiID, rootID, lookupMethod, tt.updateInput))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_PaginatedListing tests GetAPIKeysPage, GetDomainNamesPage, GetUsagePlansPage (via handler).
func TestBoost_PaginatedListing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		setupAction  string
		setupBodies  []string
		wantCode     int
		wantMinItems int
	}{
		{
			name:        "api_keys_paginated",
			action:      "GetApiKeys",
			setupAction: "CreateApiKey",
			setupBodies: []string{
				`{"name":"key-a","enabled":true}`,
				`{"name":"key-b","enabled":true}`,
			},
			wantCode:     http.StatusOK,
			wantMinItems: 2,
		},
		{
			name:        "domain_names_paginated",
			action:      "GetDomainNames",
			setupAction: "CreateDomainName",
			setupBodies: []string{
				`{"domainName":"a.example.com"}`,
				`{"domainName":"b.example.com"}`,
			},
			wantCode:     http.StatusOK,
			wantMinItems: 2,
		},
		{
			name:        "usage_plans_paginated",
			action:      "GetUsagePlans",
			setupAction: "CreateUsagePlan",
			setupBodies: []string{
				`{"name":"plan-a"}`,
				`{"name":"plan-b"}`,
			},
			wantCode:     http.StatusOK,
			wantMinItems: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			for _, body := range tt.setupBodies {
				postWithHandler(t, handler, e, tt.setupAction, body)
			}

			// Request with limit=1 to exercise pagination logic
			rec := postWithHandler(t, handler, e, tt.action, `{"limit":1}`)
			assert.Equal(t, tt.wantCode, rec.Code)

			// Also request all to verify total count
			rec2 := postWithHandler(t, handler, e, tt.action, `{}`)
			assert.Equal(t, tt.wantCode, rec2.Code)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			items, _ := resp["item"].([]any)
			assert.GreaterOrEqual(t, len(items), tt.wantMinItems)
		})
	}
}

// TestBoost_UpdateIntegration tests UpdateIntegration.
func TestBoost_UpdateIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newURI   string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_uri",
			newURI:   "https://new.example.com",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "method_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			rootID := boostRootResource(t, handler, e, apiID)

			postWithHandler(t, handler, e, "PutMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":"POST","authorizationType":"NONE"}`,
					apiID, rootID))
			postWithHandler(
				t,
				handler,
				e,
				"PutIntegration",
				fmt.Sprintf(
					`{"restApiId":%q,"resourceId":%q,"httpMethod":"POST","type":"HTTP","uri":"https://orig.example.com"}`,
					apiID,
					rootID,
				),
			)

			lookupMethod := "POST"
			if !tt.useValid {
				lookupMethod = "DELETE"
			}

			rec := postWithHandler(t, handler, e, "UpdateIntegration",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":%q,"uri":%q}`,
					apiID, rootID, lookupMethod, tt.newURI))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateMethodResponse tests UpdateMethodResponse.
func TestBoost_UpdateMethodResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_response_models",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "method_response_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			rootID := boostRootResource(t, handler, e, apiID)

			postWithHandler(t, handler, e, "PutMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","authorizationType":"NONE"}`,
					apiID, rootID))
			postWithHandler(t, handler, e, "PutMethodResponse",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","statusCode":"200"}`,
					apiID, rootID))

			lookupCode := "200"
			if !tt.useValid {
				lookupCode = "404"
			}

			rec := postWithHandler(
				t,
				handler,
				e,
				"UpdateMethodResponse",
				fmt.Sprintf(
					`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","statusCode":%q,"responseModels":{"application/json":"Empty"}}`, //nolint:lll // existing issue.
					apiID,
					rootID,
					lookupCode,
				),
			)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateRestAPI tests UpdateRestApi.
func TestBoost_UpdateRestAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		newDesc  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name_and_description",
			newName:  "renamed-api",
			newDesc:  "updated desc",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "api_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			lookupID := apiID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateRestApi",
				fmt.Sprintf(`{"restApiId":%q,"name":%q,"description":%q}`,
					lookupID, tt.newName, tt.newDesc))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.newName, resp["name"])
			}
		})
	}
}

// TestBoost_UpdateResource tests UpdateResource.
func TestBoost_UpdateResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		newPathPart string
		wantCode    int
		useValid    bool
	}{
		{
			name:        "update_path_part",
			newPathPart: "items",
			wantCode:    http.StatusOK,
			useValid:    true,
		},
		{
			name:     "resource_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			rootID := boostRootResource(t, handler, e, apiID)

			// Create a child resource to update
			createRec := postWithHandler(t, handler, e, "CreateResource",
				fmt.Sprintf(`{"restApiId":%q,"parentId":%q,"pathPart":"original"}`, apiID, rootID))
			require.Equal(t, http.StatusCreated, createRec.Code)
			var childResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &childResp))
			childID := childResp["id"].(string)

			lookupID := childID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateResource",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"pathPart":%q}`,
					apiID, lookupID, tt.newPathPart))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateStage tests UpdateStage.
func TestBoost_UpdateStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newDesc  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_description",
			newDesc:  "updated stage",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "stage_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			boostDeployment(t, handler, e, apiID, "prod")

			lookupStage := "prod"
			if !tt.useValid {
				lookupStage = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateStage",
				fmt.Sprintf(`{"restApiId":%q,"stageName":%q,"description":%q}`,
					apiID, lookupStage, tt.newDesc))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_GetAccount_And_UpdateAccount tests GetAccount and UpdateAccount.
func TestBoost_GetAccount_And_UpdateAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		throttleBurst  int
		throttleRate   float64
		wantUpdateCode int
		wantGetCode    int
	}{
		{
			name:        "get_account",
			wantGetCode: http.StatusOK,
		},
		{
			name:           "update_throttle_settings",
			throttleBurst:  500,
			throttleRate:   100.0,
			wantUpdateCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			if tt.wantGetCode != 0 {
				rec := postWithHandler(t, handler, e, "GetAccount", `{}`)
				assert.Equal(t, tt.wantGetCode, rec.Code)
			}

			if tt.wantUpdateCode != 0 {
				rec := postWithHandler(t, handler, e, "UpdateAccount",
					fmt.Sprintf(`{"throttleSettings":{"burstLimit":%d,"rateLimit":%g}}`,
						tt.throttleBurst, tt.throttleRate))
				assert.Equal(t, tt.wantUpdateCode, rec.Code)
			}
		})
	}
}

// TestBoost_UpdateApiKey tests UpdateApiKey.
func TestBoost_UpdateApiKey(t *testing.T) {
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

// TestBoost_UpdateUsagePlan tests UpdateUsagePlan.
func TestBoost_UpdateUsagePlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name",
			newName:  "renamed-plan",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "plan_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			createRec := postWithHandler(t, handler, e, "CreateUsagePlan", `{"name":"orig-plan"}`)
			require.Equal(t, http.StatusCreated, createRec.Code)
			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			planID := createResp["id"].(string)

			lookupID := planID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateUsagePlan",
				fmt.Sprintf(`{"usagePlanId":%q,"name":%q}`, lookupID, tt.newName))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateDomainName tests UpdateDomainName.
func TestBoost_UpdateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newCert  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_certificate",
			wantCode: http.StatusOK,
			useValid: true,
			newCert:  "arn:aws:acm:us-east-1:123:certificate/new",
		},
		{
			name:     "domain_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			postWithHandler(t, handler, e, "CreateDomainName", `{"domainName":"api.example.com"}`)

			lookupDomain := "api.example.com"
			if !tt.useValid {
				lookupDomain = "notexist.example.com"
			}

			rec := postWithHandler(t, handler, e, "UpdateDomainName",
				fmt.Sprintf(`{"domainName":%q,"certificateArn":%q}`, lookupDomain, tt.newCert))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateIntegrationResponse tests UpdateIntegrationResponse.
func TestBoost_UpdateIntegrationResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode string
		wantCode   int
		useValid   bool
	}{
		{
			name:       "update_response_templates",
			statusCode: "200",
			wantCode:   http.StatusOK,
			useValid:   true,
		},
		{
			name:       "integration_response_not_found",
			statusCode: "404",
			wantCode:   http.StatusNotFound,
			useValid:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			rootID := boostRootResource(t, handler, e, apiID)

			postWithHandler(t, handler, e, "PutMethod",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","authorizationType":"NONE"}`,
					apiID, rootID))
			postWithHandler(t, handler, e, "PutIntegration",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","type":"MOCK"}`,
					apiID, rootID))
			postWithHandler(t, handler, e, "PutIntegrationResponse",
				fmt.Sprintf(`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","statusCode":"200"}`,
					apiID, rootID))

			rec := postWithHandler(
				t,
				handler,
				e,
				"UpdateIntegrationResponse",
				fmt.Sprintf(
					`{"restApiId":%q,"resourceId":%q,"httpMethod":"GET","statusCode":%q,"responseTemplates":{"application/json":"#set($x=1)"}}`, //nolint:lll // existing issue.
					apiID,
					rootID,
					tt.statusCode,
				),
			)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_Reset tests the Handler Reset method.
func TestBoost_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			// Confirm API exists
			rec := postWithHandler(t, handler, e, "GetRestApi",
				fmt.Sprintf(`{"restApiId":%q}`, apiID))
			assert.Equal(t, http.StatusOK, rec.Code)

			// Reset the handler
			handler.Reset()

			// After reset, the API should be gone
			rec2 := postWithHandler(t, handler, e, "GetRestApi",
				fmt.Sprintf(`{"restApiId":%q}`, apiID))
			assert.Equal(t, http.StatusNotFound, rec2.Code)
		})
	}
}

// TestBoost_UpdateAuthorizer tests UpdateAuthorizer.
func TestBoost_UpdateAuthorizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name",
			newName:  "new-auth-name",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "authorizer_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			authID := boostAuthorizer(t, handler, e, apiID)

			lookupID := authID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateAuthorizer",
				fmt.Sprintf(`{"restApiId":%q,"authorizerId":%q,"name":%q}`,
					apiID, lookupID, tt.newName))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBoost_UpdateRequestValidator tests UpdateRequestValidator.
func TestBoost_UpdateRequestValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name",
			newName:  "new-validator-name",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "validator_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			createRec := postWithHandler(t, handler, e, "CreateRequestValidator",
				fmt.Sprintf(`{"restApiId":%q,"name":"orig-validator"}`, apiID))
			require.Equal(t, http.StatusCreated, createRec.Code)
			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			validatorID := createResp["id"].(string)

			lookupID := validatorID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateRequestValidator",
				fmt.Sprintf(`{"restApiId":%q,"requestValidatorId":%q,"name":%q}`,
					apiID, lookupID, tt.newName))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
