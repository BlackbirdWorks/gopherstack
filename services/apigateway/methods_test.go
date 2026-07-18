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

// TestPutMethod_AuthTypeValidation verifies authorizationType validation.
// Real AWS returns BadRequestException for invalid authorizationType.
func TestPutMethod_AuthTypeValidation(t *testing.T) {
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

// TestPutMethod_CustomRequiresAuthorizerId verifies that CUSTOM and COGNITO_USER_POOLS
// authorizationType require an authorizerId. Real AWS returns BadRequestException otherwise.
func TestPutMethod_CustomRequiresAuthorizerId(t *testing.T) {
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

// TestPutMethod_RequestParametersRoundtrip verifies that RequestParameters are stored
// and returned. Real AWS preserves the requestParameters map on a method.
func TestPutMethod_RequestParametersRoundtrip(t *testing.T) {
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

func TestMethod_RequestModels(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "model-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	m, err := b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "POST",
		AuthorizationType: "NONE",
		RequestModels:     map[string]string{"application/json": "Empty"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"application/json": "Empty"}, m.RequestModels)
}

// TestTestInvokeMethod tests TestInvokeMethod.
func TestTestInvokeMethod(t *testing.T) {
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

// TestUpdateMethod tests UpdateMethod.
func TestUpdateMethod(t *testing.T) {
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

// TestUpdateMethodResponse tests UpdateMethodResponse.
func TestUpdateMethodResponse(t *testing.T) {
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

// TestBackend_DeleteMethod_NotFound covers the "resource not found" and
// "method not found" error branches in DeleteMethod.
func TestBackend_DeleteMethod_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		httpMethod string
		wantErr    bool
	}{
		{
			name:       "resource_not_found",
			resourceID: "nonexistent",
			httpMethod: "GET",
			wantErr:    true,
		},
		{
			name:       "method_not_found",
			resourceID: "", // filled in by setup
			httpMethod: "DELETE",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			resID := tt.resourceID
			if resID == "" {
				resID = rootID
			}

			err = b.DeleteMethod(api.ID, resID, tt.httpMethod)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestMethodActions_RESTPathCoverage exercises methodActions closures via REST paths.
func TestMethodActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:   "PUT_method_via_REST",
			method: http.MethodPut,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", api.ID, resources[0].ID)
			},
			body:     `{"authorizationType":"NONE"}`,
			wantCode: http.StatusCreated,
		},
		{
			name:   "GET_method_via_REST",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				_, _ = b.PutMethod(
					apigateway.PutMethodInput{
						RestAPIID:         api.ID,
						ResourceID:        resources[0].ID,
						HTTPMethod:        "POST",
						AuthorizationType: "NONE",
					},
				)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/POST", api.ID, resources[0].ID)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParseAPIGWMethodPath_EdgeCases covers the branches in parseAPIGWMethodPath
// that are unreachable via normal REST calls:
//   - path ending at "methods" with no httpMethod segment → returns false
//   - integration segment with an unsupported HTTP method → returns false
func TestParseAPIGWMethodPath_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "methods_segment_without_httpMethod_returns_404",
			method:   http.MethodGet,
			path:     "/restapis/abc123/resources/resxyz/methods",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "integration_with_POST_method_returns_404",
			method:   http.MethodPost,
			path:     "/restapis/abc123/resources/resxyz/methods/GET/integration",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			rec := restRequest(t, h, tt.method, tt.path, "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_Method(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "put_get_delete",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				m, err := b.PutMethod(
					apigateway.PutMethodInput{
						RestAPIID:         api.ID,
						ResourceID:        rootID,
						HTTPMethod:        "GET",
						AuthorizationType: "NONE",
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "GET", m.HTTPMethod)

				got, err := b.GetMethod(api.ID, rootID, "GET")
				require.NoError(t, err)
				assert.Equal(t, "NONE", got.AuthorizationType)

				err = b.DeleteMethod(api.ID, rootID, "GET")
				require.NoError(t, err)

				_, err = b.GetMethod(api.ID, rootID, "GET")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBackend_MethodResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put_get_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			resources, _, _ := b.GetResources(api.ID, "", 0)
			rootID := resources[0].ID

			_, _ = b.PutMethod(
				apigateway.PutMethodInput{
					RestAPIID:         api.ID,
					ResourceID:        rootID,
					HTTPMethod:        "GET",
					AuthorizationType: "NONE",
				},
			)

			mr, err := b.PutMethodResponse(api.ID, rootID, "GET", "200", apigateway.PutMethodResponseInput{
				ResponseModels: map[string]string{"application/json": "Empty"},
			})
			require.NoError(t, err)
			assert.Equal(t, "200", mr.StatusCode)

			got, err := b.GetMethodResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)
			assert.Equal(t, "200", got.StatusCode)

			err = b.DeleteMethodResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)

			_, err = b.GetMethodResponse(api.ID, rootID, "GET", "200")
			require.Error(t, err)
		})
	}
}
