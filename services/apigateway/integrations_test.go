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

// TestPutIntegration_DefaultTimeout verifies that PutIntegration
// returns timeoutInMillis=29000 when the caller does not specify it, matching
// AWS behaviour. Previously the field was absent (serialised as 0 with omitempty).
func TestPutIntegration_DefaultTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          string
		wantTimeoutMs float64
	}{
		{
			name:          "no_timeout_defaults_to_29000",
			body:          `{"type":"AWS_PROXY","httpMethod":"POST","uri":"arn:aws:lambda:::function:fn"}`,
			wantTimeoutMs: 29000,
		},
		{
			name: "explicit_timeout_preserved",
			body: `{"type":"AWS_PROXY","httpMethod":"POST","uri":"arn:aws:lambda:::function:fn",` +
				`"timeoutInMillis":5000}`,
			wantTimeoutMs: 5000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			createRec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"timeout-api"}`)
			require.True(t, createRec.Code >= 200 && createRec.Code < 300)

			var apiResp map[string]any
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&apiResp))
			apiID := apiResp["id"].(string)
			require.NotEmpty(t, apiID)

			resRec := restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
			require.True(t, resRec.Code >= 200 && resRec.Code < 300)

			var resResp map[string]any
			require.NoError(t, json.NewDecoder(resRec.Body).Decode(&resResp))
			items := resResp["item"].([]any)
			rootID := items[0].(map[string]any)["id"].(string)

			putMethodRec := restRequest(t, h, http.MethodPut,
				"/restapis/"+apiID+"/resources/"+rootID+"/methods/POST",
				`{"httpMethod":"POST","authorizationType":"NONE"}`)
			require.True(t, putMethodRec.Code >= 200 && putMethodRec.Code < 300)

			intRec := restRequest(t, h, http.MethodPut,
				"/restapis/"+apiID+"/resources/"+rootID+"/methods/POST/integration",
				tt.body)
			require.True(t, intRec.Code >= 200 && intRec.Code < 300)

			var intResp map[string]any
			require.NoError(t, json.NewDecoder(intRec.Body).Decode(&intResp))

			got, ok := intResp["timeoutInMillis"].(float64)
			require.True(t, ok, "timeoutInMillis must be present in PutIntegration response")
			assert.InDelta(t, tt.wantTimeoutMs, got, 0.1,
				"timeoutInMillis must equal %v", tt.wantTimeoutMs)
		})
	}
}

// TestPutIntegration_TypeValidation verifies that invalid integration types are rejected.
// Real AWS returns BadRequestException for unknown integration types.
func TestPutIntegration_TypeValidation(t *testing.T) {
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

func TestIntegration_ConnectionType_VpcLink(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "vpc-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})

	vpcLink, _ := b.CreateVpcLink(apigateway.CreateVpcLinkInput{
		Name:       "my-link",
		TargetARNs: []string{"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/abc123"},
	})

	integ, err := b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:           "HTTP",
		HTTPMethod:     "GET",
		URI:            "https://internal.example.com/api",
		ConnectionType: "VPC_LINK",
		ConnectionID:   vpcLink.ID,
	})
	require.NoError(t, err)
	assert.Equal(t, "VPC_LINK", integ.ConnectionType)
	assert.Equal(t, vpcLink.ID, integ.ConnectionID)

	got, err := b.GetIntegration(api.ID, rootID, "GET")
	require.NoError(t, err)
	assert.Equal(t, "VPC_LINK", got.ConnectionType)
	assert.Equal(t, vpcLink.ID, got.ConnectionID)
}

func TestIntegration_ConnectionType_Internet(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "inet-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "POST",
		AuthorizationType: "NONE",
	})

	integ, err := b.PutIntegration(api.ID, rootID, "POST", apigateway.PutIntegrationInput{
		Type:           "HTTP_PROXY",
		HTTPMethod:     "POST",
		URI:            "https://api.example.com/v1/items",
		ConnectionType: "INTERNET",
	})
	require.NoError(t, err)
	assert.Equal(t, "INTERNET", integ.ConnectionType)
}

func TestIntegration_ContentHandling_Credentials_Cache(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cred-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "PUT",
		AuthorizationType: "NONE",
	})

	integ, err := b.PutIntegration(api.ID, rootID, "PUT", apigateway.PutIntegrationInput{
		Type:               "AWS",
		HTTPMethod:         "POST",
		URI:                "arn:aws:lambda:::function:fn",
		ContentHandling:    "CONVERT_TO_TEXT",
		Credentials:        "arn:aws:iam::123456789012:role/MyApiRole",
		CacheNamespace:     "myns",
		CacheKeyParameters: []string{"method.request.header.Authorization"},
	})
	require.NoError(t, err)
	assert.Equal(t, "CONVERT_TO_TEXT", integ.ContentHandling)
	assert.Equal(t, "arn:aws:iam::123456789012:role/MyApiRole", integ.Credentials)
	assert.Equal(t, "myns", integ.CacheNamespace)
	assert.Equal(t, []string{"method.request.header.Authorization"}, integ.CacheKeyParameters)
}

func TestIntegration_RequestParameters(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "rp-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})

	reqParams := map[string]string{
		"integration.request.header.X-User-Id": "method.request.header.Authorization",
	}
	integ, err := b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:              "AWS_PROXY",
		HTTPMethod:        "POST",
		URI:               "arn:aws:lambda:::function:fn",
		RequestParameters: reqParams,
	})
	require.NoError(t, err)
	assert.Equal(t, reqParams, integ.RequestParameters)

	newParams := map[string]string{
		"integration.request.querystring.userId": "method.request.querystring.id",
	}
	updated, err := b.UpdateIntegration(apigateway.UpdateIntegrationInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		RequestParameters: newParams,
	})
	require.NoError(t, err)
	assert.Equal(t, newParams, updated.RequestParameters)
}

func TestUpdateIntegration_ConnectionType(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-integ-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "HTTP",
		HTTPMethod: "GET",
		URI:        "https://example.com/api",
	})

	vpcLink, _ := b.CreateVpcLink(apigateway.CreateVpcLinkInput{
		Name:       "my-link2",
		TargetARNs: []string{"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/nlb/abc"},
	})

	updated, err := b.UpdateIntegration(apigateway.UpdateIntegrationInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "GET",
		ConnectionType:     "VPC_LINK",
		ConnectionID:       vpcLink.ID,
		ContentHandling:    "CONVERT_TO_BINARY",
		CacheNamespace:     "mynamespace",
		CacheKeyParameters: []string{"method.request.path.id"},
	})
	require.NoError(t, err)
	assert.Equal(t, "VPC_LINK", updated.ConnectionType)
	assert.Equal(t, vpcLink.ID, updated.ConnectionID)
	assert.Equal(t, "CONVERT_TO_BINARY", updated.ContentHandling)
	assert.Equal(t, "mynamespace", updated.CacheNamespace)
	assert.Equal(t, []string{"method.request.path.id"}, updated.CacheKeyParameters)
}

func TestHandlerIntegration_ConnectionType_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	rec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"conn-type-api"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&apiResp))
	apiID := apiResp["id"].(string)

	rec = restRequest(t, h, http.MethodGet, "/restapis/"+apiID+"/resources", "")
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var resResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resResp))
	items := resResp["item"].([]any)
	rootID := items[0].(map[string]any)["id"].(string)

	rec = restRequest(t, h, http.MethodPut,
		"/restapis/"+apiID+"/resources/"+rootID+"/methods/GET",
		`{"httpMethod":"GET","authorizationType":"NONE"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	rec = restRequest(t, h, http.MethodPut,
		"/restapis/"+apiID+"/resources/"+rootID+"/methods/GET/integration",
		`{"type":"HTTP","httpMethod":"GET","uri":"https://api.example.com/","connectionType":"INTERNET"}`)
	require.True(t, rec.Code >= 200 && rec.Code < 300)

	var integResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&integResp))
	assert.Equal(t, "INTERNET", integResp["connectionType"])
}

func TestIntegrationResponse_ContentHandling(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "ch-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = b.PutIntegration(
		api.ID,
		rootID,
		"GET",
		apigateway.PutIntegrationInput{Type: "AWS", URI: "arn:aws:lambda:::function:fn"},
	)

	ir, err := b.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{
		ContentHandling: "CONVERT_TO_BINARY",
	})
	require.NoError(t, err)
	assert.Equal(t, "CONVERT_TO_BINARY", ir.ContentHandling)

	// Verify UpdateIntegrationResponse preserves contentHandling.
	updated, err := b.UpdateIntegrationResponse(apigateway.UpdateIntegrationResponseInput{
		RestAPIID:       api.ID,
		ResourceID:      rootID,
		HTTPMethod:      "GET",
		StatusCode:      "200",
		ContentHandling: "CONVERT_TO_TEXT",
	})
	require.NoError(t, err)
	assert.Equal(t, "CONVERT_TO_TEXT", updated.ContentHandling)
}

// TestUpdateIntegration tests UpdateIntegration.
func TestUpdateIntegration(t *testing.T) {
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

// TestUpdateIntegrationResponse tests UpdateIntegrationResponse.
func TestUpdateIntegrationResponse(t *testing.T) {
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

// TestBackend_PutIntegration_NotFound covers the "resource not found" and
// "method not found" error branches in PutIntegration.
func TestBackend_PutIntegration_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		httpMethod string
	}{
		{
			name:       "resource_not_found",
			resourceID: "nonexistent",
			httpMethod: "GET",
		},
		{
			name:       "method_not_found",
			resourceID: "", // uses root ID (method not set)
			httpMethod: "PATCH",
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

			_, err = b.PutIntegration(api.ID, resID, tt.httpMethod, apigateway.PutIntegrationInput{Type: "MOCK"})
			require.Error(t, err)
		})
	}
}

// TestHandler_RESTPath_Integration exercises the PUT/GET/DELETE integration REST-path
// branches in parseAPIGWMethodPath.
func TestHandler_RESTPath_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:     "PUT_integration_via_REST",
			method:   http.MethodPut,
			body:     `{"type":"MOCK"}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "GET_integration_via_REST",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "DELETE_integration_via_REST",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			resources, _, err := backend.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			_, err = backend.PutMethod(
				apigateway.PutMethodInput{
					RestAPIID:         api.ID,
					ResourceID:        rootID,
					HTTPMethod:        "GET",
					AuthorizationType: "NONE",
				},
			)
			require.NoError(t, err)

			// Ensure integration exists for GET and DELETE operations.
			if tt.method != http.MethodPut {
				_, err = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
				require.NoError(t, err)
			}

			h := apigateway.NewHandler(backend)
			path := fmt.Sprintf("/restapis/%s/resources/%s/methods/GET/integration", api.ID, rootID)

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBackend_Integration(t *testing.T) {
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

				_, _ = b.PutMethod(
					apigateway.PutMethodInput{
						RestAPIID:         api.ID,
						ResourceID:        rootID,
						HTTPMethod:        "POST",
						AuthorizationType: "NONE",
					},
				)

				input := apigateway.PutIntegrationInput{Type: "MOCK"}
				integ, err := b.PutIntegration(api.ID, rootID, "POST", input)
				require.NoError(t, err)
				assert.Equal(t, "MOCK", integ.Type)

				got, err := b.GetIntegration(api.ID, rootID, "POST")
				require.NoError(t, err)
				assert.Equal(t, "MOCK", got.Type)

				err = b.DeleteIntegration(api.ID, rootID, "POST")
				require.NoError(t, err)

				_, err = b.GetIntegration(api.ID, rootID, "POST")
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

func TestBackend_IntegrationResponse(t *testing.T) {
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
			_, _ = b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})

			ir, err := b.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{
				ResponseTemplates: map[string]string{"application/json": `{"status": "ok"}`},
			})
			require.NoError(t, err)
			assert.Equal(t, "200", ir.StatusCode)

			got, err := b.GetIntegrationResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)
			assert.Equal(t, "200", got.StatusCode)

			err = b.DeleteIntegrationResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)

			_, err = b.GetIntegrationResponse(api.ID, rootID, "GET", "200")
			require.Error(t, err)
		})
	}
}

func TestBackend_Integration_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, got *apigateway.Integration)
		name  string
		input apigateway.PutIntegrationInput
	}{
		{
			name: "connection_type_vpc_link",
			input: apigateway.PutIntegrationInput{
				Type:           "HTTP",
				HTTPMethod:     "GET",
				URI:            "https://internal.example.com/api",
				ConnectionType: "VPC_LINK",
				ConnectionID:   "abc123",
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "VPC_LINK", got.ConnectionType)
				assert.Equal(t, "abc123", got.ConnectionID)
			},
		},
		{
			name: "connection_type_internet",
			input: apigateway.PutIntegrationInput{
				Type:           "HTTP_PROXY",
				HTTPMethod:     "POST",
				URI:            "https://api.example.com/v1",
				ConnectionType: "INTERNET",
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "INTERNET", got.ConnectionType)
			},
		},
		{
			name: "content_handling_convert_to_text",
			input: apigateway.PutIntegrationInput{
				Type:            "AWS",
				HTTPMethod:      "POST",
				URI:             "arn:aws:lambda:::function:fn",
				ContentHandling: "CONVERT_TO_TEXT",
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "CONVERT_TO_TEXT", got.ContentHandling)
			},
		},
		{
			name: "credentials_and_cache",
			input: apigateway.PutIntegrationInput{
				Type:               "AWS",
				HTTPMethod:         "POST",
				URI:                "arn:aws:lambda:::function:fn",
				Credentials:        "arn:aws:iam::123456789012:role/MyRole",
				CacheNamespace:     "mynamespace",
				CacheKeyParameters: []string{"method.request.header.X-User-Id"},
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "arn:aws:iam::123456789012:role/MyRole", got.Credentials)
				assert.Equal(t, "mynamespace", got.CacheNamespace)
				assert.Equal(t, []string{"method.request.header.X-User-Id"}, got.CacheKeyParameters)
			},
		},
		{
			name: "request_parameters",
			input: apigateway.PutIntegrationInput{
				Type:       "AWS_PROXY",
				HTTPMethod: "POST",
				URI:        "arn:aws:lambda:::function:fn",
				RequestParameters: map[string]string{
					"integration.request.header.X-User-Id": "method.request.header.Authorization",
				},
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				require.Contains(t, got.RequestParameters, "integration.request.header.X-User-Id")
				assert.Equal(t, "method.request.header.Authorization",
					got.RequestParameters["integration.request.header.X-User-Id"])
			},
		},
		{
			name: "timeout_in_millis",
			input: apigateway.PutIntegrationInput{
				Type:            "AWS",
				HTTPMethod:      "POST",
				URI:             "arn:aws:lambda:::function:fn",
				TimeoutInMillis: 15000,
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, 15000, got.TimeoutInMillis)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "integ-" + tt.name})
			resources, _, _ := b.GetResources(api.ID, "", 0)
			rootID := resources[0].ID

			_, _ = b.PutMethod(apigateway.PutMethodInput{
				RestAPIID:         api.ID,
				ResourceID:        rootID,
				HTTPMethod:        "GET",
				AuthorizationType: "NONE",
			})

			integ, err := b.PutIntegration(api.ID, rootID, "GET", tt.input)
			require.NoError(t, err)
			tt.check(t, integ)

			got, err := b.GetIntegration(api.ID, rootID, "GET")
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}

func TestBackend_UpdateIntegration_NewFields(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-integ-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "HTTP",
		HTTPMethod: "GET",
		URI:        "https://example.com/api",
	})

	updated, err := b.UpdateIntegration(apigateway.UpdateIntegrationInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "GET",
		ConnectionType:     "INTERNET",
		ContentHandling:    "CONVERT_TO_BINARY",
		Credentials:        "arn:aws:iam::123456789012:role/TestRole",
		CacheNamespace:     "ns1",
		CacheKeyParameters: []string{"method.request.querystring.key"},
		RequestParameters: map[string]string{
			"integration.request.header.X-Forwarded-For": "method.request.header.X-Forwarded-For",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "INTERNET", updated.ConnectionType)
	assert.Equal(t, "CONVERT_TO_BINARY", updated.ContentHandling)
	assert.Equal(t, "arn:aws:iam::123456789012:role/TestRole", updated.Credentials)
	assert.Equal(t, "ns1", updated.CacheNamespace)
	assert.Equal(t, []string{"method.request.querystring.key"}, updated.CacheKeyParameters)
	require.Contains(t, updated.RequestParameters, "integration.request.header.X-Forwarded-For")
}
