package apigatewayv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestCreateAPIMapping_DuplicateKeyConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainName(t, h, "api.example.com")

	apiID := createAPI(t, h, "mapping-api")
	createStage(t, h, apiID, "prod")

	mappingPath := "/v2/domainnames/api.example.com/apimappings"

	// First mapping with key "v1" succeeds.
	rr := doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId":         apiID,
		"stage":         "prod",
		"apiMappingKey": "v1",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	// Duplicate key on same domain → 409 ConflictException.
	rr = doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId":         apiID,
		"stage":         "prod",
		"apiMappingKey": "v1",
	})
	assert.Equal(t, http.StatusConflict, rr.Code)
	assert.Equal(t, "ConflictException", rr.Header().Get(errTypeHeaderKey))

	// A different key still succeeds.
	rr = doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId":         apiID,
		"stage":         "prod",
		"apiMappingKey": "v2",
	})
	assert.Equal(t, http.StatusCreated, rr.Code)
}

func TestCreateAPIMapping_DuplicateDefaultKeyConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createDomainName(t, h, "default.example.com")

	apiID := createAPI(t, h, "default-mapping-api")
	createStage(t, h, apiID, "beta")

	mappingPath := "/v2/domainnames/default.example.com/apimappings"

	// Empty (default) mapping key.
	rr := doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId": apiID,
		"stage": "beta",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	// Second default mapping is a conflict.
	rr = doRequest(t, h, http.MethodPost, mappingPath, map[string]any{
		"apiId": apiID,
		"stage": "beta",
	})
	assert.Equal(t, http.StatusConflict, rr.Code)
}

// createStageAutoDeploy creates a stage with the given autoDeploy flag.
func createStageAutoDeploy(t *testing.T, h *apigatewayv2.Handler, apiID, stageName string, autoDeploy bool) {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/stages", map[string]any{
		"stageName":  stageName,
		"autoDeploy": autoDeploy,
	})
	require.Equal(t, http.StatusCreated, rr.Code)
}

func getStage(t *testing.T, h *apigatewayv2.Handler, apiID, stageName string) apigatewayv2.Stage {
	t.Helper()

	rr := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/stages/"+stageName, nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var s apigatewayv2.Stage
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &s))

	return s
}

func createIntegrationHelper(t *testing.T, h *apigatewayv2.Handler, apiID string) string {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/integrations", map[string]any{
		"integrationType":      "AWS_PROXY",
		"integrationUri":       backendFnURI,
		"payloadFormatVersion": "2.0",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var integ apigatewayv2.Integration
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integ))

	return integ.IntegrationID
}

func TestAutoDeploy_RouteAndIntegrationChangesDeploy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		autoDeploy   bool
		wantDeployed bool
	}{
		{name: "auto_deploy_enabled_deploys", autoDeploy: true, wantDeployed: true},
		{name: "auto_deploy_disabled_no_deploy", autoDeploy: false, wantDeployed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "autodeploy-api")
			createStageAutoDeploy(t, h, apiID, "prod", tt.autoDeploy)

			// Before any change the stage has no deployment.
			require.Empty(t, getStage(t, h, apiID, "prod").DeploymentID)

			// Creating an integration then a route mutates routing config.
			integID := createIntegrationHelper(t, h, apiID)

			rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
				"routeKey": "GET /a",
				"target":   "integrations/" + integID,
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			stage := getStage(t, h, apiID, "prod")

			if tt.wantDeployed {
				require.NotEmpty(t, stage.DeploymentID, "auto-deploy should link a deployment")

				// The linked deployment must be marked autoDeployed.
				drr := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/deployments", nil)
				require.Equal(t, http.StatusOK, drr.Code)

				var out struct {
					Items []apigatewayv2.Deployment `json:"items"`
				}
				require.NoError(t, json.Unmarshal(drr.Body.Bytes(), &out))
				require.NotEmpty(t, out.Items)

				var foundAuto bool
				for _, d := range out.Items {
					if d.AutoDeployed {
						foundAuto = true
					}
				}
				assert.True(t, foundAuto, "expected an autoDeployed deployment")
			} else {
				assert.Empty(t, stage.DeploymentID, "no auto-deploy expected")
			}
		})
	}
}

func TestExportAPI_ReturnsRawSpec(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "export-api")

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
		"routeKey": "GET /items",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	rr = doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/exports/OAS30", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var spec map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &spec))

	// Raw OpenAPI doc — not the old {"body":...,"specification":...} wrapper.
	assert.Equal(t, "3.0.1", spec["openapi"])
	assert.Contains(t, spec, "paths")
	assert.NotContains(t, spec, "specification")
	if _, wrapped := spec["body"]; wrapped {
		t.Fatalf("export should not wrap the spec in a body field: %v", spec)
	}
}

func TestExportAPI_InvalidSpecification(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "export-bad-api")

	rr := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/exports/OAS99", nil)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, "BadRequestException", rr.Header().Get(errTypeHeaderKey))
}

func TestExportAPI_JWTSecurityScheme(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "export-jwt-api")

	// JWT authorizer.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/authorizers", map[string]any{
		"name":           "jwt-auth",
		"authorizerType": "JWT",
		"identitySource": []string{"$request.header.Authorization"},
		"jwtConfiguration": map[string]any{
			"issuer":   "https://issuer.example.com",
			"audience": []string{"aud-1"},
		},
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var auth apigatewayv2.Authorizer
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &auth))

	rr = doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
		"routeKey":          "GET /jwt",
		"authorizationType": "JWT",
		"authorizerId":      auth.AuthorizerID,
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	rr = doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/exports/OAS30", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var spec map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &spec))

	components, ok := spec["components"].(map[string]any)
	require.True(t, ok, "expected components block")

	schemes, ok := components["securitySchemes"].(map[string]any)
	require.True(t, ok, "expected securitySchemes")
	assert.Contains(t, schemes, "jwt-auth")
}

func TestErrorTypeHeader_AcrossHandlers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *apigatewayv2.Handler) (string, string, any)
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "get_missing_api_404",
			setup: func(_ *testing.T, _ *apigatewayv2.Handler) (string, string, any) {
				return http.MethodGet, "/v2/apis/does-not-exist", nil
			},
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
		{
			name: "create_route_empty_key_400",
			setup: func(t *testing.T, h *apigatewayv2.Handler) (string, string, any) {
				t.Helper()

				apiID := createAPI(t, h, "err-api-1")

				return http.MethodPost, "/v2/apis/" + apiID + "/routes", map[string]any{"routeKey": ""}
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "BadRequestException",
		},
		{
			name: "duplicate_route_409",
			setup: func(t *testing.T, h *apigatewayv2.Handler) (string, string, any) {
				t.Helper()

				apiID := createAPI(t, h, "err-api-2")
				rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
					map[string]any{"routeKey": "GET /dup"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return http.MethodPost, "/v2/apis/" + apiID + "/routes", map[string]any{"routeKey": "GET /dup"}
			},
			wantStatus: http.StatusConflict,
			wantType:   "ConflictException",
		},
		{
			name: "get_missing_route_404",
			setup: func(t *testing.T, h *apigatewayv2.Handler) (string, string, any) {
				t.Helper()

				apiID := createAPI(t, h, "err-api-3")

				return http.MethodGet, "/v2/apis/" + apiID + "/routes/nope", nil
			},
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			method, path, body := tt.setup(t, h)

			rr := doRequest(t, h, method, path, body)

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, tt.wantType, rr.Header().Get(errTypeHeaderKey))
		})
	}
}
