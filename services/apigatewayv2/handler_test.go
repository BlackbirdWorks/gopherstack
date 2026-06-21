package apigatewayv2_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// newTestHandler creates a fresh Handler backed by an InMemoryBackend for tests.
func newTestHandler() *apigatewayv2.Handler {
	return apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend())
}

// doRequest performs an HTTP request against the handler and returns the recorder.
func doRequest(t *testing.T, h *apigatewayv2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rr)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rr
}

// doRequestRaw sends a POST request with a raw string body and returns the recorder.
func doRequestRaw(t *testing.T, h *apigatewayv2.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rr)
	require.NoError(t, h.Handler()(c))

	return rr
}

// createAPI is a test helper that creates an API and returns its ID.
func createAPI(t *testing.T, h *apigatewayv2.Handler, name string) string {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         name,
		"protocolType": "HTTP",
	})

	require.Equal(t, http.StatusCreated, rr.Code)

	var api apigatewayv2.API
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))

	return api.APIID
}

func TestHandler_CreateAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"name": "my-api", "protocolType": "HTTP"},
			wantStatus: http.StatusCreated,
			wantName:   "my-api",
		},
		{
			name:       "with_description",
			body:       map[string]any{"name": "api2", "protocolType": "HTTP", "description": "test api"},
			wantStatus: http.StatusCreated,
			wantName:   "api2",
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, "/v2/apis", s)
			} else {
				rr = doRequest(t, h, http.MethodPost, "/v2/apis", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var api apigatewayv2.API
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))
				assert.Equal(t, tt.wantName, api.Name)
				assert.NotEmpty(t, api.APIID)
			}
		})
	}
}

func TestHandler_GetAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		name       string
		apiID      string
		wantStatus int
	}{
		{
			name:       "existing_api",
			wantStatus: http.StatusOK,
			setup: func(h *apigatewayv2.Handler) string {
				return createAPI(t, h, "test-api")
			},
		},
		{
			name:       "not_found",
			apiID:      "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := tt.apiID
			if tt.setup != nil {
				apiID = tt.setup(h)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var api apigatewayv2.API
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))
				assert.Equal(t, apiID, api.APIID)
			}
		})
	}
}

func TestHandler_GetAPIs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiCount   int
		wantStatus int
	}{
		{
			name:       "empty",
			apiCount:   0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple_apis",
			apiCount:   3,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.apiCount {
				createAPI(t, h, fmt.Sprintf("api-%d", i))
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
			require.Equal(t, tt.wantStatus, rr.Code)

			type listResp struct {
				Items []apigatewayv2.API `json:"items"`
			}

			var resp listResp
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
			assert.Len(t, resp.Items, tt.apiCount)
		})
	}
}

func TestHandler_DeleteAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		name       string
		apiID      string
		wantStatus int
	}{
		{
			name:       "existing_api",
			wantStatus: http.StatusNoContent,
			setup: func(h *apigatewayv2.Handler) string {
				return createAPI(t, h, "to-delete")
			},
		},
		{
			name:       "not_found",
			apiID:      "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := tt.apiID
			if tt.setup != nil {
				apiID = tt.setup(h)
			}

			rr := doRequest(t, h, http.MethodDelete, "/v2/apis/"+apiID, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name:       "update_name",
			update:     map[string]any{"name": "updated-name"},
			wantStatus: http.StatusOK,
			wantName:   "updated-name",
		},
		{
			name:       "not_found",
			update:     map[string]any{"name": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var apiID string

			if tt.wantStatus == http.StatusOK {
				apiID = createAPI(t, h, "original")
			} else {
				apiID = "nonexistent"
			}

			rr := doRequest(t, h, http.MethodPatch, "/v2/apis/"+apiID, tt.update)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var api apigatewayv2.API
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))
				assert.Equal(t, tt.wantName, api.Name)
			}
		})
	}
}

func TestHandler_CreateStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageName  string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "success",
			stageName:  "prod",
			wantStatus: http.StatusCreated,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			stageName:  "prod",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": tt.stageName,
			})

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusCreated {
				var stage apigatewayv2.Stage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &stage))
				assert.Equal(t, tt.stageName, stage.StageName)
			}
		})
	}
}

func TestHandler_GetStages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stages     []string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "empty",
			stages:     nil,
			wantStatus: http.StatusOK,
			apiExists:  true,
		},
		{
			name:       "multiple_stages",
			stages:     []string{"dev", "prod"},
			wantStatus: http.StatusOK,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")

				for _, sn := range tt.stages {
					rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
						"stageName": sn,
					})
					require.Equal(t, http.StatusCreated, rr.Code)
				}
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/stages", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				type listResp struct {
					Items []apigatewayv2.Stage `json:"items"`
				}

				var resp listResp
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				assert.Len(t, resp.Items, len(tt.stages))
			}
		})
	}
}

func TestHandler_CreateRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		routeKey   string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "success",
			routeKey:   "GET /items",
			wantStatus: http.StatusCreated,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			routeKey:   "GET /items",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
				"routeKey": tt.routeKey,
			})

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusCreated {
				var route apigatewayv2.Route
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))
				assert.Equal(t, tt.routeKey, route.RouteKey)
				assert.NotEmpty(t, route.RouteID)
			}
		})
	}
}

func TestHandler_GetRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		routes     []string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "empty",
			routes:     nil,
			wantStatus: http.StatusOK,
			apiExists:  true,
		},
		{
			name:       "multiple_routes",
			routes:     []string{"GET /a", "POST /b"},
			wantStatus: http.StatusOK,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")

				for _, rk := range tt.routes {
					rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
						"routeKey": rk,
					})
					require.Equal(t, http.StatusCreated, rr.Code)
				}
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/routes", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				type listResp struct {
					Items []apigatewayv2.Route `json:"items"`
				}

				var resp listResp
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				assert.Len(t, resp.Items, len(tt.routes))
			}
		})
	}
}

func TestHandler_CreateIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		integrationType string
		wantStatus      int
		apiExists       bool
	}{
		{
			name:            "success",
			integrationType: "AWS_PROXY",
			wantStatus:      http.StatusCreated,
			apiExists:       true,
		},
		{
			name:            "api_not_found",
			integrationType: "AWS_PROXY",
			wantStatus:      http.StatusNotFound,
			apiExists:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
				"integrationType": tt.integrationType,
			})

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusCreated {
				var integration apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))
				assert.Equal(t, tt.integrationType, integration.IntegrationType)
				assert.NotEmpty(t, integration.IntegrationID)
			}
		})
	}
}

func TestHandler_GetIntegrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		integrations []string
		wantStatus   int
		apiExists    bool
	}{
		{
			name:         "empty",
			integrations: nil,
			wantStatus:   http.StatusOK,
			apiExists:    true,
		},
		{
			name:         "multiple",
			integrations: []string{"AWS_PROXY", "HTTP_PROXY"},
			wantStatus:   http.StatusOK,
			apiExists:    true,
		},
		{
			name:       "api_not_found",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")

				for _, it := range tt.integrations {
					rr := doRequest(
						t,
						h,
						http.MethodPost,
						fmt.Sprintf("/v2/apis/%s/integrations", apiID),
						map[string]any{
							"integrationType": it,
						},
					)
					require.Equal(t, http.StatusCreated, rr.Code)
				}
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/integrations", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				type listResp struct {
					Items []apigatewayv2.Integration `json:"items"`
				}

				var resp listResp
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				assert.Len(t, resp.Items, len(tt.integrations))
			}
		})
	}
}

func TestHandler_CreateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		desc       string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "success",
			desc:       "initial deploy",
			wantStatus: http.StatusCreated,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			desc:       "deploy",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{
				"description": tt.desc,
			})

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusCreated {
				var deployment apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &deployment))
				assert.NotEmpty(t, deployment.DeploymentID)
				assert.Equal(t, "DEPLOYED", deployment.DeploymentStatus)
			}
		})
	}
}

func TestHandler_RouteMatching(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name    string
		path    string
		matches bool
	}{
		{
			name:    "v2_apis_path",
			path:    "/v2/apis",
			matches: true,
		},
		{
			name:    "v2_sub_path",
			path:    "/v2/apis/abc/stages",
			matches: true,
		},
		{
			name:    "non_v2_path",
			path:    "/restapis",
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			matcher := h.RouteMatcher()
			assert.Equal(t, tt.matches, matcher(c))
		})
	}
}

func TestHandler_GetStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageName  string
		wantStatus int
		setupStage bool
	}{
		{
			name:       "existing",
			stageName:  "prod",
			wantStatus: http.StatusOK,
			setupStage: true,
		},
		{
			name:       "not_found",
			stageName:  "nonexistent",
			wantStatus: http.StatusNotFound,
			setupStage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			if tt.setupStage {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
					"stageName": tt.stageName,
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/stages/%s", apiID, tt.stageName), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageName  string
		wantStatus int
		setupStage bool
	}{
		{
			name:       "success",
			stageName:  "prod",
			wantStatus: http.StatusNoContent,
			setupStage: true,
		},
		{
			name:       "not_found",
			stageName:  "nonexistent",
			wantStatus: http.StatusNotFound,
			setupStage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			if tt.setupStage {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
					"stageName": tt.stageName,
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodDelete, fmt.Sprintf("/v2/apis/%s/stages/%s", apiID, tt.stageName), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     map[string]any
		name       string
		stageName  string
		wantStatus int
		setupStage bool
	}{
		{
			name:       "success",
			stageName:  "prod",
			update:     map[string]any{"description": "updated"},
			wantStatus: http.StatusOK,
			setupStage: true,
		},
		{
			name:       "not_found",
			stageName:  "nonexistent",
			update:     map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
			setupStage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			if tt.setupStage {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
					"stageName": tt.stageName,
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(
				t,
				h,
				http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/stages/%s", apiID, tt.stageName),
				tt.update,
			)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		setupRoute bool
	}{
		{
			name:       "existing",
			wantStatus: http.StatusOK,
			setupRoute: true,
		},
		{
			name:       "not_found",
			wantStatus: http.StatusNotFound,
			setupRoute: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			routeID := "nonexistent"
			if tt.setupRoute {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
					"routeKey": "GET /test",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var route apigatewayv2.Route
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))
				routeID = route.RouteID
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/routes/%s", apiID, routeID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		setupRoute bool
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
			setupRoute: true,
		},
		{
			name:       "not_found",
			wantStatus: http.StatusNotFound,
			setupRoute: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			routeID := "nonexistent"
			if tt.setupRoute {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
					"routeKey": "GET /test",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var route apigatewayv2.Route
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))
				routeID = route.RouteID
			}

			rr := doRequest(t, h, http.MethodDelete, fmt.Sprintf("/v2/apis/%s/routes/%s", apiID, routeID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		setupRoute bool
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
			setupRoute: true,
		},
		{
			name:       "not_found",
			wantStatus: http.StatusNotFound,
			setupRoute: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			routeID := "nonexistent"
			if tt.setupRoute {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
					"routeKey": "GET /test",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var route apigatewayv2.Route
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))
				routeID = route.RouteID
			}

			rr := doRequest(
				t,
				h,
				http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/routes/%s", apiID, routeID),
				map[string]any{
					"routeKey": "POST /test",
				},
			)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantStatus       int
		setupIntegration bool
	}{
		{
			name:             "existing",
			wantStatus:       http.StatusOK,
			setupIntegration: true,
		},
		{
			name:             "not_found",
			wantStatus:       http.StatusNotFound,
			setupIntegration: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			integrationID := "nonexistent"
			if tt.setupIntegration {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
					"integrationType": "AWS_PROXY",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var integration apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))
				integrationID = integration.IntegrationID
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/integrations/%s", apiID, integrationID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantStatus       int
		setupIntegration bool
	}{
		{
			name:             "success",
			wantStatus:       http.StatusNoContent,
			setupIntegration: true,
		},
		{
			name:             "not_found",
			wantStatus:       http.StatusNotFound,
			setupIntegration: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			integrationID := "nonexistent"
			if tt.setupIntegration {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
					"integrationType": "AWS_PROXY",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var integration apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))
				integrationID = integration.IntegrationID
			}

			rr := doRequest(
				t,
				h,
				http.MethodDelete,
				fmt.Sprintf("/v2/apis/%s/integrations/%s", apiID, integrationID),
				nil,
			)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantStatus       int
		setupIntegration bool
	}{
		{
			name:             "success",
			wantStatus:       http.StatusOK,
			setupIntegration: true,
		},
		{
			name:             "not_found",
			wantStatus:       http.StatusNotFound,
			setupIntegration: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			integrationID := "nonexistent"
			if tt.setupIntegration {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
					"integrationType": "AWS_PROXY",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var integration apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))
				integrationID = integration.IntegrationID
			}

			rr := doRequest(
				t,
				h,
				http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/integrations/%s", apiID, integrationID),
				map[string]any{
					"integrationType": "HTTP_PROXY",
				},
			)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantStatus      int
		setupDeployment bool
	}{
		{
			name:            "existing",
			wantStatus:      http.StatusOK,
			setupDeployment: true,
		},
		{
			name:            "not_found",
			wantStatus:      http.StatusNotFound,
			setupDeployment: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			deploymentID := "nonexistent"
			if tt.setupDeployment {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)

				var deployment apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &deployment))
				deploymentID = deployment.DeploymentID
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/deployments/%s", apiID, deploymentID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantStatus      int
		setupDeployment bool
	}{
		{
			name:            "success",
			wantStatus:      http.StatusNoContent,
			setupDeployment: true,
		},
		{
			name:            "not_found",
			wantStatus:      http.StatusNotFound,
			setupDeployment: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			deploymentID := "nonexistent"
			if tt.setupDeployment {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)

				var deployment apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &deployment))
				deploymentID = deployment.DeploymentID
			}

			rr := doRequest(
				t,
				h,
				http.MethodDelete,
				fmt.Sprintf("/v2/apis/%s/deployments/%s", apiID, deploymentID),
				nil,
			)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_Authorizers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		authorizerName string
		wantCreateCode int
	}{
		{
			name:           "success",
			authorizerName: "my-auth",
			wantCreateCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/authorizers", apiID), map[string]any{
				"name":           tt.authorizerName,
				"authorizerType": "JWT",
				"jwtConfiguration": map[string]any{
					"issuer":   "https://issuer.example.com",
					"audience": []string{"client-id"},
				},
			})
			require.Equal(t, tt.wantCreateCode, rr.Code)

			var auth apigatewayv2.Authorizer
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &auth))
			assert.Equal(t, tt.authorizerName, auth.Name)

			// GetAuthorizers
			rr = doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/authorizers", apiID), nil)
			require.Equal(t, http.StatusOK, rr.Code)

			// GetAuthorizer
			rr = doRequest(
				t,
				h,
				http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/authorizers/%s", apiID, auth.AuthorizerID),
				nil,
			)
			require.Equal(t, http.StatusOK, rr.Code)

			// UpdateAuthorizer
			rr = doRequest(
				t,
				h,
				http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/authorizers/%s", apiID, auth.AuthorizerID),
				map[string]any{
					"name": "updated-auth",
				},
			)
			require.Equal(t, http.StatusOK, rr.Code)

			// DeleteAuthorizer
			rr = doRequest(
				t,
				h,
				http.MethodDelete,
				fmt.Sprintf("/v2/apis/%s/authorizers/%s", apiID, auth.AuthorizerID),
				nil,
			)
			require.Equal(t, http.StatusNoContent, rr.Code)

			// Get after delete = 404
			rr = doRequest(
				t,
				h,
				http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/authorizers/%s", apiID, auth.AuthorizerID),
				nil,
			)
			require.Equal(t, http.StatusNotFound, rr.Code)
		})
	}
}

func TestHandler_HandlerMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, "APIGatewayV2", h.Name())
	assert.Equal(t, "apigatewayv2", h.ChaosServiceName())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "with_api_id",
			path: "/v2/apis/abc123/stages",
			want: "abc123",
		},
		{
			name: "empty_path",
			path: "/v2/apis",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_NotFoundPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rr := doRequest(t, h, http.MethodGet, "/not-a-v2-path", nil)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "create_api",
			method: http.MethodPost,
			path:   "/v2/apis",
			wantOp: "CreateApi",
		},
		{
			name:   "get_apis",
			method: http.MethodGet,
			path:   "/v2/apis",
			wantOp: "GetApis",
		},
		{
			name:   "get_api",
			method: http.MethodGet,
			path:   "/v2/apis/abc123",
			wantOp: "GetApi",
		},
		{
			name:   "delete_api",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123",
			wantOp: "DeleteApi",
		},
		{
			name:   "create_stage",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/stages",
			wantOp: "CreateStage",
		},
		{
			name:   "get_stage",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/stages/prod",
			wantOp: "GetStage",
		},
		{
			name:   "create_route",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/routes",
			wantOp: "CreateRoute",
		},
		{
			name:   "delete_route",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/routes/r1",
			wantOp: "DeleteRoute",
		},
		{
			name:   "create_deployment",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/deployments",
			wantOp: "CreateDeployment",
		},
		{
			name:   "get_deployment",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/deployments/d1",
			wantOp: "GetDeployment",
		},
		{
			name:   "create_authorizer",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/authorizers",
			wantOp: "CreateAuthorizer",
		},
		{
			name:   "create_integration_response",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses",
			wantOp: "CreateIntegrationResponse",
		},
		{
			name:   "create_route_response",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/routes/r1/routeresponses",
			wantOp: "CreateRouteResponse",
		},
		{
			name:   "create_domain_name",
			method: http.MethodPost,
			path:   "/v2/domainnames",
			wantOp: "CreateDomainName",
		},
		{
			name:   "create_api_mapping",
			method: http.MethodPost,
			path:   "/v2/domainnames/example.com/apimappings",
			wantOp: "CreateApiMapping",
		},
		{
			name:   "create_portal",
			method: http.MethodPost,
			path:   "/v2/portals",
			wantOp: "CreatePortal",
		},
		{
			name:   "create_portal_product",
			method: http.MethodPost,
			path:   "/v2/portalproducts",
			wantOp: "CreatePortalProduct",
		},
		{
			name:   "create_product_page",
			method: http.MethodPost,
			path:   "/v2/portalproducts/pp1/productpages",
			wantOp: "CreateProductPage",
		},
		{
			name:   "create_product_rest_endpoint_page",
			method: http.MethodPost,
			path:   "/v2/portalproducts/pp1/productrestendpointpages",
			wantOp: "CreateProductRestEndpointPage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_GetDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deployCount int
		wantStatus  int
		apiExists   bool
	}{
		{
			name:        "empty",
			deployCount: 0,
			wantStatus:  http.StatusOK,
			apiExists:   true,
		},
		{
			name:        "multiple",
			deployCount: 2,
			wantStatus:  http.StatusOK,
			apiExists:   true,
		},
		{
			name:       "api_not_found",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")

				for range tt.deployCount {
					rr := doRequest(
						t,
						h,
						http.MethodPost,
						fmt.Sprintf("/v2/apis/%s/deployments", apiID),
						map[string]any{},
					)
					require.Equal(t, http.StatusCreated, rr.Code)
				}
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/deployments", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				type listResp struct {
					Items []apigatewayv2.Deployment `json:"items"`
				}

				var resp listResp
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				assert.Len(t, resp.Items, tt.deployCount)
			}
		})
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "delete_on_apis_list",
			method:     http.MethodDelete,
			path:       "/v2/apis",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "post_on_api_by_id",
			method:     http.MethodPost,
			path:       "/v2/apis/abc123",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "unknown_collection",
			method:     http.MethodGet,
			path:       "/v2/apis/abc123/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown_sub_resource",
			method:     http.MethodGet,
			path:       "/v2/apis/abc123/unknown/res123",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	h := apigatewayv2.NewHandler(backend)

	// Create an API via backend
	api, err := backend.CreateAPI(
		context.Background(),
		apigatewayv2.CreateAPIInput{Name: "snap-api", ProtocolType: "HTTP"},
	)
	require.NoError(t, err)

	// Test Snapshot
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into a new backend/handler
	b2 := apigatewayv2.NewInMemoryBackend()
	h2 := apigatewayv2.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	got, err := b2.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "snap-api", got.Name)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, 85, h.MatchPriority())
}

func TestInMemoryBackend_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "update_stage_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateStage("bad-api", "prod", apigatewayv2.UpdateStageInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "update_route_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateRoute("bad-api", "r1", apigatewayv2.UpdateRouteInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "update_integration_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateIntegration("bad-api", "i1", apigatewayv2.UpdateIntegrationInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "delete_deployment_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				err := b.DeleteDeployment("bad-api", "d1")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "delete_authorizer_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				err := b.DeleteAuthorizer("bad-api", "a1")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "update_authorizer_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.UpdateAuthorizer("bad-api", "a1", apigatewayv2.UpdateAuthorizerInput{})
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_stages_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetStages("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_routes_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetRoutes("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_integrations_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetIntegrations("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_deployments_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetDeployments("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
			},
		},
		{
			name: "get_authorizers_api_not_found",
			run: func(t *testing.T) {
				t.Helper()
				b := apigatewayv2.NewInMemoryBackend()
				_, err := b.GetAuthorizers("bad-api")
				require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
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

func TestHandler_CreateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantDomain string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"domainName": "example.com"},
			wantStatus: http.StatusCreated,
			wantDomain: "example.com",
		},
		{
			name:       "with_tags",
			body:       map[string]any{"domainName": "tagged.com", "tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusCreated,
			wantDomain: "tagged.com",
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, "/v2/domainnames", s)
			} else {
				rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantDomain != "" {
				var dn apigatewayv2.DomainName
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))
				assert.Equal(t, tt.wantDomain, dn.DomainNameValue)
			}
		})
	}
}

func TestHandler_CreateApiMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		setup        func(h *apigatewayv2.Handler) (apiID, stageName, domainName string)
		extraBody    map[string]any
		name         string
		domainName   string
		wantStatus   int
		wantAPIIDSet bool
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(
					t,
					h,
					http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/stages", apiID),
					map[string]any{"stageName": "prod"},
				)
				require.Equal(t, http.StatusCreated, rr.Code)
				rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{"domainName": "example.com"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return apiID, "prod", "example.com"
			},
			wantStatus:   http.StatusCreated,
			wantAPIIDSet: true,
		},
		{
			name: "with_mapping_key",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(
					t,
					h,
					http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/stages", apiID),
					map[string]any{"stageName": "dev"},
				)
				require.Equal(t, http.StatusCreated, rr.Code)
				rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{"domainName": "example.com"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return apiID, "dev", "example.com"
			},
			extraBody:  map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "domain_not_found",
			domainName: "missing.com",
			body:       map[string]any{"apiId": "abc123", "stage": "prod"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			domainName: "example.com",
			body:       "not-json",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{"domainName": "example.com"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return "", "", "example.com"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var apiID, stageName, domainName string
			domainName = tt.domainName

			if tt.setup != nil {
				apiID, stageName, domainName = tt.setup(h)
			}

			path := fmt.Sprintf("/v2/domainnames/%s/apimappings", domainName)

			var rr *httptest.ResponseRecorder

			if tt.body != nil {
				if s, ok := tt.body.(string); ok {
					rr = doRequestRaw(t, h, path, s)
				} else {
					rr = doRequest(t, h, http.MethodPost, path, tt.body)
				}
			} else {
				body := map[string]any{"apiId": apiID, "stage": stageName}
				maps.Copy(body, tt.extraBody)
				rr = doRequest(t, h, http.MethodPost, path, body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantAPIIDSet {
				var mapping apigatewayv2.APIMapping
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &mapping))
				assert.Equal(t, apiID, mapping.APIID)
				assert.NotEmpty(t, mapping.APIMappingID)
			}
		})
	}
}

func TestHandler_CreateIntegrationResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          any
		name          string
		apiID         string
		integrationID string
		wantRespKey   string
		wantStatus    int
	}{
		{
			name:        "success",
			body:        map[string]any{"integrationResponseKey": "$default"},
			wantRespKey: "$default",
			wantStatus:  http.StatusCreated,
		},
		{
			name:       "api_not_found",
			apiID:      "nonexistent",
			body:       map[string]any{"integrationResponseKey": "$default"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:          "integration_not_found",
			integrationID: "nonexistent",
			body:          map[string]any{"integrationResponseKey": "$default"},
			wantStatus:    http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := tt.apiID
			integrationID := tt.integrationID

			if apiID == "" {
				apiID = createAPI(t, h, "test-api")
			}

			if integrationID == "" && tt.wantStatus != http.StatusBadRequest && tt.apiID == "" {
				integrationRR := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/integrations", apiID),
					map[string]any{"integrationType": "HTTP_PROXY"})
				require.Equal(t, http.StatusCreated, integrationRR.Code)

				var integ apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(integrationRR.Body.Bytes(), &integ))
				integrationID = integ.IntegrationID
			}

			if integrationID == "" {
				integrationID = "placeholder"
			}

			path := fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses", apiID, integrationID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantRespKey != "" {
				var ir apigatewayv2.IntegrationResponse
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ir))
				assert.Equal(t, tt.wantRespKey, ir.IntegrationResponseKey)
				assert.NotEmpty(t, ir.IntegrationResponseID)
			}
		})
	}
}

func TestHandler_CreateModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		apiID      string
		wantName   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"name": "UserModel", "schema": `{"type":"object"}`},
			wantName:   "UserModel",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_content_type",
			body:       map[string]any{"name": "OrderModel", "schema": `{}`, "contentType": "application/json"},
			wantName:   "OrderModel",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "api_not_found",
			apiID:      "nonexistent",
			body:       map[string]any{"name": "M", "schema": "{}"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := tt.apiID
			if apiID == "" {
				apiID = createAPI(t, h, "model-api")
			}

			path := fmt.Sprintf("/v2/apis/%s/models", apiID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var model apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &model))
				assert.Equal(t, tt.wantName, model.Name)
				assert.NotEmpty(t, model.ModelID)
			}
		})
	}
}

// TestHandler_GetModelTemplate verifies that GetModelTemplate returns the model's
// schema as the template value, falling back to an empty object only when the model
// has no schema defined.
func TestHandler_GetModelTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		schema    string
		wantValue string
	}{
		{
			name:      "returns_schema",
			schema:    `{"type":"object","properties":{"id":{"type":"string"}}}`,
			wantValue: `{"type":"object","properties":{"id":{"type":"string"}}}`,
		},
		{
			name:      "empty_schema_falls_back_to_object",
			schema:    "",
			wantValue: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "tmpl-api")

			body := map[string]any{"name": "TmplModel", "contentType": "application/json"}
			if tt.schema != "" {
				body["schema"] = tt.schema
			}

			createRec := doRequest(t, h, http.MethodPost,
				fmt.Sprintf("/v2/apis/%s/models", apiID), body)
			require.Equal(t, http.StatusCreated, createRec.Code)

			var model apigatewayv2.Model
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &model))

			rec := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/models/%s/template", apiID, model.ModelID), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Value string `json:"value"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantValue, out.Value)
		})
	}
}

func TestHandler_CreateRouteResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		name        string
		apiID       string
		routeID     string
		wantRespKey string
		wantStatus  int
	}{
		{
			name:        "success",
			body:        map[string]any{"routeResponseKey": "$default"},
			wantRespKey: "$default",
			wantStatus:  http.StatusCreated,
		},
		{
			name:       "api_not_found",
			apiID:      "nonexistent",
			body:       map[string]any{"routeResponseKey": "$default"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "route_not_found",
			routeID:    "nonexistent",
			body:       map[string]any{"routeResponseKey": "$default"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := tt.apiID
			routeID := tt.routeID

			if apiID == "" {
				apiID = createAPI(t, h, "rr-api")
			}

			if routeID == "" && tt.wantStatus != http.StatusBadRequest && tt.apiID == "" {
				routeRR := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/routes", apiID),
					map[string]any{"routeKey": "GET /test"})
				require.Equal(t, http.StatusCreated, routeRR.Code)

				var route apigatewayv2.Route
				require.NoError(t, json.Unmarshal(routeRR.Body.Bytes(), &route))
				routeID = route.RouteID
			}

			if routeID == "" {
				routeID = "placeholder"
			}

			path := fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses", apiID, routeID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantRespKey != "" {
				var rresp apigatewayv2.RouteResponse
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rresp))
				assert.Equal(t, tt.wantRespKey, rresp.RouteResponseKey)
				assert.NotEmpty(t, rresp.RouteResponseID)
			}
		})
	}
}

func TestHandler_CreatePortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantPortal bool
	}{
		{
			name:       "success",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantPortal: true,
		},
		{
			name:       "with_logo",
			body:       map[string]any{"logoUri": "https://example.com/logo.png"},
			wantStatus: http.StatusCreated,
			wantPortal: true,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "method_not_allowed",
			body:       nil,
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var rr *httptest.ResponseRecorder

			if tt.name == "method_not_allowed" {
				rr = doRequest(t, h, http.MethodDelete, "/v2/portals", nil)
			} else if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, "/v2/portals", s)
			} else {
				rr = doRequest(t, h, http.MethodPost, "/v2/portals", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantPortal {
				var portal apigatewayv2.Portal
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &portal))
				assert.NotEmpty(t, portal.PortalID)
				assert.Equal(t, "ACTIVE", portal.Status)
			}
		})
	}
}

func TestHandler_CreatePortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name:       "success",
			body:       map[string]any{"displayName": "My Product"},
			wantName:   "My Product",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "with_description",
			body:       map[string]any{"displayName": "Product 2", "description": "A product"},
			wantName:   "Product 2",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, "/v2/portalproducts", s)
			} else {
				rr = doRequest(t, h, http.MethodPost, "/v2/portalproducts", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var product apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))
				assert.Equal(t, tt.wantName, product.DisplayName)
				assert.NotEmpty(t, product.PortalProductID)
			}
		})
	}
}

func TestHandler_CreateProductPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            any
		name            string
		portalProductID string
		wantStatus      int
		wantPage        bool
	}{
		{
			name:       "success",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantPage:   true,
		},
		{
			name:            "product_not_found",
			portalProductID: "nonexistent",
			body:            map[string]any{},
			wantStatus:      http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			portalProductID := tt.portalProductID
			if portalProductID == "" {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts",
					map[string]any{"displayName": "Test Product"})
				require.Equal(t, http.StatusCreated, rr.Code)

				var product apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))
				portalProductID = product.PortalProductID
			}

			path := fmt.Sprintf("/v2/portalproducts/%s/productpages", portalProductID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantPage {
				var page apigatewayv2.ProductPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &page))
				assert.NotEmpty(t, page.ProductPageID)
			}
		})
	}
}

func TestHandler_CreateProductRestEndpointPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            any
		name            string
		portalProductID string
		wantStatus      int
		wantPage        bool
	}{
		{
			name:       "success",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantPage:   true,
		},
		{
			name:            "product_not_found",
			portalProductID: "nonexistent",
			body:            map[string]any{},
			wantStatus:      http.StatusNotFound,
		},
		{
			name:       "invalid_body",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			portalProductID := tt.portalProductID
			if portalProductID == "" {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts",
					map[string]any{"displayName": "Test Product"})
				require.Equal(t, http.StatusCreated, rr.Code)

				var product apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &product))
				portalProductID = product.PortalProductID
			}

			path := fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", portalProductID)

			var rr *httptest.ResponseRecorder

			if s, ok := tt.body.(string); ok {
				rr = doRequestRaw(t, h, path, s)
			} else {
				rr = doRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantPage {
				var page apigatewayv2.ProductRestEndpointPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &page))
				assert.NotEmpty(t, page.ProductRestEndpointPageID)
			}
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *apigatewayv2.InMemoryBackend)
		verify func(t *testing.T, b *apigatewayv2.InMemoryBackend)
		name   string
	}{
		{
			name: "clears_apis",
			setup: func(b *apigatewayv2.InMemoryBackend) {
				_, err := b.CreateAPI(
					context.Background(),
					apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *apigatewayv2.InMemoryBackend) {
				t.Helper()
				apis, err := b.GetAPIs()
				require.NoError(t, err)
				assert.Empty(t, apis)
			},
		},
		{
			name: "clears_domain_names",
			setup: func(b *apigatewayv2.InMemoryBackend) {
				_, err := b.CreateDomainName(
					context.Background(),
					apigatewayv2.CreateDomainNameInput{DomainNameValue: "example.com"},
				)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *apigatewayv2.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateDomainName(
					context.Background(),
					apigatewayv2.CreateDomainNameInput{DomainNameValue: "example.com"},
				)
				require.NoError(t, err)
			},
		},
		{
			name:  "empty_backend_reset_is_safe",
			setup: func(_ *apigatewayv2.InMemoryBackend) {},
			verify: func(t *testing.T, b *apigatewayv2.InMemoryBackend) {
				t.Helper()
				apis, err := b.GetAPIs()
				require.NoError(t, err)
				assert.Empty(t, apis)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()
			tt.setup(b)
			b.Reset()
			tt.verify(t, b)
		})
	}
}

func TestHandler_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "create_domain_name_missing_field",
			method:     http.MethodPost,
			path:       "/v2/domainnames",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_portal_product_missing_display_name",
			method:     http.MethodPost,
			path:       "/v2/portalproducts",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DuplicateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate_domain_name_returns_409",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DuplicateModelName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate_model_name_returns_409",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name": "MyModel",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name": "MyModel",
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DuplicateIntegrationResponseKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate_key_returns_409",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
				"integrationType": "HTTP_PROXY",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var integration apigatewayv2.Integration
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))

			path := fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses", apiID, integration.IntegrationID)

			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"integrationResponseKey": "$default",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"integrationResponseKey": "$default",
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DuplicateRouteResponseKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate_key_returns_409",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
				"routeKey": "GET /items",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var route apigatewayv2.Route
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))

			path := fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses", apiID, route.RouteID)

			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"routeResponseKey": "$default",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"routeResponseKey": "$default",
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

// --- Tests for new operations (refinement 2) ---

func TestHandler_GetDomainNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainCnt  int
		wantStatus int
	}{
		{
			name:       "empty_list",
			domainCnt:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			domainCnt:  2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.domainCnt {
				rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
					"domainName": fmt.Sprintf("domain%d.example.com", i),
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/domainnames", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.DomainName `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.domainCnt)
			}
		})
	}
}

func TestHandler_GetDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		wantStatus int
	}{
		{
			name:       "found",
			domain:     "example.com",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			domain:     "nonexistent.com",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodGet, "/v2/domainnames/"+tt.domain, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var dn apigatewayv2.DomainName
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))
				assert.Equal(t, "example.com", dn.DomainNameValue)
			}
		})
	}
}

func TestHandler_DeleteDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		wantStatus int
	}{
		{
			name:       "success",
			domain:     "example.com",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			domain:     "nonexistent.com",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodDelete, "/v2/domainnames/"+tt.domain, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetAPIMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		domainName  string
		wantStatus  int
		mappingsCnt int
	}{
		{
			name:        "found_empty",
			domainName:  "example.com",
			wantStatus:  http.StatusOK,
			mappingsCnt: 0,
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent.com",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			// Create a stage for the api mapping
			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodGet, "/v2/domainnames/"+tt.domainName+"/apimappings", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.APIMapping `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.mappingsCnt)
			}
		})
	}
}

func TestHandler_GetAPIMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useBadDN   bool
		useBadID   bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "bad_domain",
			useBadDN:   true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "bad_mapping_id",
			useBadID:   true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, "/v2/domainnames/example.com/apimappings", map[string]any{
				"apiId": apiID,
				"stage": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var mapping apigatewayv2.APIMapping
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &mapping))

			domainName := "example.com"
			mappingID := mapping.APIMappingID

			if tt.useBadDN {
				domainName = "bad.com"
			}

			if tt.useBadID {
				mappingID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/domainnames/%s/apimappings/%s", domainName, mappingID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteAPIMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useBadDN   bool
		useBadID   bool
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "bad_domain",
			useBadDN:   true,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "bad_mapping_id",
			useBadID:   true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
				"domainName": "example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			rr = doRequest(t, h, http.MethodPost, "/v2/domainnames/example.com/apimappings", map[string]any{
				"apiId": apiID,
				"stage": "prod",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var mapping apigatewayv2.APIMapping
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &mapping))

			domainName := "example.com"
			mappingID := mapping.APIMappingID

			if tt.useBadDN {
				domainName = "bad.com"
			}

			if tt.useBadID {
				mappingID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/domainnames/%s/apimappings/%s", domainName, mappingID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetModels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiExists  bool
		modelCnt   int
		wantStatus int
	}{
		{
			name:       "empty",
			apiExists:  true,
			modelCnt:   0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			apiExists:  true,
			modelCnt:   2,
			wantStatus: http.StatusOK,
		},
		{
			name:       "api_not_found",
			apiExists:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			for i := range tt.modelCnt {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
					"name":        fmt.Sprintf("Model%d", i),
					"schema":      `{}`,
					"contentType": "application/json",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/models", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.Model `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.modelCnt)
			}
		})
	}
}

func TestHandler_GetModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name":        "MyModel",
				"schema":      `{}`,
				"contentType": "application/json",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var model apigatewayv2.Model
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &model))

			modelID := model.ModelID
			if tt.useWrongID {
				modelID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/models/%s", apiID, modelID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var got apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
				assert.Equal(t, "MyModel", got.Name)
			}
		})
	}
}

func TestHandler_DeleteModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
				"name":        "MyModel",
				"schema":      `{}`,
				"contentType": "application/json",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var model apigatewayv2.Model
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &model))

			modelID := model.ModelID
			if tt.useWrongID {
				modelID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodDelete, fmt.Sprintf("/v2/apis/%s/models/%s", apiID, modelID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetIntegrationResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		apiExists         bool
		integrationExists bool
		responseCnt       int
		wantStatus        int
	}{
		{
			name:              "empty",
			apiExists:         true,
			integrationExists: true,
			responseCnt:       0,
			wantStatus:        http.StatusOK,
		},
		{
			name:              "one_response",
			apiExists:         true,
			integrationExists: true,
			responseCnt:       1,
			wantStatus:        http.StatusOK,
		},
		{
			name:              "api_not_found",
			apiExists:         false,
			integrationExists: false,
			wantStatus:        http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			integrationID := "nonexistent"

			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			if tt.integrationExists {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
					"integrationType": "HTTP_PROXY",
					"integrationUri":  "https://example.com",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var integration apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))
				integrationID = integration.IntegrationID
			}

			for i := range tt.responseCnt {
				path := fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses", apiID, integrationID)
				rr := doRequest(t, h, http.MethodPost, path, map[string]any{
					"integrationResponseKey": fmt.Sprintf("/2%d0/", i),
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses", apiID, integrationID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.IntegrationResponse `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.responseCnt)
			}
		})
	}
}

func TestHandler_GetIntegrationResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
				"integrationType": "HTTP_PROXY",
				"integrationUri":  "https://example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var integration apigatewayv2.Integration
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))

			path := fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses", apiID, integration.IntegrationID)
			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"integrationResponseKey": "$default",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var ir apigatewayv2.IntegrationResponse
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ir))

			responseID := ir.IntegrationResponseID
			if tt.useWrongID {
				responseID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses/%s",
					apiID, integration.IntegrationID, responseID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteIntegrationResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
				"integrationType": "HTTP_PROXY",
				"integrationUri":  "https://example.com",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var integration apigatewayv2.Integration
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &integration))

			path := fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses", apiID, integration.IntegrationID)
			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"integrationResponseKey": "$default",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var ir apigatewayv2.IntegrationResponse
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ir))

			responseID := ir.IntegrationResponseID
			if tt.useWrongID {
				responseID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses/%s",
					apiID, integration.IntegrationID, responseID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetRouteResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		apiExists   bool
		routeExists bool
		responseCnt int
		wantStatus  int
	}{
		{
			name:        "empty",
			apiExists:   true,
			routeExists: true,
			responseCnt: 0,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "api_not_found",
			apiExists:   false,
			routeExists: false,
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			routeID := "nonexistent"

			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			if tt.routeExists {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
					"routeKey": "GET /items",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var route apigatewayv2.Route
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))
				routeID = route.RouteID
			}

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses", apiID, routeID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.RouteResponse `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.responseCnt)
			}
		})
	}
}

func TestHandler_GetRouteResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
				"routeKey": "GET /items",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var route apigatewayv2.Route
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))

			path := fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses", apiID, route.RouteID)
			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"routeResponseKey": "$default",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var rr2 apigatewayv2.RouteResponse
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rr2))

			responseID := rr2.RouteResponseID
			if tt.useWrongID {
				responseID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses/%s", apiID, route.RouteID, responseID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteRouteResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
				"routeKey": "GET /items",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var route apigatewayv2.Route
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &route))

			path := fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses", apiID, route.RouteID)
			rr = doRequest(t, h, http.MethodPost, path, map[string]any{
				"routeResponseKey": "$default",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var rr2 apigatewayv2.RouteResponse
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rr2))

			responseID := rr2.RouteResponseID
			if tt.useWrongID {
				responseID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses/%s", apiID, route.RouteID, responseID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ListPortals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		portalCnt  int
		wantStatus int
	}{
		{
			name:       "empty",
			portalCnt:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			portalCnt:  2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for range tt.portalCnt {
				rr := doRequest(t, h, http.MethodPost, "/v2/portals", map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/portals", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			var out struct {
				Items []apigatewayv2.Portal `json:"items"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
			assert.Len(t, out.Items, tt.portalCnt)
		})
	}
}

func TestHandler_GetPortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/portals", map[string]any{})
			require.Equal(t, http.StatusCreated, rr.Code)

			var portal apigatewayv2.Portal
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &portal))

			portalID := portal.PortalID
			if tt.useWrongID {
				portalID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet, "/v2/portals/"+portalID, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ListPortalProducts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		productCnt int
		wantStatus int
	}{
		{
			name:       "empty",
			productCnt: 0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple",
			productCnt: 2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tt.productCnt {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
					"displayName": fmt.Sprintf("Product %d", i),
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/portalproducts", nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			var out struct {
				Items []apigatewayv2.PortalProduct `json:"items"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
			assert.Len(t, out.Items, tt.productCnt)
		})
	}
}

func TestHandler_GetPortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useWrongID bool
		wantStatus int
	}{
		{
			name:       "found",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useWrongID: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
				"displayName": "My Product",
			})
			require.Equal(t, http.StatusCreated, rr.Code)

			var pp apigatewayv2.PortalProduct
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))

			ppID := pp.PortalProductID
			if tt.useWrongID {
				ppID = "nonexistent"
			}

			rr = doRequest(t, h, http.MethodGet, "/v2/portalproducts/"+ppID, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ListProductPages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ppExists   bool
		pageCnt    int
		wantStatus int
	}{
		{
			name:       "empty",
			ppExists:   true,
			pageCnt:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "one_page",
			ppExists:   true,
			pageCnt:    1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "portal_product_not_found",
			ppExists:   false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			ppID := "nonexistent"
			if tt.ppExists {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
					"displayName": "My Product",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var pp apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))
				ppID = pp.PortalProductID
			}

			for range tt.pageCnt {
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.ProductPage `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.pageCnt)
			}
		})
	}
}

func TestHandler_ListProductRestEndpointPages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ppExists   bool
		pageCnt    int
		wantStatus int
	}{
		{
			name:       "empty",
			ppExists:   true,
			pageCnt:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "one_page",
			ppExists:   true,
			pageCnt:    1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "portal_product_not_found",
			ppExists:   false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			ppID := "nonexistent"
			if tt.ppExists {
				rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
					"displayName": "My Product",
				})
				require.Equal(t, http.StatusCreated, rr.Code)

				var pp apigatewayv2.PortalProduct
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))
				ppID = pp.PortalProductID
			}

			for range tt.pageCnt {
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Items []apigatewayv2.ProductRestEndpointPage `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Len(t, out.Items, tt.pageCnt)
			}
		})
	}
}

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagsToSet  map[string]string
		wantTags   map[string]string
		name       string
		keysToRM   []string
		wantStatus int
	}{
		{
			name:       "get_tags_empty",
			wantTags:   map[string]string{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag_and_get",
			tagsToSet:  map[string]string{"env": "prod", "team": "platform"},
			wantTags:   map[string]string{"env": "prod", "team": "platform"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag_then_untag",
			tagsToSet:  map[string]string{"env": "prod", "team": "platform"},
			keysToRM:   []string{"team"},
			wantTags:   map[string]string{"env": "prod"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "tagged-api")

			arn := "arn:aws:apigateway:us-east-1::/apis/" + apiID

			if len(tt.tagsToSet) > 0 {
				rr := doRequest(t, h, http.MethodPost, "/v2/tags/"+arn, map[string]any{
					"tags": tt.tagsToSet,
				})
				assert.Equal(t, http.StatusCreated, rr.Code)
			}

			if len(tt.keysToRM) > 0 {
				tagKeysParam := strings.Join(tt.keysToRM, ",")
				req := httptest.NewRequest(http.MethodDelete, "/v2/tags/"+arn+"?tagKeys="+tagKeysParam, nil)
				rr := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rr)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, http.StatusNoContent, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/tags/"+arn, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Tags map[string]string `json:"tags"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Equal(t, tt.wantTags, out.Tags)
			}
		})
	}
}

func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "get_not_found",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "post_not_found",
			method:     http.MethodPost,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_not_found",
			method:     http.MethodDelete,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := "arn:aws:apigateway:us-east-1::/apis/nonexistent"

			var rr *httptest.ResponseRecorder
			if tt.method == http.MethodPost {
				rr = doRequest(t, h, tt.method, "/v2/tags/"+arn, map[string]any{"tags": map[string]string{}})
			} else {
				rr = doRequest(t, h, tt.method, "/v2/tags/"+arn, nil)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ExtractOperation_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "get_domain_names",
			method: http.MethodGet,
			path:   "/v2/domainnames",
			wantOp: "GetDomainNames",
		},
		{
			name:   "get_domain_name",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com",
			wantOp: "GetDomainName",
		},
		{
			name:   "delete_domain_name",
			method: http.MethodDelete,
			path:   "/v2/domainnames/example.com",
			wantOp: "DeleteDomainName",
		},
		{
			name:   "get_api_mappings",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com/apimappings",
			wantOp: "GetApiMappings",
		},
		{
			name:   "get_api_mapping",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com/apimappings/abc123",
			wantOp: "GetApiMapping",
		},
		{
			name:   "delete_api_mapping",
			method: http.MethodDelete,
			path:   "/v2/domainnames/example.com/apimappings/abc123",
			wantOp: "DeleteApiMapping",
		},
		{
			name:   "list_portals",
			method: http.MethodGet,
			path:   "/v2/portals",
			wantOp: "ListPortals",
		},
		{
			name:   "get_portal",
			method: http.MethodGet,
			path:   "/v2/portals/abc123",
			wantOp: "GetPortal",
		},
		{
			name:   "list_portal_products",
			method: http.MethodGet,
			path:   "/v2/portalproducts",
			wantOp: "ListPortalProducts",
		},
		{
			name:   "get_portal_product",
			method: http.MethodGet,
			path:   "/v2/portalproducts/abc123",
			wantOp: "GetPortalProduct",
		},
		{
			name:   "list_product_pages",
			method: http.MethodGet,
			path:   "/v2/portalproducts/abc123/productpages",
			wantOp: "ListProductPages",
		},
		{
			name:   "list_product_re_pages",
			method: http.MethodGet,
			path:   "/v2/portalproducts/abc123/productrestendpointpages",
			wantOp: "ListProductRestEndpointPages",
		},
		{
			name:   "get_models",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/models",
			wantOp: "GetModels",
		},
		{
			name:   "get_model",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/models/modelId",
			wantOp: "GetModel",
		},
		{
			name:   "delete_model",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/models/modelId",
			wantOp: "DeleteModel",
		},
		{
			name:   "get_integration_responses",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses",
			wantOp: "GetIntegrationResponses",
		},
		{
			name:   "get_integration_response",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses/resp1",
			wantOp: "GetIntegrationResponse",
		},
		{
			name:   "delete_integration_response",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses/resp1",
			wantOp: "DeleteIntegrationResponse",
		},
		{
			name:   "get_route_responses",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/routes/r1/routeresponses",
			wantOp: "GetRouteResponses",
		},
		{
			name:   "get_route_response",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/routes/r1/routeresponses/resp1",
			wantOp: "GetRouteResponse",
		},
		{
			name:   "delete_route_response",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/routes/r1/routeresponses/resp1",
			wantOp: "DeleteRouteResponse",
		},
		{
			name:   "get_tags",
			method: http.MethodGet,
			path:   "/v2/tags/arn:aws:apigateway:us-east-1::/apis/abc123",
			wantOp: "GetTags",
		},
		{
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/v2/tags/arn:aws:apigateway:us-east-1::/apis/abc123",
			wantOp: "TagResource",
		},
		{
			name:   "untag_resource",
			method: http.MethodDelete,
			path:   "/v2/tags/arn:aws:apigateway:us-east-1::/apis/abc123",
			wantOp: "UntagResource",
		},
		{
			name:   "create_vpc_link",
			method: http.MethodPost,
			path:   "/v2/vpclinks",
			wantOp: "CreateVpcLink",
		},
		{
			name:   "get_vpc_link",
			method: http.MethodGet,
			path:   "/v2/vpclinks/vpc1",
			wantOp: "GetVpcLink",
		},
		{
			name:   "list_routing_rules",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com/routingrules",
			wantOp: "ListRoutingRules",
		},
		{
			name:   "put_routing_rule",
			method: http.MethodPut,
			path:   "/v2/domainnames/example.com/routingrules/rule1",
			wantOp: "PutRoutingRule",
		},
		{
			name:   "delete_access_log_settings",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/stages/prod/accesslogsettings",
			wantOp: "DeleteAccessLogSettings",
		},
		{
			name:   "delete_cors_configuration",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/cors",
			wantOp: "DeleteCorsConfiguration",
		},
		{
			name:   "sharing_policy_get",
			method: http.MethodGet,
			path:   "/v2/portalproducts/pp1/sharingpolicy",
			wantOp: "GetPortalProductSharingPolicy",
		},
		{
			name:   "delete_route_request_parameter",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/routes/r1/requestparameters/param",
			wantOp: "DeleteRouteRequestParameter",
		},
		{
			name:   "delete_route_settings",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/stages/prod/routesettings/$default",
			wantOp: "DeleteRouteSettings",
		},
		{
			name:   "disable_portal",
			method: http.MethodDelete,
			path:   "/v2/portals/p1/publish",
			wantOp: "DisablePortal",
		},
		{
			name:   "preview_portal",
			method: http.MethodPost,
			path:   "/v2/portals/p1/preview",
			wantOp: "PreviewPortal",
		},
		{
			name:   "publish_portal",
			method: http.MethodPost,
			path:   "/v2/portals/p1/publish",
			wantOp: "PublishPortal",
		},
		{
			name:   "export_api",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/exports/oas30",
			wantOp: "ExportApi",
		},
		{
			name:   "import_api",
			method: http.MethodPut,
			path:   "/v2/apis",
			wantOp: "ImportApi",
		},
		{
			name:   "reimport_api",
			method: http.MethodPut,
			path:   "/v2/apis/abc123",
			wantOp: "ReimportApi",
		},
		{
			name:   "get_model_template",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/models/m1/template",
			wantOp: "GetModelTemplate",
		},
		{
			name:   "update_product_page",
			method: http.MethodPatch,
			path:   "/v2/portalproducts/p1/productpages/page1",
			wantOp: "UpdateProductPage",
		},
		{
			name:   "update_product_rest_endpoint_page",
			method: http.MethodPatch,
			path:   "/v2/portalproducts/p1/productrestendpointpages/page1",
			wantOp: "UpdateProductRestEndpointPage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func createStage(t *testing.T, h *apigatewayv2.Handler, apiID, stageName string) {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
		"stageName": stageName,
	})
	require.Equal(t, http.StatusCreated, rr.Code)
}

func createDomainName(t *testing.T, h *apigatewayv2.Handler, domainName string) {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
		"domainName": domainName,
	})
	require.Equal(t, http.StatusCreated, rr.Code)
}

func createPortal(t *testing.T, h *apigatewayv2.Handler) string {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, "/v2/portals", map[string]any{})
	require.Equal(t, http.StatusCreated, rr.Code)
	var p apigatewayv2.Portal
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

	return p.PortalID
}

func createPortalProduct(t *testing.T, h *apigatewayv2.Handler) string {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, "/v2/portalproducts", map[string]any{
		"displayName": "test product",
	})
	require.Equal(t, http.StatusCreated, rr.Code)
	var pp apigatewayv2.PortalProduct
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &pp))

	return pp.PortalProductID
}

func TestHandler_UpdateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (apiID, deploymentID string)
		body       any
		name       string
		wantDesc   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/deployments", apiID), map[string]any{
					"description": "initial",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var dep apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dep))

				return apiID, dep.DeploymentID
			},
			body:       map[string]any{"description": "updated"},
			wantStatus: http.StatusOK,
			wantDesc:   "updated",
		},
		{
			name: "api_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent", "dep123"
			},
			body:       map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "deployment_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createAPI(t, h, "test-api"), "nonexistent"
			},
			body:       map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, deploymentID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/deployments/%s", apiID, deploymentID), tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantDesc != "" {
				var dep apigatewayv2.Deployment
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dep))
				assert.Equal(t, tt.wantDesc, dep.Description)
			}
		})
	}
}

func TestHandler_UpdateModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (apiID, modelID string)
		body       any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/models", apiID), map[string]any{
					"name": "MyModel",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var m apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &m))

				return apiID, m.ModelID
			},
			body:       map[string]any{"name": "UpdatedModel"},
			wantStatus: http.StatusOK,
			wantName:   "UpdatedModel",
		},
		{
			name: "api_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent", "model123"
			},
			body:       map[string]any{"name": "x"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "model_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createAPI(t, h, "test-api"), "nonexistent"
			},
			body:       map[string]any{"name": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, modelID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/models/%s", apiID, modelID), tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantName != "" {
				var m apigatewayv2.Model
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &m))
				assert.Equal(t, tt.wantName, m.Name)
			}
		})
	}
}

func TestHandler_UpdateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				createDomainName(t, h, "example.com")

				return "example.com"
			},
			body:       map[string]any{"tags": map[string]string{"env": "prod"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent.com"
			},
			body:       map[string]any{"tags": map[string]string{}},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			domainName := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				"/v2/domainnames/"+domainName, tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateAPIMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (domainName, mappingID string)
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success_update_key",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				apiID := createAPI(t, h, "test-api")
				createStage(t, h, apiID, "prod")
				createDomainName(t, h, "api.example.com")
				rr := doRequest(t, h, http.MethodPost, "/v2/domainnames/api.example.com/apimappings", map[string]any{
					"apiId": apiID,
					"stage": "prod",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var m apigatewayv2.APIMapping
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &m))

				return "api.example.com", m.APIMappingID
			},
			body:       map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusOK,
		},
		{
			name: "domain_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent.com", "mapping123"
			},
			body:       map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "mapping_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				createDomainName(t, h, "api.example.com")

				return "api.example.com", "nonexistent"
			},
			body:       map[string]any{"apiMappingKey": "v1"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			domainName, mappingID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/domainnames/%s/apimappings/%s", domainName, mappingID), tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateIntegrationResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (apiID, integrationID, responseID string)
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), map[string]any{
					"integrationType": "HTTP",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var i apigatewayv2.Integration
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &i))

				rr = doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses", apiID, i.IntegrationID),
					map[string]any{"integrationResponseKey": "200"})
				require.Equal(t, http.StatusCreated, rr.Code)
				var ir apigatewayv2.IntegrationResponse
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ir))

				return apiID, i.IntegrationID, ir.IntegrationResponseID
			},
			body:       map[string]any{"contentHandlingStrategy": "CONVERT_TO_TEXT"},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string, string) {
				return "nonexistent", "int123", "resp123"
			},
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, integrationID, responseID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/integrations/%s/integrationresponses/%s", apiID, integrationID, responseID),
				tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateRouteResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (apiID, routeID, responseID string)
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string, string) {
				apiID := createAPI(t, h, "test-api")
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), map[string]any{
					"routeKey": "GET /test",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
				var r apigatewayv2.Route
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &r))

				rr = doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses", apiID, r.RouteID),
					map[string]any{"routeResponseKey": "$default"})
				require.Equal(t, http.StatusCreated, rr.Code)
				var rr2 apigatewayv2.RouteResponse
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rr2))

				return apiID, r.RouteID, rr2.RouteResponseID
			},
			body:       map[string]any{"routeResponseKey": "$updated"},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string, string) {
				return "nonexistent", "route123", "resp123"
			},
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, routeID, responseID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/routes/%s/routeresponses/%s", apiID, routeID, responseID),
				tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdatePortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortal(t, h)
			},
			body:       map[string]any{"logoUri": "https://example.com/logo.png"},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			body:       map[string]any{"logoUri": "https://example.com/logo.png"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			portalID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch, "/v2/portals/"+portalID, tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeletePortal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortal(t, h)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			portalID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete, "/v2/portals/"+portalID, nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdatePortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortalProduct(t, h)
			},
			body:       map[string]any{"displayName": "Updated Product"},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			body:       map[string]any{"displayName": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID := tt.setup(h)

			rr := doRequest(t, h, http.MethodPatch, "/v2/portalproducts/"+ppID, tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeletePortalProduct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) string {
				return createPortalProduct(t, h)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(_ *apigatewayv2.Handler) string {
				return "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete, "/v2/portalproducts/"+ppID, nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetProductPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductPageID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "product_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent", "page123"
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "page_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteProductPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductPageID
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/portalproducts/%s/productpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_GetProductRestEndpointPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductRestEndpointPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductRestEndpointPageID
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "page_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteProductRestEndpointPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (ppID, pageID string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				ppID := createPortalProduct(t, h)
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages", ppID), map[string]any{})
				require.Equal(t, http.StatusCreated, rr.Code)
				var p apigatewayv2.ProductRestEndpointPage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &p))

				return ppID, p.ProductRestEndpointPageID
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createPortalProduct(t, h), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ppID, pageID := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/portalproducts/%s/productrestendpointpages/%s", ppID, pageID), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ResetAuthorizersCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *apigatewayv2.Handler) (apiID, stageName string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				apiID := createAPI(t, h, "test-api")
				createStage(t, h, apiID, "prod")

				return apiID, "prod"
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "api_not_found",
			setup: func(_ *apigatewayv2.Handler) (string, string) {
				return "nonexistent", "prod"
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "stage_not_found",
			setup: func(h *apigatewayv2.Handler) (string, string) {
				return createAPI(t, h, "test-api"), "nonexistent"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID, stageName := tt.setup(h)

			rr := doRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/v2/apis/%s/stages/%s/cache/authorizers", apiID, stageName), nil)

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_CreateAPI_InvalidProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "valid_http",
			body:       map[string]any{"name": "my-api", "protocolType": "HTTP"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "valid_websocket",
			body:       map[string]any{"name": "my-api", "protocolType": "WEBSOCKET"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "invalid_protocol",
			body:       map[string]any{"name": "my-api", "protocolType": "GRPC"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_protocol",
			body:       map[string]any{"name": "my-api", "protocolType": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, http.MethodPost, "/v2/apis", tt.body)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_CreateIntegration_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "valid_http",
			body:       map[string]any{"integrationType": "HTTP"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "valid_mock",
			body:       map[string]any{"integrationType": "MOCK"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "invalid_type",
			body:       map[string]any{"integrationType": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/integrations", apiID), tt.body)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_CreateAuthorizer_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid_jwt",
			body: map[string]any{
				"name":           "my-auth",
				"authorizerType": "JWT",
				"jwtConfiguration": map[string]any{
					"issuer":   "https://issuer.example.com",
					"audience": []string{"client-id"},
				},
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "valid_request",
			body:       map[string]any{"name": "my-auth", "authorizerType": "REQUEST"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "invalid_type",
			body:       map[string]any{"name": "my-auth", "authorizerType": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/authorizers", apiID), tt.body)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_CreateRoute_DuplicateKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		duplicate  bool
	}{
		{
			name:       "first_route_succeeds",
			wantStatus: http.StatusCreated,
			duplicate:  false,
		},
		{
			name:       "duplicate_key_conflicts",
			wantStatus: http.StatusConflict,
			duplicate:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			body := map[string]any{"routeKey": "GET /test"}
			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), body)
			require.Equal(t, http.StatusCreated, rr.Code)

			if tt.duplicate {
				rr = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/routes", apiID), body)
				assert.Equal(t, tt.wantStatus, rr.Code)
			} else {
				assert.Equal(t, tt.wantStatus, rr.Code)
			}
		})
	}
}

func TestHandler_CreateDomainName_WithConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantConfig bool
	}{
		{
			name: "with_configurations",
			body: map[string]any{
				"domainName": "api.example.com",
				"domainNameConfigurations": []map[string]any{
					{"certificateArn": "arn:aws:acm:us-east-1:123:certificate/abc", "endpointType": "REGIONAL"},
				},
			},
			wantStatus: http.StatusCreated,
			wantConfig: true,
		},
		{
			name: "without_configurations",
			body: map[string]any{
				"domainName": "plain.example.com",
			},
			wantStatus: http.StatusCreated,
			wantConfig: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", tt.body)

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantConfig {
				var dn apigatewayv2.DomainName
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dn))
				require.Len(t, dn.DomainNameConfigurations, 1)
				assert.Equal(t, "AVAILABLE", dn.DomainNameConfigurations[0].DomainNameStatus)
				assert.Equal(t, "REGIONAL", dn.DomainNameConfigurations[0].EndpointType)
			}
		})
	}
}
