package apigatewayv2_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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
				req := httptest.NewRequest(http.MethodPost, "/v2/apis", strings.NewReader(s))
				rr2 := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rr2)
				require.NoError(t, h.Handler()(c))
				rr = rr2
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

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s", apiID), nil)
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

			rr := doRequest(t, h, http.MethodDelete, fmt.Sprintf("/v2/apis/%s", apiID), nil)
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

			rr := doRequest(t, h, http.MethodPatch, fmt.Sprintf("/v2/apis/%s", apiID), tt.update)
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
			wantOp: "CreateAPI",
		},
		{
			name:   "get_apis",
			method: http.MethodGet,
			path:   "/v2/apis",
			wantOp: "GetAPIs",
		},
		{
			name:   "get_api",
			method: http.MethodGet,
			path:   "/v2/apis/abc123",
			wantOp: "GetAPI",
		},
		{
			name:   "delete_api",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123",
			wantOp: "DeleteAPI",
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
	api, err := backend.CreateAPI(apigatewayv2.CreateAPIInput{Name: "snap-api", ProtocolType: "HTTP"})
	require.NoError(t, err)

	// Test Snapshot
	snap := h.Snapshot()
	require.NotNil(t, snap)

	// Restore into a new backend/handler
	b2 := apigatewayv2.NewInMemoryBackend()
	h2 := apigatewayv2.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

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
