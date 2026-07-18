package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestGetAPIs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		total       int
		maxResults  int
		wantPages   int
		wantPerPage int
	}{
		{
			name:        "all_fit_no_token",
			total:       3,
			maxResults:  10,
			wantPages:   1,
			wantPerPage: 3,
		},
		{
			name:        "two_per_page",
			total:       5,
			maxResults:  2,
			wantPages:   3,
			wantPerPage: 2,
		},
		{
			name:        "exact_page_boundary",
			total:       4,
			maxResults:  2,
			wantPages:   2,
			wantPerPage: 2,
		},
		{
			name:        "single_per_page",
			total:       3,
			maxResults:  1,
			wantPages:   3,
			wantPerPage: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			for i := range tc.total {
				rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
					"name":         fmt.Sprintf("api-%02d", i),
					"protocolType": "HTTP",
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				path := fmt.Sprintf("/v2/apis?maxResults=%d", tc.maxResults)
				if nextToken != "" {
					path += "&nextToken=" + nextToken
				}

				rr := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rr.Code)

				var resp struct {
					NextToken string             `json:"nextToken"`
					Items     []apigatewayv2.API `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Items), tc.maxResults, "page must not exceed maxResults")

				for _, api := range resp.Items {
					seen[api.APIID]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20, "pagination must terminate")
			}

			assert.Equal(t, tc.wantPages, pages, "unexpected page count")
			assert.Len(t, seen, tc.total, "must visit all APIs exactly once")

			for id, count := range seen {
				assert.Equalf(t, 1, count, "API %s appeared %d times", id, count)
			}
		})
	}
}

func TestGetAPIs_NoMaxResults_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 5 {
		rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
			"name": fmt.Sprintf("api-%02d", i), "protocolType": "HTTP",
		})
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	rr := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		NextToken string             `json:"nextToken"`
		Items     []apigatewayv2.API `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Len(t, resp.Items, 5)
	assert.Empty(t, resp.NextToken)
}

func TestGetAPIs_EmptyLastPage(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 2 {
		doRequest(t, h, http.MethodPost, "/v2/apis",
			map[string]any{"name": fmt.Sprintf("api-%d", i), "protocolType": "HTTP"})
	}

	// First page: maxResults=2 returns both; no nextToken.
	rr := doRequest(t, h, http.MethodGet, "/v2/apis?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		NextToken string             `json:"nextToken"`
		Items     []apigatewayv2.API `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Len(t, resp.Items, 2)
	assert.Empty(t, resp.NextToken, "no nextToken when all items fit")
}

// TestCreateAPI_HTTP_DefaultAPIKeySelectionExpression verifies that
// HTTP APIs automatically get $request.header.x-api-key when the caller
// omits apiKeySelectionExpression.
func TestCreateAPI_HTTP_DefaultAPIKeySelectionExpression(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         "http-api",
		"protocolType": "HTTP",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, "$request.header.x-api-key", resp["apiKeySelectionExpression"])
}

// TestCreateAPI_WebSocket_DefaultAPIKeySelectionExpression verifies
// that WEBSOCKET APIs automatically get $context.authorizer.usageIdentifierKey.
func TestCreateAPI_WebSocket_DefaultAPIKeySelectionExpression(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         "ws-api",
		"protocolType": "WEBSOCKET",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, "$context.authorizer.usageIdentifierKey", resp["apiKeySelectionExpression"])
}

// TestCreateAPI_ExplicitAPIKeySelectionExpressionPreserved verifies
// that an explicitly supplied apiKeySelectionExpression is not overridden.
func TestCreateAPI_ExplicitAPIKeySelectionExpressionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	custom := "$context.identity.apiKey"

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":                      "custom-api",
		"protocolType":              "HTTP",
		"apiKeySelectionExpression": custom,
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, custom, resp["apiKeySelectionExpression"])
}
