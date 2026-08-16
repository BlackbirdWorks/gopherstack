package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

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
