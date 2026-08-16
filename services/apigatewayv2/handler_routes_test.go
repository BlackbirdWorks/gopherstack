package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

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

// TestUpdateRouteClearsAuthorizerIDOnNone verifies that when UpdateRoute
// patches AuthorizationType to "NONE", the AuthorizerID is cleared.
// AWS clears authorizerId when authorizationType is set to NONE.
func TestUpdateRouteClearsAuthorizerIDOnNone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		patchBody       map[string]any
		wantAuthType    string
		wantAuthIDEmpty bool
	}{
		{
			name:            "patch_to_none_clears_authorizer_id",
			patchBody:       map[string]any{"authorizationType": "NONE"},
			wantAuthType:    "NONE",
			wantAuthIDEmpty: true,
		},
		{
			name:            "patch_to_jwt_preserves_authorizer_id",
			patchBody:       map[string]any{"authorizationType": "JWT", "authorizerId": "new-auth-id"},
			wantAuthType:    "JWT",
			wantAuthIDEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := createAPI(t, h, "test-api")

			// Create a JWT authorizer first so we have a valid authorizer ID.
			authRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/authorizers", map[string]any{
				"name":           "my-authorizer",
				"authorizerType": "JWT",
				"identitySource": []string{"$request.header.Authorization"},
				"jwtConfiguration": map[string]any{
					"issuer":   "https://example.com",
					"audience": []string{"my-app"},
				},
			})
			require.Equal(t, http.StatusCreated, authRR.Code)

			var authResult map[string]any
			require.NoError(t, json.Unmarshal(authRR.Body.Bytes(), &authResult))
			authID := authResult["authorizerId"].(string)

			// Create a route with JWT authorization.
			routeRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
				"routeKey":          "GET /items",
				"authorizationType": "JWT",
				"authorizerId":      authID,
			})
			require.Equal(t, http.StatusCreated, routeRR.Code)

			var routeResult map[string]any
			require.NoError(t, json.Unmarshal(routeRR.Body.Bytes(), &routeResult))
			routeID := routeResult["routeId"].(string)

			// Patch the route with the test body.
			patchBody := tt.patchBody
			if !tt.wantAuthIDEmpty {
				// Keep authorizer ID for non-NONE test.
				patchBody["authorizerId"] = authID
			}

			patchRR := doRequest(t, h, http.MethodPatch, "/v2/apis/"+apiID+"/routes/"+routeID, patchBody)
			require.Equal(t, http.StatusOK, patchRR.Code)

			var updated map[string]any
			require.NoError(t, json.Unmarshal(patchRR.Body.Bytes(), &updated))

			assert.Equal(t, tt.wantAuthType, updated["authorizationType"])

			if tt.wantAuthIDEmpty {
				authIDVal, hasKey := updated["authorizerId"]
				if hasKey {
					assert.Empty(t, authIDVal,
						"authorizerId should be empty after setting authorizationType to NONE")
				}
			}
		})
	}
}

// TestUpdateRouteClearsAuthorizerIDRoundtrip verifies the full round-trip:
// create route with JWT, patch to NONE, GET should show empty authorizerId.
func TestUpdateRouteClearsAuthorizerIDRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "roundtrip-api")

	// Create authorizer.
	authRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/authorizers", map[string]any{
		"name":           "rt-authorizer",
		"authorizerType": "JWT",
		"identitySource": []string{"$request.header.Authorization"},
		"jwtConfiguration": map[string]any{
			"issuer":   "https://example.com",
			"audience": []string{"my-app"},
		},
	})
	require.Equal(t, http.StatusCreated, authRR.Code)

	var authResult map[string]any
	require.NoError(t, json.Unmarshal(authRR.Body.Bytes(), &authResult))
	authID := authResult["authorizerId"].(string)

	// Create route with JWT auth.
	routeRR := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes", map[string]any{
		"routeKey":          "POST /orders",
		"authorizationType": "JWT",
		"authorizerId":      authID,
	})
	require.Equal(t, http.StatusCreated, routeRR.Code)

	var routeResult map[string]any
	require.NoError(t, json.Unmarshal(routeRR.Body.Bytes(), &routeResult))
	routeID := routeResult["routeId"].(string)

	// Verify initial state: JWT auth with authorizer ID set.
	assert.Equal(t, "JWT", routeResult["authorizationType"])
	assert.Equal(t, authID, routeResult["authorizerId"])

	// Patch to NONE.
	patchRR := doRequest(t, h, http.MethodPatch, "/v2/apis/"+apiID+"/routes/"+routeID,
		map[string]any{"authorizationType": "NONE"})
	require.Equal(t, http.StatusOK, patchRR.Code)

	// GET route and verify authorizerId is cleared.
	getRR := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/routes/"+routeID, nil)
	require.Equal(t, http.StatusOK, getRR.Code)

	var getResult map[string]any
	require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &getResult))

	assert.Equal(t, "NONE", getResult["authorizationType"])
	assert.Empty(t, getResult["authorizerId"], "authorizerId should be empty after UpdateRoute to NONE")
}

func TestGetRoutes_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		total      int
		maxResults int
		wantPages  int
	}{
		{"three_routes_two_per_page", 3, 2, 2},
		{"four_routes_two_per_page", 4, 2, 2},
		{"five_routes_two_per_page", 5, 2, 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			for i := range tc.total {
				rr := doRequest(t, h, http.MethodPost,
					fmt.Sprintf("/v2/apis/%s/routes", apiID),
					map[string]any{"routeKey": fmt.Sprintf("GET /path%02d", i)},
				)
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				path := fmt.Sprintf("/v2/apis/%s/routes?maxResults=%d", apiID, tc.maxResults)
				if nextToken != "" {
					path += "&nextToken=" + nextToken
				}

				rr := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rr.Code)

				var resp struct {
					NextToken string               `json:"nextToken"`
					Items     []apigatewayv2.Route `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Items), tc.maxResults)

				for _, r := range resp.Items {
					seen[r.RouteID]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20)
			}

			assert.Equal(t, tc.wantPages, pages)
			assert.Len(t, seen, tc.total)

			for id, count := range seen {
				assert.Equalf(t, 1, count, "route %s duplicated", id)
			}
		})
	}
}

// TestCreateRoute_APIKeyRequiredPresentAndFalse verifies that every
// CreateRoute response includes apiKeyRequired=false even when the caller
// does not set it.  Real AWS always returns this field.
func TestCreateRoute_APIKeyRequiredPresentAndFalse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "test-api")

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
		map[string]any{"routeKey": "GET /items"})

	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	v, hasKey := resp["apiKeyRequired"]
	assert.True(t, hasKey, "apiKeyRequired should be present in response")
	assert.Equal(t, false, v, "apiKeyRequired should be false by default")
}

// TestCreateRoute_AuthorizationScopesIsEmptyArray verifies that
// authorizationScopes is always [] (never null or absent) in CreateRoute
// responses.  Real AWS returns an empty array by default.
func TestCreateRoute_AuthorizationScopesIsEmptyArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "scopes-api")

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
		map[string]any{"routeKey": "POST /orders"})

	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	raw, hasKey := resp["authorizationScopes"]
	assert.True(t, hasKey, "authorizationScopes should be present in response")

	scopes, ok := raw.([]any)
	assert.True(t, ok, "authorizationScopes should be a JSON array, got %T", raw)
	assert.Empty(t, scopes, "authorizationScopes should be empty array by default")
}

// TestGetRoute_AuthorizationScopesIsEmptyArray verifies the same
// guarantee on a GetRoute round-trip.
func TestGetRoute_AuthorizationScopesIsEmptyArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "get-scopes-api")

	// Create a route.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
		map[string]any{"routeKey": "GET /ping"})
	require.Equal(t, http.StatusCreated, rr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &created))
	routeID, _ := created["routeId"].(string)
	require.NotEmpty(t, routeID)

	// Get the route back.
	rr2 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/routes/"+routeID, nil)
	require.Equal(t, http.StatusOK, rr2.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(rr2.Body.Bytes(), &got))

	raw, hasKey := got["authorizationScopes"]
	assert.True(t, hasKey, "authorizationScopes should be present in GetRoute response")

	scopes, ok := raw.([]any)
	assert.True(t, ok, "authorizationScopes should be a JSON array")
	assert.Empty(t, scopes)
}
