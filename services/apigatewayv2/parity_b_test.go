package apigatewayv2_test

// parity_b_test.go — §B parity: API Gateway v2 limit/nextToken pagination
// (GetAPIs, GetRoutes, GetStages, GetIntegrations, GetDeployments,
//  GetAuthorizers, GetModels)

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// ---------------------------------------------------------------------------
// GetAPIs pagination
// ---------------------------------------------------------------------------

func TestParity_GetAPIs_Pagination(t *testing.T) {
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

func TestParity_GetAPIs_NoMaxResults_ReturnsAll(t *testing.T) {
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

// ---------------------------------------------------------------------------
// GetRoutes pagination
// ---------------------------------------------------------------------------

func TestParity_GetRoutes_Pagination(t *testing.T) {
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

// ---------------------------------------------------------------------------
// GetStages pagination
// ---------------------------------------------------------------------------

func TestParity_GetStages_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "stage-api")

	for i := range 5 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/stages", apiID),
			map[string]any{"stageName": fmt.Sprintf("stage-%02d", i)},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	tests := []struct {
		name       string
		maxResults int
		wantPages  int
	}{
		{"two_per_page", 2, 3},
		{"three_per_page", 3, 2},
		{"all", 10, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				path := fmt.Sprintf("/v2/apis/%s/stages?maxResults=%d", apiID, tc.maxResults)
				if nextToken != "" {
					path += "&nextToken=" + nextToken
				}

				rr := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rr.Code)

				var resp struct {
					NextToken string           `json:"nextToken"`
					Items     []map[string]any `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Items), tc.maxResults)

				for _, s := range resp.Items {
					name, _ := s["stageName"].(string)
					seen[name]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20)
			}

			assert.Equal(t, tc.wantPages, pages)
			assert.Len(t, seen, 5)
		})
	}
}

// ---------------------------------------------------------------------------
// GetIntegrations pagination
// ---------------------------------------------------------------------------

func TestParity_GetIntegrations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "intg-api")

	for i := range 4 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/integrations", apiID),
			map[string]any{
				"integrationType": "HTTP_PROXY",
				"integrationUri":  fmt.Sprintf("https://example.com/%d", i),
			},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	tests := []struct {
		name       string
		maxResults int
		wantPages  int
	}{
		{"two_per_page", 2, 2},
		{"all_at_once", 10, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				path := fmt.Sprintf("/v2/apis/%s/integrations?maxResults=%d", apiID, tc.maxResults)
				if nextToken != "" {
					path += "&nextToken=" + nextToken
				}

				rr := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rr.Code)

				var resp struct {
					NextToken string           `json:"nextToken"`
					Items     []map[string]any `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Items), tc.maxResults)

				for _, s := range resp.Items {
					id, _ := s["integrationId"].(string)
					seen[id]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20)
			}

			assert.Equal(t, tc.wantPages, pages)
			assert.Len(t, seen, 4)

			for id, count := range seen {
				assert.Equalf(t, 1, count, "integration %s duplicated", id)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetDeployments pagination
// ---------------------------------------------------------------------------

func TestParity_GetDeployments_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "depl-api")

	// Create a stage first (required for deployments in some configurations)
	doRequest(t, h, http.MethodPost,
		fmt.Sprintf("/v2/apis/%s/stages", apiID),
		map[string]any{"stageName": "prod"},
	)

	for i := range 3 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/deployments", apiID),
			map[string]any{"description": fmt.Sprintf("deploy %d", i)},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	seen := map[string]int{}
	nextToken := ""
	pages := 0

	for {
		path := fmt.Sprintf("/v2/apis/%s/deployments?maxResults=2", apiID)
		if nextToken != "" {
			path += "&nextToken=" + nextToken
		}

		rr := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rr.Code)

		var resp struct {
			NextToken string           `json:"nextToken"`
			Items     []map[string]any `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		require.LessOrEqual(t, len(resp.Items), 2)

		for _, d := range resp.Items {
			id, _ := d["deploymentId"].(string)
			seen[id]++
		}

		pages++
		nextToken = resp.NextToken

		if nextToken == "" {
			break
		}

		require.Less(t, pages, 20)
	}

	assert.Equal(t, 2, pages)
	assert.Len(t, seen, 3)

	for id, count := range seen {
		assert.Equalf(t, 1, count, "deployment %s duplicated", id)
	}
}

// ---------------------------------------------------------------------------
// GetAuthorizers pagination
// ---------------------------------------------------------------------------

func TestParity_GetAuthorizers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "auth-api")

	for i := range 4 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/authorizers", apiID),
			map[string]any{
				"name":           fmt.Sprintf("auth-%02d", i),
				"authorizerType": "JWT",
				"jwtConfiguration": map[string]any{
					"issuer":   "https://issuer.example.com",
					"audience": []string{"aud"},
				},
			},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	tests := []struct {
		name       string
		maxResults int
		wantPages  int
	}{
		{"two_per_page", 2, 2},
		{"all_at_once", 10, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				path := fmt.Sprintf("/v2/apis/%s/authorizers?maxResults=%d", apiID, tc.maxResults)
				if nextToken != "" {
					path += "&nextToken=" + nextToken
				}

				rr := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rr.Code)

				var resp struct {
					NextToken string           `json:"nextToken"`
					Items     []map[string]any `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Items), tc.maxResults)

				for _, a := range resp.Items {
					id, _ := a["authorizerId"].(string)
					seen[id]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20)
			}

			assert.Equal(t, tc.wantPages, pages)
			assert.Len(t, seen, 4)

			for id, count := range seen {
				assert.Equalf(t, 1, count, "authorizer %s duplicated", id)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetModels pagination
// ---------------------------------------------------------------------------

func TestParity_GetModels_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "model-api")

	for i := range 3 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/models", apiID),
			map[string]any{
				"name":        fmt.Sprintf("Model%02d", i),
				"contentType": "application/json",
				"schema":      `{"type":"object"}`,
			},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	seen := map[string]int{}
	nextToken := ""
	pages := 0

	for {
		path := fmt.Sprintf("/v2/apis/%s/models?maxResults=2", apiID)
		if nextToken != "" {
			path += "&nextToken=" + nextToken
		}

		rr := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rr.Code)

		var resp struct {
			NextToken string           `json:"nextToken"`
			Items     []map[string]any `json:"items"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		require.LessOrEqual(t, len(resp.Items), 2)

		for _, m := range resp.Items {
			id, _ := m["modelId"].(string)
			seen[id]++
		}

		pages++
		nextToken = resp.NextToken

		if nextToken == "" {
			break
		}

		require.Less(t, pages, 20)
	}

	assert.Equal(t, 2, pages)
	assert.Len(t, seen, 3)

	for id, count := range seen {
		assert.Equalf(t, 1, count, "model %s duplicated", id)
	}
}

// ---------------------------------------------------------------------------
// Pagination token stability: empty list after token exhausted
// ---------------------------------------------------------------------------

func TestParity_GetAPIs_EmptyLastPage(t *testing.T) {
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
