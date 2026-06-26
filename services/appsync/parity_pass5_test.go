package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListGraphqlAPIs_Pagination verifies maxResults/nextToken on ListGraphqlApis.
func TestParity_ListGraphqlAPIs_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	for i := range 5 {
		rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
			"name":               fmt.Sprintf("api-%d", i),
			"authenticationType": "API_KEY",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		query         string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			query:         "/v1/apis",
			wantLen:       5,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			query:         "/v1/apis?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodGet, tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				NextToken   string           `json:"nextToken"`
				GraphqlAPIs []map[string]any `json:"graphqlApis"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.GraphqlAPIs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListGraphqlAPIs_FullPagination walks all pages and collects all apis.
func TestParity_ListGraphqlAPIs_FullPagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	const total = 5
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := range total {
		rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
			"name":               names[i],
			"authenticationType": "API_KEY",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		path := "/v1/apis?maxResults=2"
		if token != "" {
			path += "&nextToken=" + token
		}

		rec := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			NextToken   string           `json:"nextToken"`
			GraphqlAPIs []map[string]any `json:"graphqlApis"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.LessOrEqual(t, len(out.GraphqlAPIs), 2)

		for _, api := range out.GraphqlAPIs {
			name := api["name"].(string)
			assert.False(t, seen[name], "api %s seen twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10)

		token = out.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total)
	assert.GreaterOrEqual(t, pages, 3)
}

// TestParity_ListAPIKeys_Pagination verifies maxResults/nextToken on Listapikeys.
func TestParity_ListAPIKeys_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "key-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	for range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/apikeys", apiID), map[string]any{
			"description": "test key",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/apikeys", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/apikeys?maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken string           `json:"nextToken"`
				APIKeys   []map[string]any `json:"apiKeys"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.APIKeys, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListFunctions_Pagination verifies maxResults/nextToken on ListFunctions.
func TestParity_ListFunctions_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "fn-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/datasources", apiID), map[string]any{
		"name": "ds",
		"type": "NONE",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/functions", apiID), map[string]any{
			"name":            fmt.Sprintf("fn-%d", i),
			"dataSourceName":  "ds",
			"functionVersion": "2018-05-29",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/functions", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/functions?maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken string           `json:"nextToken"`
				Functions []map[string]any `json:"functions"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.Functions, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListDataSources_Pagination verifies maxResults/nextToken on ListDataSources.
func TestParity_ListDataSources_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "ds-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/datasources", apiID), map[string]any{
			"name": fmt.Sprintf("ds-%d", i),
			"type": "NONE",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/datasources", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/datasources?maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken   string           `json:"nextToken"`
				DataSources []map[string]any `json:"dataSources"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.DataSources, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListResolvers_Pagination verifies maxResults/nextToken on ListResolvers.
func TestParity_ListResolvers_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "res-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/datasources", apiID), map[string]any{
		"name": "ds",
		"type": "NONE",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/types", apiID), map[string]any{
		"definition": "type Query { placeholder: String }",
		"format":     "SDL",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/types/Query/resolvers", apiID), map[string]any{
			"fieldName":      fmt.Sprintf("field%d", i),
			"dataSourceName": "ds",
			"kind":           "UNIT",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/types/Query/resolvers", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/types/Query/resolvers?maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken string           `json:"nextToken"`
				Resolvers []map[string]any `json:"resolvers"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.Resolvers, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListTypes_Pagination verifies maxResults/nextToken on ListTypes.
func TestParity_ListTypes_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "type-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/types", apiID), map[string]any{
			"definition": fmt.Sprintf("type Type%d { id: ID }", i),
			"format":     "SDL",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/types?format=SDL", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/types?format=SDL&maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken string           `json:"nextToken"`
				Types     []map[string]any `json:"types"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.Types, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParity_ListDomainNames_Pagination verifies maxResults/nextToken on ListDomainNames.
func TestParity_ListDomainNames_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	for i := range 4 {
		rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", map[string]any{
			"domainName":     fmt.Sprintf("domain%d.example.com", i),
			"certificateArn": fmt.Sprintf("arn:aws:acm:us-east-1:000000000000:certificate/cert-%d", i),
			"description":    "test",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v1/domainnames",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v1/domainnames?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				NextToken         string           `json:"nextToken"`
				DomainNameConfigs []map[string]any `json:"domainNameConfigs"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.DomainNameConfigs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
