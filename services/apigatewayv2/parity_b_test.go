package apigatewayv2_test

// Tests for parity §B: API Gateway v2 list endpoints paginate with maxResults/nextToken (go-nace).

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// createTestAPI creates an API and returns its ID.
func createTestAPI(t *testing.T, h *apigatewayv2.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         name,
		"protocolType": "HTTP",
	})
	require.Equal(t, http.StatusCreated, rec.Code, "createAPI %s: %s", name, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	id, ok := out["apiId"].(string)
	require.True(t, ok, "apiId missing")

	return id
}

// createTestStage creates a stage on the given API and returns the stage name.
func createTestStage(t *testing.T, h *apigatewayv2.Handler, apiID, stageName string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
		"stageName": stageName,
	})
	require.Equal(t, http.StatusCreated, rec.Code, "createStage %s: %s", stageName, rec.Body.String())
}

// TestParityB_GetAPIs_Pagination verifies that GET /v2/apis respects maxResults
// and returns a nextToken when more pages exist.
func TestParityB_GetAPIs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		apiCount      int
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:          "all_on_one_page",
			apiCount:      3,
			maxResults:    10,
			wantCount:     3,
			wantNextToken: false,
		},
		{
			name:          "paginated_first_page",
			apiCount:      3,
			maxResults:    2,
			wantCount:     2,
			wantNextToken: true,
		},
		{
			name:          "single_result_per_page",
			apiCount:      2,
			maxResults:    1,
			wantCount:     1,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			for i := range tt.apiCount {
				createTestAPI(t, h, fmt.Sprintf("api-%d", i))
			}

			rec := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis?maxResults=%d", tt.maxResults), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Items     []apigatewayv2.API `json:"items"`
				NextToken string             `json:"nextToken"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.Len(t, out.Items, tt.wantCount)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken, "nextToken must be present")
			} else {
				assert.Empty(t, out.NextToken, "nextToken must be absent on last page")
			}
		})
	}
}

// TestParityB_GetAPIs_PaginationContinuation verifies that the nextToken from
// page 1 of GET /v2/apis correctly retrieves page 2.
func TestParityB_GetAPIs_PaginationContinuation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for i := range 3 {
		createTestAPI(t, h, fmt.Sprintf("api-%d", i))
	}

	rec1 := doRequest(t, h, http.MethodGet, "/v2/apis?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 struct {
		Items     []apigatewayv2.API `json:"items"`
		NextToken string             `json:"nextToken"`
	}
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.Items, 2)
	require.NotEmpty(t, page1.NextToken)

	rec2 := doRequest(t, h, http.MethodGet,
		"/v2/apis?maxResults=2&nextToken="+page1.NextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		Items     []apigatewayv2.API `json:"items"`
		NextToken string             `json:"nextToken"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	assert.Len(t, page2.Items, 1, "page 2 holds the remaining API")
	assert.Empty(t, page2.NextToken, "no further pages")
}

// TestParityB_GetStages_Pagination verifies that GET /v2/apis/{id}/stages paginates.
func TestParityB_GetStages_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		stageNames    []string
		maxResults    string
		wantCount     int
		wantNextToken bool
	}{
		{
			name:          "no_pagination_needed",
			stageNames:    []string{"s1", "s2"},
			maxResults:    "10",
			wantCount:     2,
			wantNextToken: false,
		},
		{
			name:          "paginated",
			stageNames:    []string{"s1", "s2", "s3"},
			maxResults:    "2",
			wantCount:     2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createTestAPI(t, h, "test-api")
			for _, s := range tt.stageNames {
				createTestStage(t, h, apiID, s)
			}

			rec := doRequest(t, h, http.MethodGet,
				fmt.Sprintf("/v2/apis/%s/stages?maxResults=%s", apiID, tt.maxResults), nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Items     []any  `json:"items"`
				NextToken string `json:"nextToken"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.Len(t, out.Items, tt.wantCount)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestParityB_MaxResultsInvalid_NoError verifies that a non-integer maxResults
// falls back to the default page size without an error.
func TestParityB_MaxResultsInvalid_NoError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for i := range 3 {
		createTestAPI(t, h, "api-"+strconv.Itoa(i))
	}

	// Non-numeric maxResults: should use default and return all items.
	rec := doRequest(t, h, http.MethodGet, "/v2/apis?maxResults=bad", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Items []apigatewayv2.API `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Items, 3)
}
