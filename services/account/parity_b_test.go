package account_test

// parity_b_test.go — §B parity: ListRegions maxResults/nextToken pagination

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ListRegions pagination
// ---------------------------------------------------------------------------

// TestParity_ListRegions_MaxResults verifies that maxResults limits page size
// and that the returned NextToken allows full traversal without duplication.
func TestParity_ListRegions_MaxResults(t *testing.T) {
	t.Parallel()

	// Default backend has 8 regions.
	const totalRegions = 8

	tests := []struct {
		name       string
		maxResults int
		wantPages  int
	}{
		{"maxResults_1", 1, totalRegions},
		{"maxResults_3", 3, 3},
		{"maxResults_4", 4, 2},
		{"maxResults_8_exact", 8, 1},
		{"maxResults_10_exceeds_total", 10, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				path := fmt.Sprintf("/regions?maxResults=%d", tc.maxResults)
				if nextToken != "" {
					path += "&nextToken=" + nextToken
				}

				rr := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rr.Code)

				var resp struct {
					NextToken string           `json:"NextToken"`
					Regions   []map[string]any `json:"Regions"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Regions), tc.maxResults, "page must not exceed maxResults")

				for _, r := range resp.Regions {
					name, _ := r["RegionName"].(string)
					seen[name]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20, "pagination must terminate")
			}

			assert.Equal(t, tc.wantPages, pages, "unexpected page count")
			assert.Len(t, seen, totalRegions, "must visit all regions")

			for name, count := range seen {
				assert.Equalf(t, 1, count, "region %s visited %d times", name, count)
			}
		})
	}
}

// TestParity_ListRegions_NoMaxResults verifies that omitting maxResults returns
// all regions in a single page with no NextToken.
func TestParity_ListRegions_NoMaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, http.MethodGet, "/regions", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		NextToken string           `json:"NextToken"`
		Regions   []map[string]any `json:"Regions"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Len(t, resp.Regions, 8, "all 8 default regions returned")
	assert.Empty(t, resp.NextToken, "no nextToken when all fit in one page")
}

// TestParity_ListRegions_Alphabetical verifies regions are returned in
// alphabetical order (stable cursor for name-based pagination).
func TestParity_ListRegions_Alphabetical(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, http.MethodGet, "/regions", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		Regions []map[string]any `json:"Regions"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.Regions)

	for i := 1; i < len(resp.Regions); i++ {
		prev, _ := resp.Regions[i-1]["RegionName"].(string)
		curr, _ := resp.Regions[i]["RegionName"].(string)
		assert.Lessf(t, prev, curr, "regions must be sorted: %s >= %s at index %d", prev, curr, i)
	}
}

// TestParity_ListRegions_NextTokenContinuesFromLastItem verifies that
// the nextToken returned on page N is an exclusive-start cursor — the first
// item on page N+1 is the item immediately after the last item of page N.
func TestParity_ListRegions_NextTokenContinuesFromLastItem(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Page 1: maxResults=2 → first 2 regions alphabetically.
	rr1 := doRequest(t, h, http.MethodGet, "/regions?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rr1.Code)

	var page1 struct {
		NextToken string           `json:"NextToken"`
		Regions   []map[string]any `json:"Regions"`
	}
	require.NoError(t, json.Unmarshal(rr1.Body.Bytes(), &page1))
	require.Len(t, page1.Regions, 2, "page 1 must have 2 regions")
	require.NotEmpty(t, page1.NextToken, "must have nextToken after page 1")

	lastName1, _ := page1.Regions[1]["RegionName"].(string)

	// Page 2: use nextToken from page 1.
	rr2 := doRequest(t, h, http.MethodGet, "/regions?maxResults=2&nextToken="+page1.NextToken, nil)
	require.Equal(t, http.StatusOK, rr2.Code)

	var page2 struct {
		NextToken string           `json:"NextToken"`
		Regions   []map[string]any `json:"Regions"`
	}
	require.NoError(t, json.Unmarshal(rr2.Body.Bytes(), &page2))
	require.NotEmpty(t, page2.Regions, "page 2 must have regions")

	firstName2, _ := page2.Regions[0]["RegionName"].(string)
	assert.Greater(t, firstName2, lastName1, "page 2 must start after last item of page 1")
}

// TestParity_ListRegions_StatusFilter verifies that RegionOptStatusContains
// correctly filters regions.
func TestParity_ListRegions_StatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusFilter string
		wantCount    int
	}{
		// Default backend: 6 ENABLED_BY_DEFAULT, 2 ENABLED (opt-in).
		{"no_filter_returns_all", "", 8},
		{"ENABLED_BY_DEFAULT", "ENABLED_BY_DEFAULT", 6},
		{"ENABLED", "ENABLED", 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := "/regions"

			if tc.statusFilter != "" {
				path += "?regionOptStatusContains=" + tc.statusFilter
			}

			rr := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rr.Code)

			var resp struct {
				Regions []map[string]any `json:"Regions"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
			assert.Len(t, resp.Regions, tc.wantCount, "wrong region count for filter %q", tc.statusFilter)
		})
	}
}

// TestParity_ListRegions_PaginationWithStatusFilter verifies that pagination
// works correctly when combined with a status filter.
func TestParity_ListRegions_PaginationWithStatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// 6 ENABLED_BY_DEFAULT regions; paginate with maxResults=2.
	seen := map[string]int{}
	nextToken := ""

	for i := range 10 {
		path := "/regions?maxResults=2&regionOptStatusContains=ENABLED_BY_DEFAULT"
		if nextToken != "" {
			path += "&nextToken=" + nextToken
		}

		rr := doRequest(t, h, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rr.Code)

		var resp struct {
			NextToken string           `json:"NextToken"`
			Regions   []map[string]any `json:"Regions"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

		for _, r := range resp.Regions {
			name, _ := r["RegionName"].(string)
			seen[name]++

			status, _ := r["RegionOptStatus"].(string)
			assert.Equal(t, "ENABLED_BY_DEFAULT", status, "filter must exclude other statuses")
		}

		nextToken = resp.NextToken

		if nextToken == "" {
			break
		}

		require.Less(t, i, 9, "pagination must terminate within 10 pages")
	}

	assert.Len(t, seen, 6, "must visit all 6 ENABLED_BY_DEFAULT regions")

	for name, count := range seen {
		assert.Equalf(t, 1, count, "region %s visited %d times", name, count)
	}
}

// TestParity_ListRegions_RegionFieldsPresent verifies that each region in the
// response includes the required fields.
func TestParity_ListRegions_RegionFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, http.MethodGet, "/regions", nil)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp struct {
		Regions []map[string]any `json:"Regions"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.Regions)

	for _, r := range resp.Regions {
		assert.Contains(t, r, "RegionName", "each region must have RegionName")
		assert.Contains(t, r, "RegionOptStatus", "each region must have RegionOptStatus")

		name, _ := r["RegionName"].(string)
		assert.NotEmpty(t, name, "RegionName must not be empty")
	}
}
