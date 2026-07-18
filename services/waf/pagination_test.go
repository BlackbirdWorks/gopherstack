package waf_test

// pagination_test.go covers NextMarker + Limit pagination behavior shared by
// every List* operation.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

// wafListPage calls a List* operation with optional NextMarker and Limit.
// Returns the raw response map.
func wafListPage(t *testing.T, h *waf.Handler, op string, body map[string]any) map[string]any {
	t.Helper()
	rec := wafDo(t, h, op, body)
	require.Equal(t, http.StatusOK, rec.Code, op+": "+rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func TestPagination_ListWebACLs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		total       int
		limit       int
		wantPage1   int
		wantHasNext bool
		wantPage2   int
	}{
		{name: "limit_covers_all", total: 3, limit: 10, wantPage1: 3, wantHasNext: false},
		{name: "limit_splits", total: 5, limit: 2, wantPage1: 2, wantHasNext: true, wantPage2: 2},
		{name: "zero_limit_returns_all", total: 3, limit: 0, wantPage1: 3, wantHasNext: false},
		{name: "limit_exact", total: 4, limit: 4, wantPage1: 4, wantHasNext: false},
		{name: "limit_one", total: 4, limit: 1, wantPage1: 1, wantHasNext: true, wantPage2: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			for i := range tt.total {
				wafCreateWebACL(t, h, "acl-paginate-"+tt.name+"-"+string(rune('a'+i)))
			}

			req := map[string]any{"Limit": tt.limit}
			resp := wafListPage(t, h, "ListWebACLs", req)
			items, _ := resp["WebACLs"].([]any)
			assert.Len(t, items, tt.wantPage1, "first page item count")

			if tt.wantHasNext {
				marker, ok := resp["NextMarker"].(string)
				require.True(t, ok, "NextMarker must be present when more pages exist")
				require.NotEmpty(t, marker)

				req2 := map[string]any{"NextMarker": marker, "Limit": tt.limit}
				resp2 := wafListPage(t, h, "ListWebACLs", req2)
				items2, _ := resp2["WebACLs"].([]any)
				assert.Len(t, items2, tt.wantPage2, "second page item count")
			} else {
				_, hasNext := resp["NextMarker"]
				assert.False(t, hasNext, "NextMarker must be absent when no more pages")
			}
		})
	}
}

func TestPagination_ListRules(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	for i := range 5 {
		wafCreateRule(t, h, "rule-pag-"+string(rune('a'+i)))
	}

	resp := wafListPage(t, h, "ListRules", map[string]any{"Limit": 3})
	items, _ := resp["Rules"].([]any)
	assert.Len(t, items, 3)
	marker, ok := resp["NextMarker"].(string)
	require.True(t, ok, "NextMarker must be present")

	resp2 := wafListPage(t, h, "ListRules", map[string]any{"NextMarker": marker, "Limit": 3})
	items2, _ := resp2["Rules"].([]any)
	assert.Len(t, items2, 2)
	_, hasNext := resp2["NextMarker"]
	assert.False(t, hasNext, "no more pages after second page")
}

func TestPagination_ListIPSets(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	for i := range 4 {
		wafCreateIPSet(t, h, "ipset-pag-"+string(rune('a'+i)))
	}

	resp := wafListPage(t, h, "ListIPSets", map[string]any{"Limit": 2})
	items, _ := resp["IPSets"].([]any)
	assert.Len(t, items, 2)
	assert.Contains(t, resp, "NextMarker")
}

func TestPagination_AllListOps_NoMarkerNoLimit(t *testing.T) {
	t.Parallel()

	// All List ops with no marker and no limit should return all items without a NextMarker.
	h := newWAFHandler(t)
	wafCreateWebACL(t, h, "acl-nopage")
	wafCreateRule(t, h, "rule-nopage")
	wafCreateIPSet(t, h, "ipset-nopage")

	tests := []struct {
		op  string
		key string
	}{
		{"ListWebACLs", "WebACLs"},
		{"ListRules", "Rules"},
		{"ListIPSets", "IPSets"},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			t.Parallel()
			resp := wafListPage(t, h, tt.op, map[string]any{})
			items, _ := resp[tt.key].([]any)
			assert.NotEmpty(t, items, "should return items")
			_, hasNext := resp["NextMarker"]
			assert.False(t, hasNext, "no NextMarker when all items fit")
		})
	}
}

func TestPagination_ListRateBasedRules(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	for i := range 4 {
		wafCreateRateBasedRule(t, h, "rbr-pag-"+string(rune('a'+i)))
	}

	resp := wafListPage(t, h, "ListRateBasedRules", map[string]any{"Limit": 2})
	items, _ := resp["Rules"].([]any)
	assert.Len(t, items, 2)
	assert.Contains(t, resp, "NextMarker")
}

func TestPagination_ListRegexPatternSets(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	for i := range 4 {
		wafCreateRegexPatternSet(t, h, "rps-pag-"+string(rune('a'+i)))
	}

	resp := wafListPage(t, h, "ListRegexPatternSets", map[string]any{"Limit": 2})
	items, _ := resp["RegexPatternSets"].([]any)
	assert.Len(t, items, 2)
	assert.Contains(t, resp, "NextMarker")
}

func TestPagination_FullCycle(t *testing.T) {
	t.Parallel()

	// Create N items and walk all pages with Limit=2; verify all items are returned.
	const total = 7
	const pageSize = 2

	h := newWAFHandler(t)
	created := make(map[string]bool, total)
	for i := range total {
		id := wafCreateWebACL(t, h, "acl-full-"+string(rune('a'+i)))
		created[id] = true
	}

	seen := map[string]bool{}
	marker := ""

	for {
		req := map[string]any{"Limit": pageSize}
		if marker != "" {
			req["NextMarker"] = marker
		}

		resp := wafListPage(t, h, "ListWebACLs", req)
		items, _ := resp["WebACLs"].([]any)
		require.NotEmpty(t, items, "page must not be empty during iteration")

		for _, raw := range items {
			m, ok := raw.(map[string]any)
			require.True(t, ok)
			id, _ := m["WebACLId"].(string)
			require.NotEmpty(t, id)
			seen[id] = true
		}

		next, ok := resp["NextMarker"].(string)
		if !ok || next == "" {
			break
		}
		marker = next
	}

	assert.Len(t, seen, total, "all items must be seen across pages")
	for id := range created {
		assert.True(t, seen[id], "created item %s not found in pages", id)
	}
}
