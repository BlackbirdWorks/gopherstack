package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListPagination_MaxResultsAndNextToken covers maxResults/nextToken
// pagination across representative List* operations. Real EKS supports
// maxResults/nextToken on every List op here except ListTagsForResource --
// verified against each op's Input struct in aws-sdk-go-v2/service/eks. Prior
// to this pass, gopherstack's List* handlers always returned the full result
// set in one page regardless of maxResults.
func TestListPagination_MaxResultsAndNextToken(t *testing.T) {
	t.Parallel()

	t.Run("list_clusters_paginates", func(t *testing.T) {
		t.Parallel()

		h := newTestEKSHandler(t)
		for _, name := range []string{"a-cluster", "b-cluster", "c-cluster"} {
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": name})
		}

		rec := doREST(t, h, http.MethodGet, "/clusters?maxResults=2", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		resp := parseResp(t, rec)
		page1, ok := resp["clusters"].([]any)
		require.True(t, ok)
		assert.Len(t, page1, 2, "first page must be capped at maxResults")

		nextToken, ok := resp["nextToken"].(string)
		require.True(t, ok, "nextToken must be present when more results remain")
		require.NotEmpty(t, nextToken)

		rec2 := doREST(t, h, http.MethodGet, "/clusters?maxResults=2&nextToken="+nextToken, nil)
		require.Equal(t, http.StatusOK, rec2.Code)

		resp2 := parseResp(t, rec2)
		page2, ok := resp2["clusters"].([]any)
		require.True(t, ok)
		assert.Len(t, page2, 1, "second page must contain the remaining item")
		assert.NotContains(t, resp2, "nextToken", "nextToken must be absent once results are exhausted")
	})

	t.Run("list_clusters_no_maxresults_returns_all", func(t *testing.T) {
		t.Parallel()

		h := newTestEKSHandler(t)
		doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "only-cluster"})

		rec := doREST(t, h, http.MethodGet, "/clusters", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		resp := parseResp(t, rec)
		clusters, ok := resp["clusters"].([]any)
		require.True(t, ok)
		assert.Len(t, clusters, 1)
		assert.NotContains(t, resp, "nextToken")
	})

	t.Run("list_nodegroups_paginates", func(t *testing.T) {
		t.Parallel()

		h := newTestEKSHandler(t)
		doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ng-page-cluster"})
		for _, name := range []string{"ng-a", "ng-b", "ng-c"} {
			doREST(t, h, http.MethodPost, "/clusters/ng-page-cluster/node-groups", map[string]any{
				"nodegroupName": name,
				"nodeRole":      "arn:aws:iam::123456789012:role/ng",
				"subnets":       []string{"subnet-aaa"},
			})
		}

		rec := doREST(t, h, http.MethodGet, "/clusters/ng-page-cluster/node-groups?maxResults=1", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		resp := parseResp(t, rec)
		names, ok := resp["nodegroups"].([]any)
		require.True(t, ok)
		assert.Len(t, names, 1)
		assert.Contains(t, resp, "nextToken")
	})

	t.Run("list_capabilities_paginates_and_returns_summaries", func(t *testing.T) {
		t.Parallel()

		h := newTestEKSHandler(t)
		doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "cap-page-cluster"})
		for _, name := range []string{"cap-a", "cap-b"} {
			doREST(t, h, http.MethodPost, "/clusters/cap-page-cluster/capabilities", map[string]any{
				"capabilityName":          name,
				"type":                    "ARGOCD",
				"roleArn":                 "arn:aws:iam::123456789012:role/capability-role",
				"deletePropagationPolicy": "RETAIN",
			})
		}

		rec := doREST(t, h, http.MethodGet, "/clusters/cap-page-cluster/capabilities?maxResults=1", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		resp := parseResp(t, rec)
		capas, ok := resp["capabilities"].([]any)
		require.True(t, ok)
		require.Len(t, capas, 1)
		assert.Contains(t, resp, "nextToken")

		// Real ListCapabilities returns CapabilitySummary objects
		// (capabilityName/arn/status/type/createdAt/modifiedAt), not bare
		// names -- verified against
		// aws-sdk-go-v2/service/eks/types.CapabilitySummary.
		summary, ok := capas[0].(map[string]any)
		require.True(t, ok)
		assert.NotEmpty(t, summary["capabilityName"])
		assert.NotEmpty(t, summary["arn"])
		assert.NotEmpty(t, summary["status"])
		assert.Contains(t, summary, "modifiedAt")
		// roleArn/deletePropagationPolicy/tags are on the full Capability,
		// not the summary.
		assert.NotContains(t, summary, "roleArn")
		assert.NotContains(t, summary, "tags")
	})

	t.Run("list_insights_paginates_via_body", func(t *testing.T) {
		t.Parallel()

		h := newTestEKSHandler(t)
		doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "insights-page-cluster"})

		rec := doREST(t, h, http.MethodPost, "/clusters/insights-page-cluster/insights", map[string]any{
			"maxResults": 1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		resp := parseResp(t, rec)
		insights, ok := resp["insights"].([]any)
		require.True(t, ok)
		assert.Len(t, insights, 1, "ListInsights maxResults comes from the POST body, not query params")
		assert.Contains(t, resp, "nextToken")
	})
}
