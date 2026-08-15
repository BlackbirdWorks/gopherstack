package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestEKS_Insights_Lifecycle(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	_, err := b.CreateCluster("insight-cluster", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	// List insights
	insights, err := b.ListInsights("insight-cluster")
	require.NoError(t, err)
	assert.NotNil(t, insights)

	// List for nonexistent cluster
	_, err = b.ListInsights("nonexistent")
	require.Error(t, err)

	// Describe insight (use handler path)
	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ih-cluster"})

	// List via handler. ListInsights is POST (it carries an optional filter
	// body), not GET -- verified against the SDK serializer.
	rec := doREST(t, h, http.MethodPost, "/clusters/ih-cluster/insights", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List nonexistent cluster
	rec = doREST(t, h, http.MethodPost, "/clusters/nonexistent/insights", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Describe insight (synthetic - always succeeds for valid cluster)
	rec = doREST(t, h, http.MethodGet, "/clusters/ih-cluster/insights/some-id", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Start insights refresh. This is a cluster-level singleton at
	// /clusters/{name}/insights-refresh -- there is no per-refresh id in the
	// real API.
	rec = doREST(t, h, http.MethodPost, "/clusters/ih-cluster/insights-refresh", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	_, hasStatus := resp["status"]
	assert.True(t, hasStatus)

	// Describe insights refresh
	rec = doREST(t, h, http.MethodGet, "/clusters/ih-cluster/insights-refresh", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListInsights_OmitsGetOnlyFields verifies gopherstack-uult: ListInsights
// must emit only types.InsightSummary's members (category, description, id,
// insightStatus, kubernetesVersion, lastRefreshTime, lastTransitionTime,
// name) -- eks@v1.90.4 types/types.go:1485-1514. recommendation is
// DescribeInsight-only (types.Insight) and must not leak. clusterName is not
// part of either wire shape at all and must not leak either.
func TestListInsights_OmitsGetOnlyFields(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "scoped-cluster"})

	rec := doREST(t, h, http.MethodPost, "/clusters/scoped-cluster/insights", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	insights, ok := resp["insights"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, insights)

	item := insights[0].(map[string]any)
	for k := range item {
		assert.Contains(t,
			[]string{"category", "description", "id", "insightStatus", "lastRefreshTime", "lastTransitionTime"},
			k,
		)
	}
	assert.NotContains(t, item, "recommendation")
	assert.NotContains(t, item, "clusterName")
}

func TestInsights_InsightStatus_Object(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "ins-cluster"})

	// ListInsights is POST (it carries an optional filter body), not GET --
	// verified against the SDK serializer.
	rec := doREST(t, h, http.MethodPost, "/clusters/ins-cluster/insights", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	insights, _ := resp["insights"].([]any)
	require.NotEmpty(t, insights)

	for _, ins := range insights {
		insight := ins.(map[string]any)
		insightStatus, ok := insight["insightStatus"].(map[string]any)
		require.True(t, ok, "insightStatus must be an object, not a flat field")
		_, hasStatus := insightStatus["status"]
		assert.True(t, hasStatus, "insightStatus must have a 'status' field")
	}
}

func TestDescribeInsight_InsightStatus_Object(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "di-cluster"})

	rec := doREST(t, h, http.MethodGet, "/clusters/di-cluster/insights/some-insight-id", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	insight := parseResp(t, rec)["insight"].(map[string]any)
	insightStatus, ok := insight["insightStatus"].(map[string]any)
	require.True(t, ok, "insightStatus must be an object")
	assert.NotEmpty(t, insightStatus["status"])
}
