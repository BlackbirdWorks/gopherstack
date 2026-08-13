package eks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestInsightToSummaryJSON_OmitsRecommendation is a white-box companion to
// TestListInsights_OmitsGetOnlyFields (insights_test.go, eks_test package).
// The InMemoryBackend's ListInsights never actually populates Recommendation
// on the Insight values it returns, so the black-box HTTP test alone cannot
// exercise the leak this guards against: if insightToSummaryJSON ever
// regressed to including "recommendation" (DescribeInsight-only per
// types.InsightSummary, eks@v1.90.4 types/types.go:1485-1514), no synthesized
// ListInsights fixture would catch it. This constructs an Insight with
// Recommendation set directly and asserts on the raw map insightToSummaryJSON
// returns, bypassing both the backend's canned data and JSON encoding.
func TestInsightToSummaryJSON_OmitsRecommendation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ins := &Insight{
		ID:              "insight-1",
		ClusterName:     "some-cluster",
		Category:        "UPGRADE_READINESS",
		Status:          "PASSING",
		Description:     "d",
		Recommendation:  "do the thing",
		LastRefreshTime: now,
		LastTransition:  now,
	}

	m := insightToSummaryJSON(ins)

	assert.NotContains(t, m, "recommendation")
	assert.NotContains(t, m, "clusterName")
	assert.Equal(t, "insight-1", m["id"])
}
