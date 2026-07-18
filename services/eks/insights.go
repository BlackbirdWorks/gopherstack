package eks

import (
	"fmt"
	"time"
)

// DescribeInsight returns a synthetic insight for a cluster.
func (b *InMemoryBackend) DescribeInsight(clusterName, insightID string) (*Insight, error) {
	b.mu.RLock("DescribeInsight")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return &Insight{
		ID:              insightID,
		ClusterName:     clusterName,
		Category:        typeUpgradeReadiness,
		Status:          statusPassing,
		Description:     "Cluster is ready for upgrade",
		Recommendation:  "No action needed",
		LastRefreshTime: now,
		LastTransition:  now,
	}, nil
}

// ListInsights returns synthetic insights for a cluster.
func (b *InMemoryBackend) ListInsights(clusterName string) ([]*Insight, error) {
	b.mu.RLock("ListInsights")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return []*Insight{
		{
			ID:              stableID(clusterName + "/upgrade-readiness"),
			ClusterName:     clusterName,
			Category:        typeUpgradeReadiness,
			Status:          statusPassing,
			Description:     "Cluster is ready for upgrade",
			LastRefreshTime: now,
			LastTransition:  now,
		},
		{
			ID:              stableID(clusterName + "/deprecated-apis"),
			ClusterName:     clusterName,
			Category:        typeUpgradeReadiness,
			Status:          statusPassing,
			Description:     "No deprecated APIs in use",
			LastRefreshTime: now,
			LastTransition:  now,
		},
	}, nil
}

// StartInsightsRefresh starts the (cluster-level singleton) insights refresh
// operation for a cluster.
func (b *InMemoryBackend) StartInsightsRefresh(clusterName string) (*InsightsRefresh, error) {
	b.mu.RLock("StartInsightsRefresh")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return &InsightsRefresh{
		ClusterName: clusterName,
		Status:      "COMPLETED",
		Message:     "Insights refresh completed successfully",
		StartedAt:   now,
		EndedAt:     now,
	}, nil
}

// DescribeInsightsRefresh returns the status of the (cluster-level singleton)
// insights refresh operation for a cluster.
func (b *InMemoryBackend) DescribeInsightsRefresh(clusterName string) (*InsightsRefresh, error) {
	b.mu.RLock("DescribeInsightsRefresh")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return &InsightsRefresh{
		ClusterName: clusterName,
		Status:      "COMPLETED",
		Message:     "Insights refresh completed successfully",
		StartedAt:   now,
		EndedAt:     now,
	}, nil
}
