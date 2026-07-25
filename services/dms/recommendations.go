package dms

import (
	"context"
	"fmt"
)

// BatchStartRecommendations starts the analysis to generate recommendations.
// In-memory: always returns an empty error list (all successful).
// BatchStartRecommendations seeds target-engine recommendations based on existing source endpoints.
func (b *InMemoryBackend) BatchStartRecommendations(ctx context.Context) error {
	b.mu.Lock("BatchStartRecommendations")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, ep := range b.endpointsByRegion.Get(region) {
		if ep.EndpointType == endpointTypeSource {
			b.recommendations[region] = append(b.recommendations[region], &Recommendation{
				DatabaseID: ep.EndpointArn,
				EngineName: "aurora-mysql",
				Status:     "active",
			})
		}
	}

	return nil
}

// StartRecommendation starts the analysis for a single Fleet Advisor source
// database and stores a resulting target-engine recommendation, visible via
// DescribeRecommendations. This is the single-target counterpart of
// BatchStartRecommendations.
func (b *InMemoryBackend) StartRecommendation(ctx context.Context, databaseID string) error {
	if databaseID == "" {
		return fmt.Errorf("%w: DatabaseId is required", ErrValidation)
	}

	b.mu.Lock("StartRecommendation")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	engine := "rds-mysql"
	if db, ok := b.fleetAdvisorDatabases.Get(regionKey(region, databaseID)); ok && db.EngineName != "" {
		engine = db.EngineName
	}

	b.recommendations[region] = append(b.recommendations[region], &Recommendation{
		DatabaseID: databaseID,
		EngineName: engine,
		Status:     "active",
	})

	return nil
}

// DescribeRecommendations returns Fleet Advisor target recommendations for the request region.
func (b *InMemoryBackend) DescribeRecommendations(ctx context.Context) ([]*Recommendation, error) {
	b.mu.RLock("DescribeRecommendations")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := b.recommendations[region]
	result := make([]*Recommendation, len(list))
	for i, r := range list {
		cp := *r
		result[i] = &cp
	}

	return result, nil
}
