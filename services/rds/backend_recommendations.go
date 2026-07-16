package rds

import "fmt"

// AddDBRecommendation adds a recommendation to the backend. Used by tests and internal
// workflows to seed recommendations so that ModifyDBRecommendation and
// DescribeDBRecommendations can exercise real state transitions.
func (b *InMemoryBackend) AddDBRecommendation(rec DBRecommendation) {
	b.mu.Lock("AddDBRecommendation")
	defer b.mu.Unlock()

	cp := rec
	b.recommendations.Put(&cp)
}

// ModifyDBRecommendation modifies the status of a DB recommendation.
func (b *InMemoryBackend) ModifyDBRecommendation(recID, status string) (*DBRecommendation, error) {
	b.mu.Lock("ModifyDBRecommendation")
	defer b.mu.Unlock()
	rec, ok := b.recommendations.Get(recID)
	if !ok {
		return nil, fmt.Errorf("%w: recommendation %s not found", ErrInvalidParameter, recID)
	}
	rec.Status = status
	cp := *rec

	return &cp, nil
}

// DescribeDBRecommendations returns DB recommendations filtered by optional parameters.
func (b *InMemoryBackend) DescribeDBRecommendations(recID, status string) []DBRecommendation {
	b.mu.RLock("DescribeDBRecommendations")
	defer b.mu.RUnlock()
	result := make([]DBRecommendation, 0, b.recommendations.Len())
	for _, rec := range b.recommendations.All() {
		if recID != "" && rec.RecommendationID != recID {
			continue
		}
		if status != "" && rec.Status != status {
			continue
		}
		result = append(result, *rec)
	}

	return result
}
