package sesv2

import "time"

// MetricDataQuery represents a single query for BatchGetMetricData.
type MetricDataQuery struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace"`
	Metric    string `json:"metric"`
}

// MetricDataResult is the result for a single metric query.
type MetricDataResult struct {
	ID         string      `json:"id"`
	Timestamps []time.Time `json:"timestamps"`
	Values     []float64   `json:"values"`
}

// BatchGetMetricData returns synthetic empty results for each query.
func (b *InMemoryBackend) BatchGetMetricData(
	queries []MetricDataQuery,
) ([]MetricDataResult, error) {
	now := time.Now().UTC().Truncate(time.Hour)
	results := make([]MetricDataResult, 0, len(queries))

	for _, q := range queries {
		results = append(results, MetricDataResult{
			ID:         q.ID,
			Timestamps: []time.Time{now},
			Values:     []float64{0},
		})
	}

	return results, nil
}

// GetMessageInsights returns a stub.
func (b *InMemoryBackend) GetMessageInsights(_ string) (map[string]any, error) {
	return map[string]any{}, nil
}
