package sesv2

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

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

// GetMessageInsights returns per-destination insights for a previously sent
// email, matching GetMessageInsightsOutput/types.EmailInsights. Real SES v2
// returns NotFoundException for a MessageId it doesn't recognize (confirmed
// against the op's documented error list); gopherstack's message history
// (SendEmail's b.emails, capped at maxRetainedEmails) is the closest
// equivalent to AWS's message tracking store. Only a synthetic SEND event is
// reported per destination -- gopherstack has no delivery/bounce/complaint
// pipeline to generate DELIVERY/BOUNCE/COMPLAINT events from.
func (b *InMemoryBackend) GetMessageInsights(messageID string) (map[string]any, error) {
	b.mu.RLock("GetMessageInsights")
	defer b.mu.RUnlock()

	for _, e := range b.emails {
		if e.MessageID != messageID {
			continue
		}

		insights := make([]map[string]any, 0, len(e.To))
		for _, dest := range e.To {
			insights = append(insights, map[string]any{
				"Destination": dest,
				"Events": []map[string]any{
					{"Type": "SEND", "Timestamp": awstime.Epoch(e.Timestamp)},
				},
			})
		}

		return map[string]any{
			keyMessageID:       e.MessageID,
			"FromEmailAddress": e.From,
			"Subject":          e.Subject,
			"Insights":         insights,
		}, nil
	}

	return nil, fmt.Errorf("%w: message %s not found", ErrNotFound, messageID)
}
