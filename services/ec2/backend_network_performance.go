package ec2

import (
	"fmt"
	"hash/fnv"
	"sort"
	"time"
)

// ---- constants ----

const (
	networkPerformanceDefaultMetric    = "aggregate-latency"
	networkPerformanceDefaultStatistic = "p50"
	networkPerformanceDefaultPeriod    = "five-minutes"

	// networkPerformanceDefaultWindow is how far back GetAwsNetworkPerformanceData
	// looks when the caller does not supply a start time.
	networkPerformanceDefaultWindow = time.Hour

	// networkPerformanceBaseLatencyMs and networkPerformanceLatencyJitterMs bound the
	// deterministic latency values derived per source/destination pair.
	networkPerformanceBaseLatencyMs   = 5.0
	networkPerformanceLatencyJitterMs = 45.0

	// networkPerformanceHashModulus bounds the hash-derived fraction used to place a
	// deterministic latency value within [0, networkPerformanceLatencyJitterMs).
	networkPerformanceHashModulus = 1000
)

// ---- models ----

// NetworkPerformanceSubscription represents an enabled AWS Network Performance metric
// subscription for a source/destination Region or Availability Zone pair.
type NetworkPerformanceSubscription struct {
	Source      string `json:"source,omitempty"`
	Destination string `json:"destination,omitempty"`
	Metric      string `json:"metric,omitempty"`
	Statistic   string `json:"statistic,omitempty"`
	Period      string `json:"period,omitempty"`
}

// networkPerformanceSubscriptionKey returns the composite uniqueness key for a
// subscription: (source, destination, metric, statistic).
func networkPerformanceSubscriptionKey(source, destination, metric, statistic string) string {
	return source + "|" + destination + "|" + metric + "|" + statistic
}

// NetworkPerformanceMetricPoint is a single deterministic data point of a network
// performance data query response.
type NetworkPerformanceMetricPoint struct {
	StartDate time.Time
	EndDate   time.Time
	Status    string
	Value     float32
}

// NetworkPerformanceDataQuery is a single query passed to GetAwsNetworkPerformanceData.
type NetworkPerformanceDataQuery struct {
	ID          string
	Source      string
	Destination string
	Metric      string
	Statistic   string
	Period      string
}

// NetworkPerformanceDataResponse is the response to a single NetworkPerformanceDataQuery.
type NetworkPerformanceDataResponse struct {
	ID           string
	Source       string
	Destination  string
	Metric       string
	Statistic    string
	Period       string
	MetricPoints []NetworkPerformanceMetricPoint
}

// EnableAwsNetworkPerformanceMetricSubscription enables (creates, or updates if
// already present) a metric subscription for a source/destination pair.
func (b *InMemoryBackend) EnableAwsNetworkPerformanceMetricSubscription(
	source, destination, metric, statistic string,
) (bool, error) {
	if source == "" || destination == "" {
		return false, fmt.Errorf("%w: Source and Destination are required", ErrInvalidParameter)
	}

	if metric == "" {
		metric = networkPerformanceDefaultMetric
	}

	if statistic == "" {
		statistic = networkPerformanceDefaultStatistic
	}

	b.mu.Lock("EnableAwsNetworkPerformanceMetricSubscription")
	defer b.mu.Unlock()

	b.networkPerformanceSubscriptions.Put(&NetworkPerformanceSubscription{
		Source:      source,
		Destination: destination,
		Metric:      metric,
		Statistic:   statistic,
		Period:      networkPerformanceDefaultPeriod,
	})

	return true, nil
}

// DisableAwsNetworkPerformanceMetricSubscription removes a metric subscription for a
// source/destination pair. Disabling a subscription that does not exist is a no-op
// that still reports success, matching AWS's idempotent behavior.
func (b *InMemoryBackend) DisableAwsNetworkPerformanceMetricSubscription(
	source, destination, metric, statistic string,
) (bool, error) {
	if source == "" || destination == "" {
		return false, fmt.Errorf("%w: Source and Destination are required", ErrInvalidParameter)
	}

	if metric == "" {
		metric = networkPerformanceDefaultMetric
	}

	if statistic == "" {
		statistic = networkPerformanceDefaultStatistic
	}

	b.mu.Lock("DisableAwsNetworkPerformanceMetricSubscription")
	defer b.mu.Unlock()
	b.networkPerformanceSubscriptions.Delete(networkPerformanceSubscriptionKey(source, destination, metric, statistic))

	return true, nil
}

// DescribeAwsNetworkPerformanceMetricSubscriptions returns all enabled metric
// subscriptions.
func (b *InMemoryBackend) DescribeAwsNetworkPerformanceMetricSubscriptions() []*NetworkPerformanceSubscription {
	b.mu.RLock("DescribeAwsNetworkPerformanceMetricSubscriptions")
	defer b.mu.RUnlock()

	out := make([]*NetworkPerformanceSubscription, 0, b.networkPerformanceSubscriptions.Len())
	for _, s := range b.networkPerformanceSubscriptions.All() {
		cp := *s
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}

		return out[i].Destination < out[j].Destination
	})

	return out
}

// networkPerformanceLatencyMs derives a deterministic latency value (in ms) for a
// source/destination pair so repeated queries against the same pair are stable.
func networkPerformanceLatencyMs(source, destination string) float32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(source + "->" + destination))

	frac := float64(h.Sum32()%networkPerformanceHashModulus) / networkPerformanceHashModulus

	return float32(networkPerformanceBaseLatencyMs + frac*networkPerformanceLatencyJitterMs)
}

// GetAwsNetworkPerformanceData returns network performance data points for the given
// queries. Each query's data point is derived deterministically from its
// source/destination pair so results are stable across repeated calls, since this mock
// has no real network telemetry to sample from.
func (b *InMemoryBackend) GetAwsNetworkPerformanceData(
	queries []NetworkPerformanceDataQuery,
	startTime, endTime time.Time,
) ([]*NetworkPerformanceDataResponse, error) {
	if len(queries) == 0 {
		return nil, fmt.Errorf("%w: DataQuery is required", ErrInvalidParameter)
	}

	if endTime.IsZero() {
		endTime = time.Now().UTC()
	}

	if startTime.IsZero() {
		startTime = endTime.Add(-networkPerformanceDefaultWindow)
	}

	out := make([]*NetworkPerformanceDataResponse, 0, len(queries))

	for _, q := range queries {
		if q.Source == "" || q.Destination == "" {
			return nil, fmt.Errorf("%w: DataQuery Source and Destination are required", ErrInvalidParameter)
		}

		metric := q.Metric
		if metric == "" {
			metric = networkPerformanceDefaultMetric
		}

		statistic := q.Statistic
		if statistic == "" {
			statistic = networkPerformanceDefaultStatistic
		}

		period := q.Period
		if period == "" {
			period = networkPerformanceDefaultPeriod
		}

		out = append(out, &NetworkPerformanceDataResponse{
			ID:          q.ID,
			Source:      q.Source,
			Destination: q.Destination,
			Metric:      metric,
			Statistic:   statistic,
			Period:      period,
			MetricPoints: []NetworkPerformanceMetricPoint{
				{
					StartDate: startTime,
					EndDate:   endTime,
					Status:    "OK",
					Value:     networkPerformanceLatencyMs(q.Source, q.Destination),
				},
			},
		})
	}

	return out, nil
}
