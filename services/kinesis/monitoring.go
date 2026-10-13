package kinesis

import (
	"context"
	"slices"
	"strings"
)

// uniqueStrings returns a deduplicated copy of ss, preserving order.
func uniqueStrings(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))

	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}

	return out
}

// removeStrings returns a copy of ss with all elements in remove deleted.
func removeStrings(ss, remove []string) []string {
	removeSet := make(map[string]struct{}, len(remove))
	for _, s := range remove {
		removeSet[s] = struct{}{}
	}

	out := make([]string, 0, len(ss))

	for _, s := range ss {
		if _, ok := removeSet[s]; !ok {
			out = append(out, s)
		}
	}

	return out
}

func shardLevelMetricNames() []string {
	return []string{
		"IncomingBytes", "IncomingRecords", "OutgoingBytes", "OutgoingRecords",
		"WriteProvisionedThroughputExceeded", "ReadProvisionedThroughputExceeded", "IteratorAgeMilliseconds",
	}
}

// expandShardLevelMetrics validates metric names and expands ALL into the concrete metric list.
func expandShardLevelMetrics(metrics []string) ([]string, error) {
	if len(metrics) == 0 {
		return nil, newValidationError(
			"Value '[]' at 'shardLevelMetrics' failed to satisfy constraint: Member must have length greater than or equal to 1",
		)
	}

	out := make([]string, 0, len(metrics))

	for _, m := range metrics {
		if m == "ALL" {
			out = append(out, shardLevelMetricNames()...)

			continue
		}

		if !slices.Contains(shardLevelMetricNames(), m) {
			return nil, newValidationError(
				"Value '[%s]' at 'shardLevelMetrics' failed to satisfy constraint: "+
					"Member must satisfy enum value set: [ALL, %s]",
				m, strings.Join(shardLevelMetricNames(), ", "),
			)
		}

		out = append(out, m)
	}

	return uniqueStrings(out), nil
}

// EnableEnhancedMonitoring adds shard-level metrics to a stream.
func (b *InMemoryBackend) EnableEnhancedMonitoring(
	ctx context.Context,
	input *EnableEnhancedMonitoringInput,
) (*EnableEnhancedMonitoringOutput, error) {
	metrics, err := expandShardLevelMetrics(input.ShardLevelMetrics)
	if err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("EnableEnhancedMonitoring")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(region, input.StreamName))
	if !ok {
		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("EnableEnhancedMonitoring.stream")
	defer stream.mu.Unlock()

	current := make([]string, len(stream.EnhancedMonitoring))
	copy(current, stream.EnhancedMonitoring)

	combined := make([]string, 0, len(current)+len(metrics))
	combined = append(combined, current...)
	combined = append(combined, metrics...)
	desired := uniqueStrings(combined)
	stream.EnhancedMonitoring = desired

	return &EnableEnhancedMonitoringOutput{
		StreamName:               stream.Name,
		StreamARN:                stream.ARN,
		CurrentShardLevelMetrics: current,
		DesiredShardLevelMetrics: desired,
	}, nil
}

// DisableEnhancedMonitoring removes shard-level metrics from a stream.
func (b *InMemoryBackend) DisableEnhancedMonitoring(
	ctx context.Context,
	input *DisableEnhancedMonitoringInput,
) (*DisableEnhancedMonitoringOutput, error) {
	metrics, err := expandShardLevelMetrics(input.ShardLevelMetrics)
	if err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DisableEnhancedMonitoring")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(region, input.StreamName))
	if !ok {
		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("DisableEnhancedMonitoring.stream")
	defer stream.mu.Unlock()

	current := make([]string, len(stream.EnhancedMonitoring))
	copy(current, stream.EnhancedMonitoring)

	desired := removeStrings(current, metrics)
	stream.EnhancedMonitoring = desired

	return &DisableEnhancedMonitoringOutput{
		StreamName:               stream.Name,
		StreamARN:                stream.ARN,
		CurrentShardLevelMetrics: current,
		DesiredShardLevelMetrics: desired,
	}, nil
}
