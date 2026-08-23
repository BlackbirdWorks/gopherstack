package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// PutLoggingConfiguration stores a full logging configuration JSON for the given resource ARN.
func (b *InMemoryBackend) PutLoggingConfiguration(
	ctx context.Context,
	resourceARN string,
	configJSON json.RawMessage,
) error {
	b.mu.Lock("PutLoggingConfiguration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	stored := make(json.RawMessage, len(configJSON))
	copy(stored, configJSON)
	b.loggingConfigsStore(region)[resourceARN] = stored

	return nil
}

// DeleteLoggingConfiguration removes the logging configuration for the given resource ARN.
func (b *InMemoryBackend) DeleteLoggingConfiguration(ctx context.Context, resourceARN string) error {
	b.mu.Lock("DeleteLoggingConfiguration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if _, exists := b.loggingConfigs[region][resourceARN]; !exists {
		return fmt.Errorf("%w: no logging configuration found for resource %q", ErrLoggingConfigNotFound, resourceARN)
	}

	delete(b.loggingConfigs[region], resourceARN)

	return nil
}

// GetLoggingConfiguration returns the stored logging configuration JSON for the given resource ARN.
func (b *InMemoryBackend) GetLoggingConfiguration(ctx context.Context, resourceARN string) (json.RawMessage, error) {
	b.mu.RLock("GetLoggingConfiguration")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	cfg, exists := b.loggingConfigs[region][resourceARN]
	if !exists {
		return nil, fmt.Errorf(
			"%w: no logging configuration found for resource %q",
			ErrLoggingConfigNotFound,
			resourceARN,
		)
	}

	out := make(json.RawMessage, len(cfg))
	copy(out, cfg)

	return out, nil
}

// ListLoggingConfigurations returns all stored logging configuration JSONs for the
// given scope (REGIONAL configs live under the request's actual region; CLOUDFRONT
// configs are global and stored under the "" region key -- see arnRegionForScope).
func (b *InMemoryBackend) ListLoggingConfigurations(ctx context.Context, scope string) []json.RawMessage {
	b.mu.RLock("ListLoggingConfigurations")
	defer b.mu.RUnlock()

	region := arnRegionForScope(scope, getRegion(ctx, b.region))
	regionMap := b.loggingConfigs[region]

	arns := make([]string, 0, len(regionMap))
	for arn := range regionMap {
		arns = append(arns, arn)
	}

	sort.Strings(arns)

	result := make([]json.RawMessage, 0, len(arns))

	for _, arn := range arns {
		cfg := regionMap[arn]
		out := make(json.RawMessage, len(cfg))
		copy(out, cfg)
		result = append(result, out)
	}

	return result
}
