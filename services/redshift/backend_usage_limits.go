package redshift

import (
	"fmt"
	"time"
)

// CreateUsageLimit creates a new usage limit for a cluster feature.
func (b *InMemoryBackend) CreateUsageLimit(
	clusterID, featureType, limitType, breachAction string,
	amount int64,
	tagMap map[string]string,
) (*UsageLimit, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateUsageLimit")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	id := fmt.Sprintf("ul-%d", time.Now().UnixNano())

	ul := &UsageLimit{
		UsageLimitID:      id,
		ClusterIdentifier: clusterID,
		FeatureType:       featureType,
		LimitType:         limitType,
		BreachAction:      breachAction,
		Amount:            amount,
		Tags:              tagMap,
	}
	b.usageLimits.Put(ul)

	cp := *ul

	return &cp, nil
}

// DeleteUsageLimit deletes the usage limit with the given ID.
func (b *InMemoryBackend) DeleteUsageLimit(usageLimitID string) error {
	if usageLimitID == "" {
		return fmt.Errorf("%w: UsageLimitId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteUsageLimit")
	defer b.mu.Unlock()

	if _, exists := b.usageLimits.Get(usageLimitID); !exists {
		return fmt.Errorf("%w: usage limit %s not found", ErrUsageLimitNotFound, usageLimitID)
	}

	b.usageLimits.Delete(usageLimitID)

	return nil
}

// DescribeUsageLimits returns usage limits, optionally filtered by cluster and feature type.
func (b *InMemoryBackend) DescribeUsageLimits(clusterID, featureType string) ([]UsageLimit, error) {
	b.mu.RLock("DescribeUsageLimits")
	defer b.mu.RUnlock()

	result := make([]UsageLimit, 0, b.usageLimits.Len())

	for _, ul := range b.usageLimits.All() {
		if clusterID != "" && ul.ClusterIdentifier != clusterID {
			continue
		}

		if featureType != "" && ul.FeatureType != featureType {
			continue
		}

		cp := *ul
		result = append(result, cp)
	}

	return result, nil
}

// ModifyUsageLimit modifies the amount and/or breach action of a usage limit.
func (b *InMemoryBackend) ModifyUsageLimit(usageLimitID, breachAction string, amount int64) (*UsageLimit, error) {
	if usageLimitID == "" {
		return nil, fmt.Errorf("%w: UsageLimitId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyUsageLimit")
	defer b.mu.Unlock()

	ul, exists := b.usageLimits.Get(usageLimitID)
	if !exists {
		return nil, fmt.Errorf("%w: usage limit %s not found", ErrUsageLimitNotFound, usageLimitID)
	}

	if amount > 0 {
		ul.Amount = amount
	}

	if breachAction != "" {
		ul.BreachAction = breachAction
	}

	cp := *ul

	return &cp, nil
}
