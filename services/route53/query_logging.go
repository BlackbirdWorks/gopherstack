package route53

import (
	"fmt"
	"sort"
	"time"
)

// CreateQueryLoggingConfig creates a new query logging configuration.
// AWS allows at most one config per hosted zone.
func (b *InMemoryBackend) CreateQueryLoggingConfig(
	hostedZoneID, logGroupArn string,
) (*QueryLoggingConfig, error) {
	if hostedZoneID == "" {
		return nil, fmt.Errorf("%w: hostedZoneId is required", ErrInvalidInput)
	}

	if logGroupArn == "" {
		return nil, fmt.Errorf("%w: cloudWatchLogsLogGroupArn is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateQueryLoggingConfig")
	defer b.mu.Unlock()

	if _, ok := b.zones.Get(hostedZoneID); !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, hostedZoneID)
	}

	if len(b.queryLoggingConfigsByZone.Get(hostedZoneID)) > 0 {
		return nil, fmt.Errorf(
			"%w: a query logging config already exists for hosted zone %s",
			ErrQueryLoggingConfigAlreadyExists, hostedZoneID,
		)
	}

	id := "Z" + randomZoneID()
	cfg := &QueryLoggingConfig{
		ID:                        id,
		HostedZoneID:              hostedZoneID,
		CloudWatchLogsLogGroupArn: logGroupArn,
		CreatedAt:                 time.Now(),
	}

	b.queryLoggingConfigs.Put(cfg)

	cp := *cfg

	return &cp, nil
}

// GetQueryLoggingConfig returns a query logging config by ID.
func (b *InMemoryBackend) GetQueryLoggingConfig(id string) (*QueryLoggingConfig, error) {
	b.mu.RLock("GetQueryLoggingConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.queryLoggingConfigs.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: query logging config %s not found",
			ErrQueryLoggingConfigNotFound,
			id,
		)
	}

	cp := *cfg

	return &cp, nil
}

// DeleteQueryLoggingConfig deletes a query logging config.
func (b *InMemoryBackend) DeleteQueryLoggingConfig(id string) error {
	b.mu.Lock("DeleteQueryLoggingConfig")
	defer b.mu.Unlock()

	if !b.queryLoggingConfigs.Has(id) {
		return fmt.Errorf(
			"%w: query logging config %s not found",
			ErrQueryLoggingConfigNotFound,
			id,
		)
	}

	b.queryLoggingConfigs.Delete(id)

	return nil
}

// ListQueryLoggingConfigs returns all query logging configs, optionally filtered by zone.
func (b *InMemoryBackend) ListQueryLoggingConfigs(
	hostedZoneID string,
) ([]*QueryLoggingConfig, error) {
	b.mu.RLock("ListQueryLoggingConfigs")
	defer b.mu.RUnlock()

	var candidates []*QueryLoggingConfig
	if hostedZoneID == "" {
		candidates = b.queryLoggingConfigs.All()
	} else {
		candidates = b.queryLoggingConfigsByZone.Get(hostedZoneID)
	}

	result := make([]*QueryLoggingConfig, 0, len(candidates))

	for _, cfg := range candidates {
		cp := *cfg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}
