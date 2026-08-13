package cloudfront

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// validateSamplingRate returns ErrValidation when rate is outside [1, 100].
func validateSamplingRate(rate int64) error {
	if rate < minSamplingRate || rate > maxSamplingRate {
		return fmt.Errorf(
			"%w: SamplingRate must be between %d and %d, got %d",
			ErrValidation, minSamplingRate, maxSamplingRate, rate,
		)
	}

	return nil
}

// realtimeLogConfigARN builds an ARN for a Realtime Log Config.
func (b *InMemoryBackend) realtimeLogConfigARN(name string) string {
	return arn.Build("cloudfront", "", b.accountID, fmt.Sprintf("realtime-log-config/%s", name))
}

// validateRealtimeLogEndPoints checks EndPoints, a required member of
// CreateRealtimeLogConfigInput (api_op_CreateRealtimeLogConfig.go:37-43): the
// Kinesis destination real-time logs are delivered to.
func validateRealtimeLogEndPoints(endPoints []RealtimeLogEndPoint) error {
	if len(endPoints) == 0 {
		return fmt.Errorf("%w: EndPoints must not be empty", ErrValidation)
	}

	return nil
}

// CreateRealtimeLogConfig creates a new Realtime Log Config.
func (b *InMemoryBackend) CreateRealtimeLogConfig(
	name string,
	samplingRate int64,
	fields []string,
	endPoints []RealtimeLogEndPoint,
) (*RealtimeLogConfig, error) {
	if err := validateSamplingRate(samplingRate); err != nil {
		return nil, err
	}

	if err := validateRealtimeLogEndPoints(endPoints); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRealtimeLogConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.realtimeLogConfigByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: realtime log config with name %q already exists",
			ErrRealtimeLogConfigAlreadyExists,
			name,
		)
	}

	arn := b.realtimeLogConfigARN(name)
	cfg := &RealtimeLogConfig{
		ARN:          arn,
		Name:         name,
		SamplingRate: samplingRate,
		Fields:       append([]string(nil), fields...),
		EndPoints:    append([]RealtimeLogEndPoint(nil), endPoints...),
	}
	b.realtimeLogConfigs.Put(cfg)
	b.realtimeLogConfigByName[name] = arn

	return b.copyRealtimeLogConfig(cfg), nil
}

// GetRealtimeLogConfig returns a Realtime Log Config by ARN.
func (b *InMemoryBackend) GetRealtimeLogConfig(arn string) (*RealtimeLogConfig, error) {
	b.mu.RLock("GetRealtimeLogConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.realtimeLogConfigs.Get(arn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: realtime log config %s not found",
			ErrRealtimeLogConfigNotFound,
			arn,
		)
	}

	return b.copyRealtimeLogConfig(cfg), nil
}

// GetRealtimeLogConfigByName returns a Realtime Log Config by name.
func (b *InMemoryBackend) GetRealtimeLogConfigByName(name string) (*RealtimeLogConfig, error) {
	b.mu.RLock("GetRealtimeLogConfigByName")
	defer b.mu.RUnlock()

	arn, ok := b.realtimeLogConfigByName[name]
	if !ok {
		return nil, fmt.Errorf(
			"%w: realtime log config %s not found",
			ErrRealtimeLogConfigNotFound,
			name,
		)
	}

	cfg, _ := b.realtimeLogConfigs.Get(arn)

	return b.copyRealtimeLogConfig(cfg), nil
}

// ListRealtimeLogConfigs returns all Realtime Log Configs sorted by name.
func (b *InMemoryBackend) ListRealtimeLogConfigs() []*RealtimeLogConfig {
	b.mu.RLock("ListRealtimeLogConfigs")
	defer b.mu.RUnlock()

	list := make([]*RealtimeLogConfig, 0, b.realtimeLogConfigs.Len())
	for _, cfg := range b.realtimeLogConfigs.All() {
		list = append(list, b.copyRealtimeLogConfig(cfg))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRealtimeLogConfig updates an existing Realtime Log Config.
func (b *InMemoryBackend) UpdateRealtimeLogConfig(
	arn string,
	samplingRate int64,
	fields []string,
	endPoints []RealtimeLogEndPoint,
) (*RealtimeLogConfig, error) {
	if err := validateSamplingRate(samplingRate); err != nil {
		return nil, err
	}

	if err := validateRealtimeLogEndPoints(endPoints); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateRealtimeLogConfig")
	defer b.mu.Unlock()

	cfg, ok := b.realtimeLogConfigs.Get(arn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: realtime log config %s not found",
			ErrRealtimeLogConfigNotFound,
			arn,
		)
	}

	cfg.SamplingRate = samplingRate
	cfg.Fields = append([]string(nil), fields...)
	cfg.EndPoints = append([]RealtimeLogEndPoint(nil), endPoints...)

	return b.copyRealtimeLogConfig(cfg), nil
}

// DeleteRealtimeLogConfig deletes a Realtime Log Config by ARN.
func (b *InMemoryBackend) DeleteRealtimeLogConfig(arn string) error {
	b.mu.Lock("DeleteRealtimeLogConfig")
	defer b.mu.Unlock()

	cfg, ok := b.realtimeLogConfigs.Get(arn)
	if !ok {
		return fmt.Errorf("%w: realtime log config %s not found", ErrRealtimeLogConfigNotFound, arn)
	}

	delete(b.realtimeLogConfigByName, cfg.Name)
	b.realtimeLogConfigs.Delete(arn)

	return nil
}

func (b *InMemoryBackend) copyRealtimeLogConfig(cfg *RealtimeLogConfig) *RealtimeLogConfig {
	cp := *cfg
	cp.Fields = append([]string(nil), cfg.Fields...)
	cp.EndPoints = append([]RealtimeLogEndPoint(nil), cfg.EndPoints...)

	return &cp
}

// --- Key Value Store CRUD ---
