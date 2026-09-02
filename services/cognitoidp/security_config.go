package cognitoidp

import (
	"fmt"
	"maps"
	"time"
)

// SetTypedRiskConfiguration stores a fully typed risk configuration for a
// pool or client. LastModifiedAt is stamped here (not by the caller) so
// every real SetRiskConfiguration call updates it, matching real
// RiskConfigurationType.LastModifiedDate semantics.
func (b *InMemoryBackend) SetTypedRiskConfiguration(cfg *TypedRiskConfiguration) error {
	b.mu.Lock("SetTypedRiskConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(cfg.UserPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, cfg.UserPoolID)
	}

	cfg.LastModifiedAt = time.Now()
	b.typedRiskConfigurations.Put(cfg)

	return nil
}

// GetTypedRiskConfiguration returns the typed risk configuration for a pool or client.
func (b *InMemoryBackend) GetTypedRiskConfiguration(poolID, clientID string) (*TypedRiskConfiguration, error) {
	b.mu.RLock("GetTypedRiskConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	cfg, ok := b.typedRiskConfigurations.Get(poolID + ":" + clientID)
	if !ok {
		return &TypedRiskConfiguration{UserPoolID: poolID, ClientID: clientID}, nil
	}

	cp := *cfg

	return &cp, nil
}

// riskKey builds the map key for risk configuration (pool-level if clientID="").
func riskKey(poolID, clientID string) string {
	return poolID + ":" + clientID
}

// SetRiskConfiguration stores a risk configuration blob for a pool (and optional client).
func (b *InMemoryBackend) SetRiskConfiguration(poolID, clientID string, raw map[string]any) error {
	b.mu.Lock("SetRiskConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	rawCopy := make(map[string]any, len(raw))
	maps.Copy(rawCopy, raw)
	b.riskConfigurations[riskKey(poolID, clientID)] = &RiskConfiguration{Raw: rawCopy}

	return nil
}

// DescribeRiskConfiguration retrieves the risk configuration for a pool and optional client.
func (b *InMemoryBackend) DescribeRiskConfiguration(poolID, clientID string) (*RiskConfiguration, error) {
	b.mu.RLock("DescribeRiskConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	rc := b.riskConfigurations[riskKey(poolID, clientID)]
	if rc == nil {
		return &RiskConfiguration{Raw: map[string]any{}}, nil
	}

	rawCopy := make(map[string]any, len(rc.Raw))
	maps.Copy(rawCopy, rc.Raw)

	return &RiskConfiguration{Raw: rawCopy}, nil
}

// SetLogDeliveryConfiguration stores the log delivery config for a pool.
func (b *InMemoryBackend) SetLogDeliveryConfiguration(poolID string, raw map[string]any) error {
	b.mu.Lock("SetLogDeliveryConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	rawCopy := make(map[string]any, len(raw))
	maps.Copy(rawCopy, raw)
	b.logDeliveryConfigs[poolID] = &LogDeliveryConfig{Raw: rawCopy}

	return nil
}

// GetLogDeliveryConfiguration retrieves the log delivery config for a pool.
func (b *InMemoryBackend) GetLogDeliveryConfiguration(poolID string) (*LogDeliveryConfig, error) {
	b.mu.RLock("GetLogDeliveryConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(poolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	cfg := b.logDeliveryConfigs[poolID]
	if cfg == nil {
		return &LogDeliveryConfig{Raw: map[string]any{}}, nil
	}

	rawCopy := make(map[string]any, len(cfg.Raw))
	maps.Copy(rawCopy, cfg.Raw)

	return &LogDeliveryConfig{Raw: rawCopy}, nil
}
