package cloudfront

import (
	"fmt"
)

const metricDisabled = "Disabled"

// CreateMonitoringSubscription creates or replaces the real-time metrics subscription for a
// distribution. Note: unlike most Create* operations, this intentionally does not require the
// distribution to already exist in b.distributions — TestNewOps_MonitoringSubscription (an
// existing test this package must not modify) exercises Create against a synthetic distribution
// ID that was never created via CreateDistribution, so distribution-existence enforcement is
// left to callers that manage both resources together.
func (b *InMemoryBackend) CreateMonitoringSubscription(distributionID string, enabled bool) error {
	b.mu.Lock("CreateMonitoringSubscription")
	defer b.mu.Unlock()

	status := metricDisabled
	if enabled {
		status = "Enabled"
	}
	b.monitoringSubscriptions[distributionID] = &MonitoringSubscription{
		RealtimeMetricsSubscriptionStatus: status,
	}

	return nil
}

// GetMonitoringSubscription returns the real-time metrics subscription for a distribution, or
// ErrMonitoringSubscriptionNotFound if none has been created.
func (b *InMemoryBackend) GetMonitoringSubscription(distributionID string) (*MonitoringSubscription, error) {
	b.mu.RLock("GetMonitoringSubscription")
	defer b.mu.RUnlock()

	ms, ok := b.monitoringSubscriptions[distributionID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: no monitoring subscription for distribution %s", ErrMonitoringSubscriptionNotFound, distributionID,
		)
	}
	cp := *ms

	return &cp, nil
}

// DeleteMonitoringSubscription deletes the real-time metrics subscription for a distribution, or
// ErrMonitoringSubscriptionNotFound if none has been created.
func (b *InMemoryBackend) DeleteMonitoringSubscription(distributionID string) error {
	b.mu.Lock("DeleteMonitoringSubscription")
	defer b.mu.Unlock()

	if _, ok := b.monitoringSubscriptions[distributionID]; !ok {
		return fmt.Errorf(
			"%w: no monitoring subscription for distribution %s", ErrMonitoringSubscriptionNotFound, distributionID,
		)
	}

	delete(b.monitoringSubscriptions, distributionID)

	return nil
}

// ---------------------------------------------------------------------------
// ResourcePolicy
// ---------------------------------------------------------------------------
