package mediastore

import "context"

// PutMetricPolicy stores a metric policy for a container.
func (b *InMemoryBackend) PutMetricPolicy(ctx context.Context, name string, policy MetricPolicy) error {
	if policy.ContainerLevelMetrics != "ENABLED" && policy.ContainerLevelMetrics != "DISABLED" {
		return ErrInvalidMetricPolicy
	}

	if len(policy.MetricPolicyRules) > maxMetricPolicyRules {
		return ErrTooManyMetricRules
	}

	region := regionFromContext(ctx)

	b.mu.Lock("PutMetricPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	p := policy
	c.MetricPolicy = &p

	return nil
}

// GetMetricPolicy retrieves the metric policy for a container.
func (b *InMemoryBackend) GetMetricPolicy(ctx context.Context, name string) (MetricPolicy, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("GetMetricPolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return MetricPolicy{}, ErrContainerNotFound
	}

	if c.MetricPolicy == nil {
		return MetricPolicy{}, ErrMetricPolicyNotFound
	}

	return *c.MetricPolicy, nil
}

// DeleteMetricPolicy removes the metric policy from a container.
func (b *InMemoryBackend) DeleteMetricPolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteMetricPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if c.MetricPolicy == nil {
		return ErrMetricPolicyNotFound
	}

	c.MetricPolicy = nil

	return nil
}
