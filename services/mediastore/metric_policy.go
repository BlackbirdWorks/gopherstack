package mediastore

import (
	"context"
	"regexp"
)

// objectGroupRE is the allowed character set for MetricPolicyRule.ObjectGroup,
// matching the ObjectGroup shape's `pattern` trait in the MediaStore API model
// (up to 10 '/'-separated path segments of [A-Za-z0-9_=:.\-~*]).
var objectGroupRE = regexp.MustCompile(
	`^/?(?:[A-Za-z0-9_=:\.\-\~\*]+/){0,10}(?:[A-Za-z0-9_=:\.\-\~\*]+)?/?$`,
)

// objectGroupNameRE is the allowed character set for
// MetricPolicyRule.ObjectGroupName, matching the ObjectGroupName shape's
// `pattern` trait in the MediaStore API model.
var objectGroupNameRE = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// validateMetricPolicyRule enforces the ObjectGroup/ObjectGroupName
// length (900/30 chars) and character-set limits from the MediaStore API
// model (models/apis/mediastore/2017-09-01/api-2.json: ObjectGroup max=900,
// ObjectGroupName max=30, both with a `pattern` trait). The real SDK's
// client-side validators.go only checks these fields are non-nil -- it does
// NOT check length or pattern -- so any client bypassing (or not generating
// from) that client-side check can reach the server with an out-of-bounds
// value; the emulator enforces the model's real constraints server-side to
// match real AWS.
func validateMetricPolicyRule(r MetricPolicyRule) error {
	if r.ObjectGroup == "" || len(r.ObjectGroup) > maxObjectGroupLen || !objectGroupRE.MatchString(r.ObjectGroup) {
		return ErrObjectGroupInvalid
	}

	if r.ObjectGroupName == "" ||
		len(r.ObjectGroupName) > maxObjectGroupNameLen ||
		!objectGroupNameRE.MatchString(r.ObjectGroupName) {
		return ErrObjectGroupNameInvalid
	}

	return nil
}

// PutMetricPolicy stores a metric policy for a container.
func (b *InMemoryBackend) PutMetricPolicy(ctx context.Context, name string, policy MetricPolicy) error {
	if policy.ContainerLevelMetrics != "ENABLED" && policy.ContainerLevelMetrics != "DISABLED" {
		return ErrInvalidMetricPolicy
	}

	if len(policy.MetricPolicyRules) > maxMetricPolicyRules {
		return ErrTooManyMetricRules
	}

	for _, r := range policy.MetricPolicyRules {
		if err := validateMetricPolicyRule(r); err != nil {
			return err
		}
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
